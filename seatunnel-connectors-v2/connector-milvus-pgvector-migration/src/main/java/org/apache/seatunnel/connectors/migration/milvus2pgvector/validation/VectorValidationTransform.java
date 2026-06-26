/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.migration.milvus2pgvector.validation;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportMapTransform;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Optional SeaTunnel transform that validates migrated vectors by sampling rows from the Milvus
 * source pipeline and comparing each vector against the corresponding pgvector row. Rows are
 * pass-through; validation results are logged on {@link #close()}.
 */
@Slf4j
public class VectorValidationTransform extends AbstractCatalogSupportMapTransform {

    public static final String PLUGIN_NAME = "VectorValidation";

    private final VectorValidationConfig config;

    private transient Connection pgConnection;
    private transient int validatedCount;
    private transient int failedCount;
    private transient int pkIndex = -1;
    private transient int vectorIndex = -1;
    private transient boolean initialized = false;
    private transient List<String> failureDetails;

    public VectorValidationTransform(
            @NonNull VectorValidationConfig config, @NonNull CatalogTable catalogTable) {
        super(catalogTable);
        this.config = config;
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        ensureInitialized();
        if (validatedCount >= config.getSampleSize() || pkIndex < 0 || vectorIndex < 0) {
            return inputRow;
        }
        Object pkVal = inputRow.getField(pkIndex);
        Object vectorVal = inputRow.getField(vectorIndex);
        if (pkVal == null || vectorVal == null) {
            return inputRow;
        }
        try {
            float[] milvusVec = seaTunnelVectorToFloats(vectorVal);
            float[] pgVec = queryPgVector(pkVal);
            if (pgVec == null) {
                failedCount++;
                failureDetails.add("pk=" + pkVal + ": not found in pgvector");
            } else if (milvusVec == null) {
                failedCount++;
                failureDetails.add("pk=" + pkVal + ": Milvus vector could not be decoded");
            } else {
                double sim = cosineSimilarity(milvusVec, pgVec);
                if (sim < config.getSimilarityThreshold()) {
                    failedCount++;
                    failureDetails.add(
                            "pk=" + pkVal + ": similarity=" + sim + " < " + config.getSimilarityThreshold());
                }
            }
        } catch (Exception e) {
            failedCount++;
            failureDetails.add("pk=" + pkVal + ": error=" + e.getMessage());
        }
        validatedCount++;
        return inputRow;
    }

    @Override
    protected TableSchema transformTableSchema() {
        return inputCatalogTable.getTableSchema();
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        return inputCatalogTable.getTableId();
    }

    @Override
    public void close() {
        log.info(
                "VectorValidation: validated={}, failed={}, threshold={}",
                validatedCount,
                failedCount,
                config.getSimilarityThreshold());
        if (failureDetails != null) {
            int shown = 0;
            for (String d : failureDetails) {
                if (shown++ >= 20) {
                    log.info("  ... and {} more failures", failureDetails.size() - 20);
                    break;
                }
                log.info("  FAIL: {}", d);
            }
        }
        if (failedCount == 0 && validatedCount > 0) {
            log.info("Vector validation PASSED: all {} sampled vectors match", validatedCount);
        } else if (validatedCount == 0) {
            log.info("Vector validation skipped: no rows sampled");
        } else {
            log.warn(
                    "Vector validation FAILED: {}/{} sampled vectors below threshold",
                    failedCount, validatedCount);
        }
        if (pgConnection != null) {
            try {
                pgConnection.close();
            } catch (SQLException e) {
                log.warn("Failed to close validation pg connection", e);
            }
        }
    }

    private void ensureInitialized() {
        if (initialized) {
            return;
        }
        initialized = true;
        failureDetails = new ArrayList<>();
        TableSchema schema = inputCatalogTable.getTableSchema();
        if (schema == null) {
            return;
        }
        PrimaryKey pk = schema.getPrimaryKey();
        String pkName = pk != null && pk.getColumnNames() != null && !pk.getColumnNames().isEmpty()
                ? pk.getColumnNames().get(0)
                : null;
        List<Column> columns = schema.getColumns();
        if (columns == null) {
            return;
        }
        for (int i = 0; i < columns.size(); i++) {
            Column col = columns.get(i);
            if (pkName != null && pkName.equals(col.getName())) {
                pkIndex = i;
            }
            SeaTunnelDataType<?> type = col.getDataType();
            if (type instanceof VectorType && vectorIndex < 0) {
                vectorIndex = i;
            }
        }
        if (pkIndex < 0) {
            log.warn("VectorValidation: no primary key column found; skipping validation");
            return;
        }
        if (vectorIndex < 0) {
            log.warn("VectorValidation: no vector column found; skipping validation");
            return;
        }
        try {
            Class.forName("org.postgresql.Driver");
            pgConnection =
                    DriverManager.getConnection(
                            config.getPgUrl(), config.getPgUser(), config.getPgPassword());
            log.info(
                    "VectorValidation: pkIndex={}, vectorIndex={}, sampleSize={}",
                    pkIndex,
                    vectorIndex,
                    config.getSampleSize());
        } catch (Exception e) {
            log.error("VectorValidation: failed to connect to pgvector", e);
            pkIndex = -1;
        }
    }

    private float[] queryPgVector(Object pkVal) throws SQLException {
        Column vectorCol = inputCatalogTable.getTableSchema().getColumns().get(vectorIndex);
        Column pkCol = inputCatalogTable.getTableSchema().getColumns().get(pkIndex);
        String sql =
                "SELECT \""
                        + vectorCol.getName()
                        + "\"::text FROM "
                        + config.getPgTable()
                        + " WHERE \""
                        + pkCol.getName()
                        + "\" = ?";
        try (PreparedStatement ps = pgConnection.prepareStatement(sql)) {
            ps.setObject(1, pkVal);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                String text = rs.getString(1);
                if (text == null) {
                    return null;
                }
                return VectorSimilarityValidator.parsePgVectorText(text);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static float[] seaTunnelVectorToFloats(Object val) {
        if (val == null) {
            return null;
        }
        if (val instanceof ByteBuffer) {
            ByteBuffer dup = ((ByteBuffer) val).duplicate();
            float[] out = new float[dup.remaining()];
            for (int i = 0; i < out.length; i++) {
                out[i] = dup.get();
            }
            return out;
        }
        if (val instanceof List) {
            List<Number> list = (List<Number>) val;
            float[] out = new float[list.size()];
            for (int i = 0; i < list.size(); i++) {
                out[i] = list.get(i).floatValue();
            }
            return out;
        }
        if (val instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) val;
            int max = 0;
            for (Object k : map.keySet()) {
                max = Math.max(max, ((Number) k).intValue());
            }
            float[] out = new float[max + 1];
            for (Map.Entry<?, ?> e : map.entrySet()) {
                out[((Number) e.getKey()).intValue()] = ((Number) e.getValue()).floatValue();
            }
            return out;
        }
        return null;
    }

    private static double cosineSimilarity(float[] a, float[] b) {
        if (a.length != b.length) {
            return -1.0;
        }
        double dot = 0.0;
        double normA = 0.0;
        double normB = 0.0;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
            normA += a[i] * a[i];
            normB += b[i] * b[i];
        }
        if (normA == 0 || normB == 0) {
            return 0.0;
        }
        return dot / (Math.sqrt(normA) * Math.sqrt(normB));
    }
}
