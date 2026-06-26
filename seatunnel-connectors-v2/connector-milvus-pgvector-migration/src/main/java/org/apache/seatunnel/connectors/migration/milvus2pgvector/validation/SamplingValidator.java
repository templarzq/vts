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

import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.vector.request.QueryIteratorReq;
import io.milvus.orm.iterator.QueryIterator;
import io.milvus.response.QueryResultsWrapper;

import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.exception.MigrationErrorCode;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.exception.MigrationException;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.internal.TokenBucketRateLimiter;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.schema.MigrationSchema;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Samples {@code sampleSize} rows from Milvus, fetches the corresponding pgvector row by primary
 * key, and compares every field. Scalar fields are compared by {@code equals}; vector fields are
 * compared with a small floating-point tolerance (1e-5) or exact equality for binary/bit vectors.
 */
@Slf4j
public class SamplingValidator {

    private static final double VECTOR_TOLERANCE = 1e-5;

    private final MilvusClientV2 milvusClient;
    private final Connection pgConnection;
    private final MigrationSchema schema;
    private final String pgSchema;
    private final String pgTable;
    private final int sampleSize;
    private final TokenBucketRateLimiter rateLimiter;

    public SamplingValidator(
            MilvusClientV2 milvusClient,
            Connection pgConnection,
            MigrationSchema schema,
            String pgSchema,
            String pgTable,
            int sampleSize,
            TokenBucketRateLimiter rateLimiter) {
        this.milvusClient = milvusClient;
        this.pgConnection = pgConnection;
        this.schema = schema;
        this.pgSchema = pgSchema;
        this.pgTable = pgTable;
        this.sampleSize = sampleSize;
        this.rateLimiter = rateLimiter;
    }

    public ValidationResult validate() {
        long start = System.currentTimeMillis();
        ValidationResult.ValidationResultBuilder b =
                ValidationResult.builder().validatorName("SamplingValidator");
        String pkName = schema.getPrimaryKeyName();
        if (pkName == null || pkName.isEmpty()) {
            b.passed(false).errorMessage("No primary key in schema; cannot join");
            b.durationMs(System.currentTimeMillis() - start);
            return b.build();
        }
        try {
            List<QueryResultsWrapper.RowRecord> samples = sampleFromMilvus();
            int checked = 0;
            int failed = 0;
            for (QueryResultsWrapper.RowRecord record : samples) {
                Object pkVal = record.get(pkName);
                if (pkVal == null) {
                    continue;
                }
                rateLimiter.acquire(1);
                Map<String, Object> pgRow = queryPgRow(pkName, pkVal);
                if (pgRow == null) {
                    failed++;
                    b.addDetail("pk=" + pkVal + ": row not found in pgvector");
                    continue;
                }
                checked++;
                List<String> mismatches = compareRow(record, pgRow, pkVal);
                if (!mismatches.isEmpty()) {
                    failed++;
                    for (String m : mismatches) {
                        b.addDetail(m);
                    }
                }
            }
            b.totalChecked(checked).failedCount(failed).passed(failed == 0);
            if (failed == 0) {
                b.addDetail("All " + checked + " sampled rows match field-by-field");
            }
        } catch (MigrationException e) {
            b.passed(false).errorMessage(e.getMessage());
        } catch (Exception e) {
            b.passed(false).errorMessage(e.getMessage());
            log.error("Sampling validation failed", e);
        }
        b.durationMs(System.currentTimeMillis() - start);
        return b.build();
    }

    private List<QueryResultsWrapper.RowRecord> sampleFromMilvus() {
        List<QueryResultsWrapper.RowRecord> all = new ArrayList<>();
        try {
            QueryIterator iterator =
                    milvusClient.queryIterator(
                            QueryIteratorReq.builder()
                                    .collectionName(schema.getCollectionName())
                                    .outputFields(new ArrayList<>(Collections.singletonList("*")))
                                    .batchSize(sampleSize)
                                    .build());
            while (all.size() < sampleSize) {
                List<QueryResultsWrapper.RowRecord> batch = iterator.next();
                if (batch == null || batch.isEmpty()) {
                    break;
                }
                for (QueryResultsWrapper.RowRecord r : batch) {
                    all.add(r);
                    if (all.size() >= sampleSize) {
                        break;
                    }
                }
            }
            iterator.close();
        } catch (Exception e) {
            throw new MigrationException(
                    MigrationErrorCode.VALIDATION_QUERY_FAILED,
                    "Failed to sample from Milvus: " + e.getMessage(),
                    e);
        }
        return all;
    }

    private Map<String, Object> queryPgRow(String pkName, Object pkVal) throws SQLException {
        StringBuilder cols = new StringBuilder();
        for (MigrationSchema.ColumnDef col : schema.getColumns()) {
            if (cols.length() > 0) {
                cols.append(", ");
            }
            cols.append("\"").append(col.getName()).append("\"");
        }
        String sql =
                "SELECT "
                        + cols
                        + " FROM \""
                        + pgSchema
                        + "\".\""
                        + pgTable
                        + "\" WHERE \""
                        + pkName
                        + "\" = ?";
        try (PreparedStatement ps = pgConnection.prepareStatement(sql)) {
            ps.setObject(1, pkVal);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                java.util.HashMap<String, Object> row = new java.util.HashMap<>();
                for (MigrationSchema.ColumnDef col : schema.getColumns()) {
                    row.put(col.getName(), rs.getObject(col.getName()));
                }
                return row;
            }
        }
    }

    private List<String> compareRow(
            QueryResultsWrapper.RowRecord milvusRow, Map<String, Object> pgRow, Object pkVal) {
        List<String> mismatches = new ArrayList<>();
        if (schema.getColumns() == null) {
            return mismatches;
        }
        for (MigrationSchema.ColumnDef col : schema.getColumns()) {
            String name = col.getName();
            Object milvusVal = milvusRow.get(name);
            Object pgVal = pgRow.get(name);
            if (!valuesMatch(milvusVal, pgVal, col.getDataType())) {
                mismatches.add(
                        "pk=" + pkVal + " col=" + name + ": milvus=" + truncate(milvusVal) + " pg=" + truncate(pgVal));
            }
        }
        return mismatches;
    }

    private boolean valuesMatch(Object milvusVal, Object pgVal, DataType dataType) {
        if (milvusVal == null && pgVal == null) {
            return true;
        }
        if (milvusVal == null || pgVal == null) {
            return false;
        }
        switch (dataType) {
            case FloatVector:
            case Float16Vector:
            case BFloat16Vector:
            case Int8Vector:
                return vectorsMatch(milvusVal, pgVal, false);
            case BinaryVector:
                return vectorsMatch(milvusVal, pgVal, true);
            case SparseFloatVector:
                return sparseMatch(milvusVal, pgVal);
            case Bool:
            case Int8:
            case Int16:
            case Int32:
            case Int64:
            case Float:
            case Double:
            case String:
            case VarChar:
            case JSON:
            case Geometry:
            case Timestamptz:
                return scalarMatch(milvusVal, pgVal);
            case Array:
                return arrayMatch(milvusVal, pgVal);
            default:
                return scalarMatch(milvusVal, pgVal);
        }
    }

    private boolean vectorsMatch(Object milvusVal, Object pgVal, boolean exact) {
        float[] mv = toFloatArray(milvusVal);
        float[] pv = VectorSimilarityValidator.parsePgVectorText(String.valueOf(pgVal));
        if (mv.length != pv.length) {
            return false;
        }
        if (exact) {
            for (int i = 0; i < mv.length; i++) {
                if (mv[i] != pv[i]) {
                    return false;
                }
            }
            return true;
        }
        for (int i = 0; i < mv.length; i++) {
            if (Math.abs(mv[i] - pv[i]) > VECTOR_TOLERANCE) {
                return false;
            }
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    private float[] toFloatArray(Object val) {
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
        return new float[0];
    }

    @SuppressWarnings("unchecked")
    private boolean sparseMatch(Object milvusVal, Object pgVal) {
        if (milvusVal instanceof Map) {
            float[] mv = sparseToDense((Map<?, ?>) milvusVal);
            float[] pv = VectorSimilarityValidator.parsePgVectorText(String.valueOf(pgVal));
            if (mv.length != pv.length) {
                return false;
            }
            for (int i = 0; i < mv.length; i++) {
                if (Math.abs(mv[i] - pv[i]) > VECTOR_TOLERANCE) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    private static float[] sparseToDense(Map<?, ?> map) {
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

    private boolean scalarMatch(Object milvusVal, Object pgVal) {
        return String.valueOf(milvusVal).equals(String.valueOf(pgVal));
    }

    @SuppressWarnings("unchecked")
    private boolean arrayMatch(Object milvusVal, Object pgVal) {
        if (milvusVal instanceof List && pgVal instanceof java.sql.Array) {
            try {
                Object[] pgArray = (Object[]) ((java.sql.Array) pgVal).getArray();
                List<?> milvusList = (List<?>) milvusVal;
                if (milvusList.size() != pgArray.length) {
                    return false;
                }
                for (int i = 0; i < pgArray.length; i++) {
                    if (!String.valueOf(milvusList.get(i)).equals(String.valueOf(pgArray[i]))) {
                        return false;
                    }
                }
                return true;
            } catch (SQLException e) {
                return false;
            }
        }
        return scalarMatch(milvusVal, pgVal);
    }

    private static String truncate(Object val) {
        String s = String.valueOf(val);
        return s.length() > 80 ? s.substring(0, 80) + "..." : s;
    }
}
