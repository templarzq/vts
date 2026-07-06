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
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.response.QueryResp;

import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.exception.MigrationErrorCode;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.exception.MigrationException;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.internal.TokenBucketRateLimiter;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.schema.MigrationSchema;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.transform.VectorFormatConverter;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Validates data consistency by comparing sampled rows between Milvus and pgvector
 * across ALL fields (scalar + vector), not just vectors.
 *
 * <p>Reports per-field mismatches with expected (Milvus) and actual (pgvector) values.
 * Pass threshold is configurable via {@code passRateThreshold} (e.g. 0.99 = 99% pass).
 */
@Slf4j
public class FieldComparisonValidator {

    private final MilvusClientV2 milvusClient;
    private final Connection pgConnection;
    private final MigrationSchema schema;
    private final String pgSchema;
    private final String pgTable;
    private final String collectionName;
    private final int sampleSize;
    private final double passRateThreshold;
    private final TokenBucketRateLimiter rateLimiter;

    public FieldComparisonValidator(
            MilvusClientV2 milvusClient,
            Connection pgConnection,
            MigrationSchema schema,
            String pgSchema,
            String pgTable,
            int sampleSize,
            double passRateThreshold,
            TokenBucketRateLimiter rateLimiter) {
        this.milvusClient = milvusClient;
        this.pgConnection = pgConnection;
        this.schema = schema;
        this.pgSchema = pgSchema;
        this.pgTable = pgTable;
        this.collectionName = schema.getCollectionName();
        this.sampleSize = sampleSize;
        this.passRateThreshold = passRateThreshold;
        this.rateLimiter = rateLimiter;
    }

    public ValidationResult validate() {
        long start = System.currentTimeMillis();
        String pkField = schema.getPrimaryKeyName();
        if (pkField == null) {
            return ValidationResult.builder()
                    .validatorName("FieldComparison")
                    .passed(false)
                    .errorMessage("No primary key defined in schema")
                    .durationMs(System.currentTimeMillis() - start)
                    .build();
        }

        List<Object> sampleIds = getSampleIds(pkField);
        if (sampleIds.isEmpty()) {
            return ValidationResult.builder()
                    .validatorName("FieldComparison")
                    .passed(true)
                    .totalChecked(0)
                    .failedCount(0)
                    .detail("No data in target table — nothing to compare")
                    .durationMs(System.currentTimeMillis() - start)
                    .build();
        }

        int matched = 0;
        int mismatched = 0;
        List<String> mismatchDetails = new ArrayList<>();

        for (Object id : sampleIds) {
            try {
                if (rateLimiter != null) {
                    rateLimiter.acquire(1);
                }
                List<String> rowMismatches = compareRow(pkField, id);
                if (rowMismatches.isEmpty()) {
                    matched++;
                } else {
                    mismatched++;
                    mismatchDetails.add("PK " + id + ": " + String.join(", ", rowMismatches));
                }
            } catch (Exception e) {
                mismatched++;
                mismatchDetails.add("PK " + id + ": query error — " + e.getMessage());
            }
        }

        int totalChecked = matched + mismatched;
        double passRate = totalChecked > 0 ? (double) matched / totalChecked : 1.0;
        boolean passed = passRate >= passRateThreshold;

        return ValidationResult.builder()
                .validatorName("FieldComparison")
                .passed(passed)
                .totalChecked(totalChecked)
                .failedCount(mismatched)
                .details(mismatchDetails)
                .errorMessage(passed ? null
                        : String.format("Pass rate %.2f%% below threshold %.2f%% (%d/%d mismatched)",
                                passRate * 100, passRateThreshold * 100, mismatched, totalChecked))
                .durationMs(System.currentTimeMillis() - start)
                .build();
    }

    /** Get random sample IDs from the pgvector target table. */
    private List<Object> getSampleIds(String pkField) {
        String qualified = pgSchema != null && !pgSchema.isEmpty()
                ? "\"" + pgSchema + "\".\"" + pgTable + "\""
                : "\"" + pgTable + "\"";
        String sql = "SELECT \"" + pkField + "\" FROM " + qualified
                + " ORDER BY RANDOM() LIMIT " + sampleSize;

        List<Object> ids = new ArrayList<>();
        try (PreparedStatement stmt = pgConnection.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                ids.add(rs.getObject(1));
            }
        } catch (SQLException e) {
            throw new MigrationException(
                    MigrationErrorCode.VALIDATION_QUERY_FAILED,
                    "Failed to get sample IDs from pgvector: " + e.getMessage(), e);
        }
        log.info("Got {} sample IDs for field comparison", ids.size());
        return ids;
    }

    /**
     * Compare a single row across all fields between Milvus and pgvector.
     * Returns a list of mismatch descriptions (empty = row matches).
     */
    private List<String> compareRow(String pkField, Object pkValue) {
        List<String> mismatches = new ArrayList<>();

        // Fetch from Milvus
        String filter = pkField + " == " + formatMilvusFilter(pkValue);
        QueryResp milvusResp = milvusClient.query(QueryReq.builder()
                .collectionName(collectionName)
                .filter(filter)
                .outputFields(Collections.singletonList("*"))
                .limit(1)
                .build());

        if (milvusResp.getQueryResults() == null || milvusResp.getQueryResults().isEmpty()) {
            mismatches.add("row exists in pgvector but not in Milvus");
            return mismatches;
        }
        QueryResp.QueryResult milvusRow = milvusResp.getQueryResults().get(0);

        // Fetch from pgvector
        String qualified = pgSchema != null && !pgSchema.isEmpty()
                ? "\"" + pgSchema + "\".\"" + pgTable + "\""
                : "\"" + pgTable + "\"";
        String sql = "SELECT * FROM " + qualified + " WHERE \"" + pkField + "\" = ?";
        try (PreparedStatement stmt = pgConnection.prepareStatement(sql)) {
            stmt.setObject(1, pkValue);
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    mismatches.add("row exists in Milvus but not in pgvector");
                    return mismatches;
                }

                // Compare all schema columns
                for (MigrationSchema.ColumnDef col : schema.getColumns()) {
                    if (col.getName() == null) continue;
                    Object milvusVal = milvusRow.getEntity().get(col.getName());
                    Object pgVal = rs.getObject(col.getName());
                    if (!valuesEqual(milvusVal, pgVal, col)) {
                        mismatches.add(col.getName() + ": milvus=" + truncate(milvusVal)
                                + " vs pg=" + truncate(pgVal));
                    }
                }
            }
        } catch (SQLException e) {
            throw new MigrationException(
                    MigrationErrorCode.VALIDATION_QUERY_FAILED,
                    "Failed to query pgvector for PK=" + pkValue + ": " + e.getMessage(), e);
        }
        return mismatches;
    }

    /** Compare two values, handling vector types specially. */
    private boolean valuesEqual(Object milvusVal, Object pgVal, MigrationSchema.ColumnDef col) {
        if (Objects.equals(milvusVal, pgVal)) {
            return true;
        }
        if (milvusVal == null || pgVal == null) {
            return false;
        }
        // For vector types, compare as float arrays
        if (milvusVal instanceof ByteBuffer && col.getDataType() != null) {
            float[] milvusFloats = decodeVector((ByteBuffer) milvusVal, col);
            float[] pgFloats = pgVectorToFloats(pgVal);
            if (milvusFloats != null && pgFloats != null) {
                return floatArraysEqual(milvusFloats, pgFloats);
            }
        }
        return false;
    }

    private float[] decodeVector(ByteBuffer buf, MigrationSchema.ColumnDef col) {
        try {
            switch (col.getDataType()) {
                case FloatVector:
                    float[] f = new float[buf.remaining() / 4];
                    ByteBuffer dup = buf.duplicate();
                    for (int i = 0; i < f.length; i++) f[i] = dup.getFloat();
                    return f;
                case Float16Vector:
                    return VectorFormatConverter.float16BufferToFloatArray(buf);
                case BFloat16Vector:
                    return VectorFormatConverter.bfloat16BufferToFloatArray(buf);
                default:
                    return null;
            }
        } catch (Exception e) {
            log.debug("Failed to decode vector: {}", e.getMessage());
            return null;
        }
    }

    private float[] pgVectorToFloats(Object pgVal) {
        if (pgVal instanceof float[]) return (float[]) pgVal;
        if (pgVal instanceof String) {
            String s = ((String) pgVal).replace("[", "").replace("]", "");
            String[] parts = s.split(",");
            float[] f = new float[parts.length];
            for (int i = 0; i < parts.length; i++) f[i] = Float.parseFloat(parts[i].trim());
            return f;
        }
        return null;
    }

    private boolean floatArraysEqual(float[] a, float[] b) {
        if (a.length != b.length) return false;
        for (int i = 0; i < a.length; i++) {
            if (Math.abs(a[i] - b[i]) > 1e-5f) return false;
        }
        return true;
    }

    private String formatMilvusFilter(Object pkValue) {
        if (pkValue instanceof Number) return pkValue.toString();
        return "\"" + pkValue.toString().replace("\"", "\\\"") + "\"";
    }

    private static String truncate(Object val) {
        if (val == null) return "null";
        String s = val.toString();
        if (s.length() > 80) s = s.substring(0, 77) + "...";
        return s;
    }
}
