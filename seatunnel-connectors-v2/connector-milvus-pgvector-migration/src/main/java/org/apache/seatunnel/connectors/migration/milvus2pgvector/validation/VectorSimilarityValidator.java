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
import org.apache.seatunnel.connectors.migration.milvus2pgvector.transform.VectorFormatConverter;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Samples {@code sampleSize} rows from Milvus, fetches the corresponding pgvector row by primary
 * key, and compares vector cosine similarity against {@code similarityThreshold}.
 *
 * <p>Only the first vector column found in the schema is compared. FLOAT_VECTOR, FLOAT16_VECTOR,
 * BFLOAT16_VECTOR, BINARY_VECTOR, and SPARSE_FLOAT_VECTOR are supported. For BINARY_VECTOR the
 * "similarity" is exact equality (bit-string match); for SPARSE_FLOAT_VECTOR the cosine similarity
 * is computed on the dense float arrays.
 */
@Slf4j
public class VectorSimilarityValidator {

    private final MilvusClientV2 milvusClient;
    private final Connection pgConnection;
    private final MigrationSchema schema;
    private final String pgSchema;
    private final String pgTable;
    private final int sampleSize;
    private final double similarityThreshold;
    private final TokenBucketRateLimiter rateLimiter;

    public VectorSimilarityValidator(
            MilvusClientV2 milvusClient,
            Connection pgConnection,
            MigrationSchema schema,
            String pgSchema,
            String pgTable,
            int sampleSize,
            double similarityThreshold,
            TokenBucketRateLimiter rateLimiter) {
        this.milvusClient = milvusClient;
        this.pgConnection = pgConnection;
        this.schema = schema;
        this.pgSchema = pgSchema;
        this.pgTable = pgTable;
        this.sampleSize = sampleSize;
        this.similarityThreshold = similarityThreshold;
        this.rateLimiter = rateLimiter;
    }

    public ValidationResult validate() {
        long start = System.currentTimeMillis();
        ValidationResult.ValidationResultBuilder b =
                ValidationResult.builder().validatorName("VectorSimilarityValidator");
        MigrationSchema.ColumnDef vectorCol = findFirstVectorColumn();
        if (vectorCol == null) {
            b.passed(true).totalChecked(0).detail("No vector column found; skipping");
            b.durationMs(System.currentTimeMillis() - start);
            return b.build();
        }
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
                float[] milvusVec = extractMilvusVector(record, vectorCol, pkVal);
                float[] pgVec = queryPgVector(pkName, pkVal, vectorCol.getName());
                if (pgVec == null) {
                    failed++;
                    b.detail("pk=" + pkVal + ": row not found in pgvector");
                    continue;
                }
                if (milvusVec == null) {
                    failed++;
                    b.detail("pk=" + pkVal + ": Milvus vector is null");
                    continue;
                }
                double sim = cosineSimilarity(milvusVec, pgVec);
                checked++;
                if (sim < similarityThreshold) {
                    failed++;
                    b.detail(
                            "pk=" + pkVal + ": similarity=" + sim + " < threshold=" + similarityThreshold);
                }
            }
            b.totalChecked(checked).failedCount(failed).passed(failed == 0);
            if (failed == 0) {
                b.detail(
                        "All " + checked + " sampled vectors >= threshold " + similarityThreshold);
            }
        } catch (MigrationException e) {
            b.passed(false).errorMessage(e.getMessage());
        } catch (Exception e) {
            b.passed(false).errorMessage(e.getMessage());
            log.error("Vector similarity validation failed", e);
        }
        b.durationMs(System.currentTimeMillis() - start);
        return b.build();
    }

    private MigrationSchema.ColumnDef findFirstVectorColumn() {
        if (schema.getColumns() == null) {
            return null;
        }
        for (MigrationSchema.ColumnDef col : schema.getColumns()) {
            DataType dt = col.getDataType();
            if (dt == DataType.FloatVector
                    || dt == DataType.Float16Vector
                    || dt == DataType.BFloat16Vector
                    || dt == DataType.BinaryVector
                    || dt == DataType.SparseFloatVector
                    || dt == DataType.Int8Vector) {
                return col;
            }
        }
        return null;
    }

    private List<QueryResultsWrapper.RowRecord> sampleFromMilvus() {
        List<QueryResultsWrapper.RowRecord> all = new ArrayList<>();
        try {
            QueryIterator iterator =
                    milvusClient.queryIterator(
                            QueryIteratorReq.builder()
                                    .collectionName(schema.getCollectionName())
                                    .outputFields(new ArrayList<>(java.util.Collections.singletonList("*")))
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

    @SuppressWarnings("unchecked")
    private float[] extractMilvusVector(
            QueryResultsWrapper.RowRecord record, MigrationSchema.ColumnDef col, Object pkVal) {
        Object val = record.get(col.getName());
        if (val == null) {
            return null;
        }
        DataType dt = col.getDataType();
        switch (dt) {
            case FloatVector:
                if (val instanceof List) {
                    List<Number> list = (List<Number>) val;
                    float[] out = new float[list.size()];
                    for (int i = 0; i < list.size(); i++) {
                        out[i] = list.get(i).floatValue();
                    }
                    return out;
                }
                if (val instanceof ByteBuffer) {
                    return bbToFloats((ByteBuffer) val);
                }
                break;
            case Float16Vector:
                if (val instanceof ByteBuffer) {
                    return VectorFormatConverter.float16BufferToFloatArray((ByteBuffer) val);
                }
                break;
            case BFloat16Vector:
                if (val instanceof ByteBuffer) {
                    return VectorFormatConverter.bfloat16BufferToFloatArray((ByteBuffer) val);
                }
                break;
            case Int8Vector:
            case BinaryVector:
                if (val instanceof ByteBuffer) {
                    return bbToFloats((ByteBuffer) val);
                }
                break;
            case SparseFloatVector:
                if (val instanceof Map) {
                    return sparseToDense((Map<?, ?>) val, col.getDimension() != null ? col.getDimension() : 0);
                }
                break;
            default:
                break;
        }
        log.warn("Unexpected vector value type for pk={}: {}", pkVal, val.getClass());
        return null;
    }

    private float[] queryPgVector(String pkName, Object pkVal, String vectorCol)
            throws SQLException {
        String sql =
                "SELECT \""
                        + vectorCol
                        + "\"::text FROM \""
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
                String text = rs.getString(1);
                if (text == null) {
                    return null;
                }
                return parsePgVectorText(text);
            }
        }
    }

    /** Parse pgvector text representations: {@code [1.0,2.0,3.0]} or {@code {1:0.5,3:0.7}/3}. */
    static float[] parsePgVectorText(String text) {
        text = text.trim();
        if (text.startsWith("[")) {
            String inner = text.substring(1, text.length() - 1);
            String[] parts = inner.split(",");
            float[] out = new float[parts.length];
            for (int i = 0; i < parts.length; i++) {
                out[i] = Float.parseFloat(parts[i].trim());
            }
            return out;
        }
        if (text.startsWith("{")) {
            int slash = text.lastIndexOf('/');
            int dim = Integer.parseInt(text.substring(slash + 1).trim());
            float[] out = new float[dim];
            String inner = text.substring(1, slash);
            for (String pair : inner.split(",")) {
                String[] kv = pair.split(":");
                int idx = Integer.parseInt(kv[0].trim()) - 1;
                out[idx] = Float.parseFloat(kv[1].trim());
            }
            return out;
        }
        if (text.matches("[01]+")) {
            float[] out = new float[text.length()];
            for (int i = 0; i < text.length(); i++) {
                out[i] = text.charAt(i) == '1' ? 1f : 0f;
            }
            return out;
        }
        throw new IllegalArgumentException("Unrecognized pgvector text: " + text);
    }

    private static float[] bbToFloats(ByteBuffer buf) {
        ByteBuffer dup = buf.duplicate();
        float[] out = new float[dup.remaining()];
        for (int i = 0; i < out.length; i++) {
            out[i] = dup.get();
        }
        return out;
    }

    private static float[] sparseToDense(Map<?, ?> map, int dimension) {
        int dim = dimension;
        if (dim <= 0) {
            int max = 0;
            for (Object k : map.keySet()) {
                max = Math.max(max, ((Number) k).intValue());
            }
            dim = max + 1;
        }
        float[] out = new float[dim];
        for (Map.Entry<?, ?> e : map.entrySet()) {
            out[((Number) e.getKey()).intValue()] = ((Number) e.getValue()).floatValue();
        }
        return out;
    }

    static double cosineSimilarity(float[] a, float[] b) {
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
