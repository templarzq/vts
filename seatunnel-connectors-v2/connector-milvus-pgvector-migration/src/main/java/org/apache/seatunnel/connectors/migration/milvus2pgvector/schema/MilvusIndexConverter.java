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

package org.apache.seatunnel.connectors.migration.milvus2pgvector.schema;

import io.milvus.v2.common.IndexParam;

import lombok.extern.slf4j.Slf4j;

import java.util.Locale;
import java.util.Map;

/**
 * Converts a Milvus index definition into a pgvector {@code CREATE INDEX} DDL string.
 *
 * <p>Mapping policy:
 *
 * <ul>
 *   <li>HNSW → {@code USING hnsw (col <ops>) WITH (m=M, ef_construction=EF)}
 *   <li>IVF_FLAT, IVF_SQ8 → {@code USING ivfflat (col <ops>) WITH (lists=NLIST)}
 *   <li>IVF_PQ, SCANN → IVFFlat with default lists=100, logs warning
 *   <li>FLAT → no index (exact search)
 *   <li>AUTOINDEX → HNSW with defaults
 *   <li>DISKANN, GPU_*, BIN_*, SPARSE_* → skipped with warning (pgvector has no equivalent)
 * </ul>
 *
 * <p>Metric → ops class:
 *
 * <ul>
 *   <li>L2 → {@code vector_l2_ops}
 *   <li>IP → {@code vector_ip_ops}
 *   <li>COSINE → {@code vector_cosine_ops}
 * </ul>
 */
@Slf4j
public final class MilvusIndexConverter {

    /** Default lists for IVFFlat when nlist isn't captured. */
    public static final int DEFAULT_IVF_LISTS = 100;
    public static final int DEFAULT_HNSW_M = 16;
    public static final int DEFAULT_HNSW_EF_CONSTRUCTION = 64;

    private MilvusIndexConverter() {}

    /**
     * @return the CREATE INDEX DDL, or {@code null} if the index type should be skipped (caller
     *     should treat null as "no DDL needed, just log").
     */
    public static String convert(
            MigrationSchema.IndexDef index, String tableName, String schemaName, String pgColumn) {
        IndexParam.IndexType milvusType = index.getIndexType();
        if (milvusType == null) {
            log.warn("Index {} has no IndexType; skipping", index.getIndexName());
            return null;
        }
        String ops = metricToOps(index.getMetricType());
        String qualifiedTable = quoteQualifiedTable(schemaName, tableName);
        String indexName = sanitizeIndexName(index.getIndexName(), pgColumn);

        switch (milvusType) {
            case HNSW:
                return buildHnsw(qualifiedTable, pgColumn, indexName, ops, index.getExtraParams());
            case IVF_FLAT:
            case IVF_SQ8:
                return buildIvfflat(qualifiedTable, pgColumn, indexName, ops, index.getExtraParams());
            case IVF_PQ:
            case SCANN:
                log.warn(
                        "Milvus {} index on {} maps loosely to pgvector IVFFlat (loses PQ compression); "
                                + "using default lists={}",
                        milvusType,
                        pgColumn,
                        DEFAULT_IVF_LISTS);
                return buildIvfflatWithDefaults(qualifiedTable, pgColumn, indexName, ops);
            case FLAT:
                log.info("Milvus FLAT index on {} → no pgvector index (exact search)", pgColumn);
                return null;
            case AUTOINDEX:
                log.info(
                        "Milvus AUTOINDEX on {} → pgvector HNSW with defaults (m={}, ef_construction={})",
                        pgColumn,
                        DEFAULT_HNSW_M,
                        DEFAULT_HNSW_EF_CONSTRUCTION);
                return buildHnswWithDefaults(qualifiedTable, pgColumn, indexName, ops);
            case DISKANN:
            case GPU_IVF_FLAT:
            case GPU_IVF_PQ:
            case GPU_BRUTE_FORCE:
            case GPU_CAGRA:
            case BIN_FLAT:
            case BIN_IVF_FLAT:
            case SPARSE_INVERTED_INDEX:
            case SPARSE_WAND:
            case TRIE:
            case STL_SORT:
            case INVERTED:
                log.warn(
                        "Milvus {} index on {} has no pgvector equivalent; skipping",
                        milvusType,
                        pgColumn);
                return null;
            default:
                log.warn("Unknown Milvus index type {} on {}; skipping", milvusType, pgColumn);
                return null;
        }
    }

    static String metricToOps(IndexParam.MetricType metric) {
        if (metric == null) {
            return "vector_l2_ops";
        }
        switch (metric) {
            case L2:
                return "vector_l2_ops";
            case IP:
                return "vector_ip_ops";
            case COSINE:
                return "vector_cosine_ops";
            case HAMMING:
            case JACCARD:
                // bit vectors use the same ops classes in pgvector (bit operators are
                // hamming / jaccard) — but pgvector currently exposes only bit_hamming_ops and
                // bit_jaccard_ops as of 0.7. Fall back to bit_hamming_ops for either.
                return metric == IndexParam.MetricType.HAMMING
                        ? "bit_hamming_ops"
                        : "bit_jaccard_ops";
            default:
                return "vector_l2_ops";
        }
    }

    private static String buildHnsw(
            String table, String column, String indexName, String ops, Map<String, Object> params) {
        int m = getIntParamOrDefault(params, DEFAULT_HNSW_M, "M", "m");
        int ef = getIntParamOrDefault(params, DEFAULT_HNSW_EF_CONSTRUCTION, "efConstruction", "ef_construction");
        return String.format(
                "CREATE INDEX IF NOT EXISTS %s ON %s USING hnsw (%s %s) WITH (m = %d, ef_construction = %d);",
                quoteIdent(indexName), table, quoteIdent(column), ops, m, ef);
    }

    private static String buildHnswWithDefaults(
            String table, String column, String indexName, String ops) {
        return String.format(
                "CREATE INDEX IF NOT EXISTS %s ON %s USING hnsw (%s %s) WITH (m = %d, ef_construction = %d);",
                quoteIdent(indexName), table, quoteIdent(column), ops, DEFAULT_HNSW_M, DEFAULT_HNSW_EF_CONSTRUCTION);
    }

    private static String buildIvfflat(
            String table, String column, String indexName, String ops, Map<String, Object> params) {
        int lists = getIntParamOrDefault(params, DEFAULT_IVF_LISTS, "nlist", "NLIST");
        return String.format(
                "CREATE INDEX IF NOT EXISTS %s ON %s USING ivfflat (%s %s) WITH (lists = %d);",
                quoteIdent(indexName), table, quoteIdent(column), ops, lists);
    }

    private static String buildIvfflatWithDefaults(
            String table, String column, String indexName, String ops) {
        return String.format(
                "CREATE INDEX IF NOT EXISTS %s ON %s USING ivfflat (%s %s) WITH (lists = %d);",
                quoteIdent(indexName), table, quoteIdent(column), ops, DEFAULT_IVF_LISTS);
    }

    /**
     * Look up an integer-valued index parameter by trying multiple case-insensitive key variants.
     */
    static int getIntParam(Map<String, Object> params, String... candidateKeys) {
        if (params == null) {
            return -1;
        }
        for (String key : candidateKeys) {
            for (Map.Entry<String, Object> e : params.entrySet()) {
                if (e.getKey() != null && e.getKey().equalsIgnoreCase(key) && e.getValue() != null) {
                    try {
                        return Integer.parseInt(e.getValue().toString());
                    } catch (NumberFormatException ignored) {
                    }
                }
            }
        }
        // Try the first candidate as default fallback indicator
        return -1;
    }

    /** Use the first candidate key that returns a positive value, else fallback. */
    static int getIntParamOrDefault(Map<String, Object> params, int fallback, String... candidateKeys) {
        int v = getIntParam(params, candidateKeys);
        return v > 0 ? v : fallback;
    }

    static String sanitizeIndexName(String indexName, String column) {
        if (indexName == null || indexName.isEmpty()) {
            return "idx_" + column.toLowerCase(Locale.ROOT);
        }
        return indexName;
    }

    static String quoteIdent(String ident) {
        return "\"" + ident.replace("\"", "\"\"") + "\"";
    }

    static String quoteQualifiedTable(String schema, String table) {
        if (schema == null || schema.isEmpty()) {
            return quoteIdent(table);
        }
        return quoteIdent(schema) + "." + quoteIdent(table);
    }
}
