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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MilvusIndexConverterTest {

    private static final String TABLE = "my_table";
    private static final String SCHEMA = "public";
    private static final String COLUMN = "embedding";

    private MigrationSchema.IndexDef buildIndex(
            IndexParam.IndexType indexType,
            IndexParam.MetricType metricType,
            Map<String, Object> extraParams) {
        return MigrationSchema.IndexDef.builder()
                .fieldName(COLUMN)
                .indexName("idx_" + COLUMN)
                .indexType(indexType)
                .metricType(metricType)
                .extraParams(extraParams)
                .build();
    }

    // ---- HNSW ----

    @Test
    public void testHnswWithExplicitParams() {
        Map<String, Object> params = new HashMap<>();
        params.put("M", 32);
        params.put("efConstruction", 200);
        MigrationSchema.IndexDef idx = buildIndex(IndexParam.IndexType.HNSW, IndexParam.MetricType.COSINE, params);

        String ddl = MilvusIndexConverter.convert(idx, TABLE, SCHEMA, COLUMN);

        assertNotNull(ddl);
        assertTrue(ddl.contains("USING hnsw"), "Should use hnsw index");
        assertTrue(ddl.contains("vector_cosine_ops"), "Should use cosine ops");
        assertTrue(ddl.contains("m = 32"), "Should have m=32");
        assertTrue(ddl.contains("ef_construction = 200"), "Should have ef_construction=200");
        assertTrue(ddl.contains("\"public\".\"my_table\""), "Should qualify table name");
    }

    @Test
    public void testHnswWithLowerCaseParams() {
        Map<String, Object> params = new HashMap<>();
        params.put("m", 8);
        params.put("ef_construction", 128);
        MigrationSchema.IndexDef idx = buildIndex(IndexParam.IndexType.HNSW, IndexParam.MetricType.L2, params);

        String ddl = MilvusIndexConverter.convert(idx, TABLE, SCHEMA, COLUMN);

        assertNotNull(ddl);
        assertTrue(ddl.contains("m = 8"));
        assertTrue(ddl.contains("ef_construction = 128"));
        assertTrue(ddl.contains("vector_l2_ops"));
    }

    @Test
    public void testHnswWithMissingParamsUsesDefaults() {
        MigrationSchema.IndexDef idx = buildIndex(IndexParam.IndexType.HNSW, IndexParam.MetricType.IP, null);

        String ddl = MilvusIndexConverter.convert(idx, TABLE, SCHEMA, COLUMN);

        assertNotNull(ddl);
        assertTrue(ddl.contains("m = " + MilvusIndexConverter.DEFAULT_HNSW_M));
        assertTrue(ddl.contains("ef_construction = " + MilvusIndexConverter.DEFAULT_HNSW_EF_CONSTRUCTION));
        assertTrue(ddl.contains("vector_ip_ops"));
    }

    // ---- IVF_FLAT / IVF_SQ8 ----

    @Test
    public void testIvfFlatWithNlist() {
        Map<String, Object> params = new HashMap<>();
        params.put("nlist", 256);
        MigrationSchema.IndexDef idx = buildIndex(IndexParam.IndexType.IVF_FLAT, IndexParam.MetricType.L2, params);

        String ddl = MilvusIndexConverter.convert(idx, TABLE, SCHEMA, COLUMN);

        assertNotNull(ddl);
        assertTrue(ddl.contains("USING ivfflat"));
        assertTrue(ddl.contains("lists = 256"));
        assertTrue(ddl.contains("vector_l2_ops"));
    }

    @Test
    public void testIvfSq8WithNlist() {
        Map<String, Object> params = new HashMap<>();
        params.put("nlist", 512);
        MigrationSchema.IndexDef idx = buildIndex(IndexParam.IndexType.IVF_SQ8, IndexParam.MetricType.COSINE, params);

        String ddl = MilvusIndexConverter.convert(idx, TABLE, SCHEMA, COLUMN);

        assertNotNull(ddl);
        assertTrue(ddl.contains("USING ivfflat"));
        assertTrue(ddl.contains("lists = 512"));
    }

    @Test
    public void testIvfFlatWithMissingNlistUsesDefault() {
        MigrationSchema.IndexDef idx = buildIndex(IndexParam.IndexType.IVF_FLAT, IndexParam.MetricType.L2, null);

        String ddl = MilvusIndexConverter.convert(idx, TABLE, SCHEMA, COLUMN);

        assertNotNull(ddl);
        assertTrue(ddl.contains("lists = " + MilvusIndexConverter.DEFAULT_IVF_LISTS));
    }

    // ---- IVF_PQ / SCANN ----

    @Test
    public void testIvfPqMapsToIvfflatWithDefaults() {
        MigrationSchema.IndexDef idx = buildIndex(IndexParam.IndexType.IVF_PQ, IndexParam.MetricType.L2, null);

        String ddl = MilvusIndexConverter.convert(idx, TABLE, SCHEMA, COLUMN);

        assertNotNull(ddl);
        assertTrue(ddl.contains("USING ivfflat"));
        assertTrue(ddl.contains("lists = " + MilvusIndexConverter.DEFAULT_IVF_LISTS));
    }

    @Test
    public void testScannMapsToIvfflatWithDefaults() {
        MigrationSchema.IndexDef idx = buildIndex(IndexParam.IndexType.SCANN, IndexParam.MetricType.IP, null);

        String ddl = MilvusIndexConverter.convert(idx, TABLE, SCHEMA, COLUMN);

        assertNotNull(ddl);
        assertTrue(ddl.contains("USING ivfflat"));
    }

    // ---- FLAT ----

    @Test
    public void testFlatReturnsNull() {
        MigrationSchema.IndexDef idx = buildIndex(IndexParam.IndexType.FLAT, IndexParam.MetricType.L2, null);
        assertNull(MilvusIndexConverter.convert(idx, TABLE, SCHEMA, COLUMN));
    }

    // ---- AUTOINDEX ----

    @Test
    public void testAutoIndexMapsToHnswWithDefaults() {
        MigrationSchema.IndexDef idx = buildIndex(IndexParam.IndexType.AUTOINDEX, IndexParam.MetricType.COSINE, null);

        String ddl = MilvusIndexConverter.convert(idx, TABLE, SCHEMA, COLUMN);

        assertNotNull(ddl);
        assertTrue(ddl.contains("USING hnsw"));
        assertTrue(ddl.contains("m = " + MilvusIndexConverter.DEFAULT_HNSW_M));
        assertTrue(ddl.contains("ef_construction = " + MilvusIndexConverter.DEFAULT_HNSW_EF_CONSTRUCTION));
    }

    // ---- Unsupported index types ----

    @Test
    public void testDiskannReturnsNull() {
        MigrationSchema.IndexDef idx = buildIndex(IndexParam.IndexType.DISKANN, IndexParam.MetricType.L2, null);
        assertNull(MilvusIndexConverter.convert(idx, TABLE, SCHEMA, COLUMN));
    }

    @Test
    public void testGpuIvfFlatReturnsNull() {
        MigrationSchema.IndexDef idx = buildIndex(IndexParam.IndexType.GPU_IVF_FLAT, IndexParam.MetricType.L2, null);
        assertNull(MilvusIndexConverter.convert(idx, TABLE, SCHEMA, COLUMN));
    }

    @Test
    public void testBinFlatReturnsNull() {
        MigrationSchema.IndexDef idx = buildIndex(IndexParam.IndexType.BIN_FLAT, IndexParam.MetricType.HAMMING, null);
        assertNull(MilvusIndexConverter.convert(idx, TABLE, SCHEMA, COLUMN));
    }

    @Test
    public void testSparseInvertedReturnsNull() {
        MigrationSchema.IndexDef idx = buildIndex(IndexParam.IndexType.SPARSE_INVERTED_INDEX, IndexParam.MetricType.IP, null);
        assertNull(MilvusIndexConverter.convert(idx, TABLE, SCHEMA, COLUMN));
    }

    @Test
    public void testTrieReturnsNull() {
        MigrationSchema.IndexDef idx = buildIndex(IndexParam.IndexType.TRIE, null, null);
        assertNull(MilvusIndexConverter.convert(idx, TABLE, SCHEMA, COLUMN));
    }

    @Test
    public void testNullIndexTypeReturnsNull() {
        MigrationSchema.IndexDef idx = buildIndex(null, IndexParam.MetricType.L2, null);
        assertNull(MilvusIndexConverter.convert(idx, TABLE, SCHEMA, COLUMN));
    }

    // ---- metric → ops ----

    @Test
    public void testMetricToOpsL2() {
        assertEquals("vector_l2_ops", MilvusIndexConverter.metricToOps(IndexParam.MetricType.L2));
    }

    @Test
    public void testMetricToOpsIP() {
        assertEquals("vector_ip_ops", MilvusIndexConverter.metricToOps(IndexParam.MetricType.IP));
    }

    @Test
    public void testMetricToOpsCosine() {
        assertEquals("vector_cosine_ops", MilvusIndexConverter.metricToOps(IndexParam.MetricType.COSINE));
    }

    @Test
    public void testMetricToOpsHamming() {
        assertEquals("bit_hamming_ops", MilvusIndexConverter.metricToOps(IndexParam.MetricType.HAMMING));
    }

    @Test
    public void testMetricToOpsJaccard() {
        assertEquals("bit_jaccard_ops", MilvusIndexConverter.metricToOps(IndexParam.MetricType.JACCARD));
    }

    @Test
    public void testMetricToOpsNullDefaultsToL2() {
        assertEquals("vector_l2_ops", MilvusIndexConverter.metricToOps(null));
    }

    // ---- identifier quoting ----

    @Test
    public void testIndexNameQuotedInDdl() {
        MigrationSchema.IndexDef idx = buildIndex(IndexParam.IndexType.HNSW, IndexParam.MetricType.L2, null);
        String ddl = MilvusIndexConverter.convert(idx, TABLE, SCHEMA, COLUMN);
        assertNotNull(ddl);
        assertTrue(ddl.contains("\"idx_embedding\""), "Index name should be quoted");
    }

    @Test
    public void testColumnNameQuotedInDdl() {
        MigrationSchema.IndexDef idx = buildIndex(IndexParam.IndexType.HNSW, IndexParam.MetricType.L2, null);
        String ddl = MilvusIndexConverter.convert(idx, TABLE, SCHEMA, COLUMN);
        assertNotNull(ddl);
        assertTrue(ddl.contains("\"embedding\""), "Column name should be quoted");
    }

    @Test
    public void testNullSchemaProducesUnqualifiedTable() {
        MigrationSchema.IndexDef idx = buildIndex(IndexParam.IndexType.HNSW, IndexParam.MetricType.L2, null);
        String ddl = MilvusIndexConverter.convert(idx, TABLE, null, COLUMN);
        assertNotNull(ddl);
        assertTrue(ddl.contains("\"my_table\""), "Table should be quoted without schema prefix");
        assertTrue(!ddl.contains(".\"my_table\""), "Should not have schema qualifier");
    }

    @Test
    public void testEmptyIndexNameUsesColumnBasedDefault() {
        MigrationSchema.IndexDef idx =
                MigrationSchema.IndexDef.builder()
                        .fieldName(COLUMN)
                        .indexName(null)
                        .indexType(IndexParam.IndexType.HNSW)
                        .metricType(IndexParam.MetricType.L2)
                        .extraParams(null)
                        .build();
        String ddl = MilvusIndexConverter.convert(idx, TABLE, SCHEMA, COLUMN);
        assertNotNull(ddl);
        assertTrue(ddl.contains("idx_embedding"), "Should use idx_<column> when index name is null");
    }
}
