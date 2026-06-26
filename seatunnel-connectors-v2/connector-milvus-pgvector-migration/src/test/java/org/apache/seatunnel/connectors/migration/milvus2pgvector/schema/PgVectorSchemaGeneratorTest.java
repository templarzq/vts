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

import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;

import org.apache.seatunnel.connectors.migration.milvus2pgvector.exception.MigrationException;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PgVectorSchemaGeneratorTest {

    private static final String PG_SCHEMA = "public";
    private static final String PG_TABLE = "my_collection";

    private MigrationSchema buildSimpleSchema() {
        return MigrationSchema.builder()
                .collectionName("my_collection")
                .primaryKeyName("id")
                .columns(
                        Arrays.asList(
                                MigrationSchema.ColumnDef.builder()
                                        .name("id")
                                        .dataType(DataType.Int64)
                                        .isPrimaryKey(true)
                                        .build(),
                                MigrationSchema.ColumnDef.builder()
                                        .name("embedding")
                                        .dataType(DataType.FloatVector)
                                        .dimension(128)
                                        .build(),
                                MigrationSchema.ColumnDef.builder()
                                        .name("title")
                                        .dataType(DataType.VarChar)
                                        .maxLength(255L)
                                        .build(),
                                MigrationSchema.ColumnDef.builder()
                                        .name("category")
                                        .dataType(DataType.String)
                                        .build()))
                .build();
    }

    // ---- CREATE TABLE ----

    @Test
    public void testCreateTableDdlBasic() {
        MigrationSchema schema = buildSimpleSchema();
        String ddl = PgVectorSchemaGenerator.generateCreateTableDdl(
                schema, PG_SCHEMA, PG_TABLE, false, true);

        assertTrue(ddl.contains("CREATE TABLE IF NOT EXISTS"));
        assertTrue(ddl.contains("\"public\".\"my_collection\""));
        assertTrue(ddl.contains("\"id\" BIGINT"));
        assertTrue(ddl.contains("\"embedding\" vector(128)"));
        assertTrue(ddl.contains("\"title\" VARCHAR(255)"));
        assertTrue(ddl.contains("\"category\" TEXT"));
        assertTrue(ddl.contains("PRIMARY KEY (\"id\")"));
    }

    @Test
    public void testCreateTableDdlWithDropExisting() {
        MigrationSchema schema = buildSimpleSchema();
        String ddl = PgVectorSchemaGenerator.generateCreateTableDdl(
                schema, PG_SCHEMA, PG_TABLE, true, true);

        assertTrue(ddl.contains("DROP TABLE IF EXISTS \"public\".\"my_collection\""));
        assertTrue(ddl.contains("CREATE TABLE IF NOT EXISTS"));
    }

    @Test
    public void testCreateTableDdlWithoutDropExisting() {
        MigrationSchema schema = buildSimpleSchema();
        String ddl = PgVectorSchemaGenerator.generateCreateTableDdl(
                schema, PG_SCHEMA, PG_TABLE, false, true);

        assertFalse(ddl.contains("DROP TABLE"));
    }

    @Test
    public void testCreateTableDdlWithoutPrimaryKey() {
        MigrationSchema schema = MigrationSchema.builder()
                .collectionName("no_pk_collection")
                .primaryKeyName(null)
                .columns(Collections.singletonList(
                        MigrationSchema.ColumnDef.builder()
                                .name("vec")
                                .dataType(DataType.FloatVector)
                                .dimension(64)
                                .build()))
                .build();

        String ddl = PgVectorSchemaGenerator.generateCreateTableDdl(
                schema, PG_SCHEMA, PG_TABLE, false, true);

        assertFalse(ddl.contains("PRIMARY KEY"));
    }

    @Test
    public void testCreateTableDdlWithBfloat16AndPrecisionLoss() {
        MigrationSchema schema = MigrationSchema.builder()
                .collectionName("bf16_collection")
                .primaryKeyName("id")
                .columns(Arrays.asList(
                        MigrationSchema.ColumnDef.builder()
                                .name("id")
                                .dataType(DataType.Int64)
                                .isPrimaryKey(true)
                                .build(),
                        MigrationSchema.ColumnDef.builder()
                                .name("vec")
                                .dataType(DataType.BFloat16Vector)
                                .dimension(64)
                                .build()))
                .build();

        String ddl = PgVectorSchemaGenerator.generateCreateTableDdl(
                schema, PG_SCHEMA, PG_TABLE, false, true);

        assertTrue(ddl.contains("halfvec(64)"));
    }

    @Test
    public void testCreateTableDdlWithBfloat16WithoutPrecisionLossThrows() {
        MigrationSchema schema = MigrationSchema.builder()
                .collectionName("bf16_collection")
                .primaryKeyName("id")
                .columns(Arrays.asList(
                        MigrationSchema.ColumnDef.builder()
                                .name("id")
                                .dataType(DataType.Int64)
                                .isPrimaryKey(true)
                                .build(),
                        MigrationSchema.ColumnDef.builder()
                                .name("vec")
                                .dataType(DataType.BFloat16Vector)
                                .dimension(64)
                                .build()))
                .build();

        assertThrows(MigrationException.class, () ->
                PgVectorSchemaGenerator.generateCreateTableDdl(
                        schema, PG_SCHEMA, PG_TABLE, false, false));
    }

    @Test
    public void testCreateTableDdlWithBinaryVector() {
        MigrationSchema schema = MigrationSchema.builder()
                .collectionName("bin_collection")
                .primaryKeyName("id")
                .columns(Arrays.asList(
                        MigrationSchema.ColumnDef.builder()
                                .name("id")
                                .dataType(DataType.Int64)
                                .isPrimaryKey(true)
                                .build(),
                        MigrationSchema.ColumnDef.builder()
                                .name("vec")
                                .dataType(DataType.BinaryVector)
                                .dimension(256)
                                .build()))
                .build();

        String ddl = PgVectorSchemaGenerator.generateCreateTableDdl(
                schema, PG_SCHEMA, PG_TABLE, false, true);

        assertTrue(ddl.contains("bit(256)"));
    }

    @Test
    public void testCreateTableDdlWithSparseVector() {
        MigrationSchema schema = MigrationSchema.builder()
                .collectionName("sparse_collection")
                .primaryKeyName("id")
                .columns(Arrays.asList(
                        MigrationSchema.ColumnDef.builder()
                                .name("id")
                                .dataType(DataType.Int64)
                                .isPrimaryKey(true)
                                .build(),
                        MigrationSchema.ColumnDef.builder()
                                .name("vec")
                                .dataType(DataType.SparseFloatVector)
                                .build()))
                .build();

        String ddl = PgVectorSchemaGenerator.generateCreateTableDdl(
                schema, PG_SCHEMA, PG_TABLE, false, true);

        assertTrue(ddl.contains("sparsevec"));
    }

    @Test
    public void testCreateTableDdlWithArrayColumn() {
        MigrationSchema schema = MigrationSchema.builder()
                .collectionName("array_collection")
                .primaryKeyName("id")
                .columns(Arrays.asList(
                        MigrationSchema.ColumnDef.builder()
                                .name("id")
                                .dataType(DataType.Int64)
                                .isPrimaryKey(true)
                                .build(),
                        MigrationSchema.ColumnDef.builder()
                                .name("tags")
                                .dataType(DataType.Array)
                                .elementType(DataType.VarChar)
                                .build()))
                .build();

        String ddl = PgVectorSchemaGenerator.generateCreateTableDdl(
                schema, PG_SCHEMA, PG_TABLE, false, true);

        assertTrue(ddl.contains("\"tags\" TEXT[]"));
    }

    @Test
    public void testCreateTableDdlWithJsonColumn() {
        MigrationSchema schema = MigrationSchema.builder()
                .collectionName("json_collection")
                .primaryKeyName("id")
                .columns(Arrays.asList(
                        MigrationSchema.ColumnDef.builder()
                                .name("id")
                                .dataType(DataType.Int64)
                                .isPrimaryKey(true)
                                .build(),
                        MigrationSchema.ColumnDef.builder()
                                .name("metadata")
                                .dataType(DataType.JSON)
                                .build()))
                .build();

        String ddl = PgVectorSchemaGenerator.generateCreateTableDdl(
                schema, PG_SCHEMA, PG_TABLE, false, true);

        assertTrue(ddl.contains("\"metadata\" JSONB"));
    }

    // ---- CREATE INDEX ----

    @Test
    public void testGenerateCreateIndexDdlsWithHnsw() {
        Map<String, Object> params = new HashMap<>();
        params.put("M", 16);
        params.put("efConstruction", 200);

        MigrationSchema schema = MigrationSchema.builder()
                .collectionName("my_collection")
                .primaryKeyName("id")
                .columns(Collections.singletonList(
                        MigrationSchema.ColumnDef.builder()
                                .name("id")
                                .dataType(DataType.Int64)
                                .build()))
                .indexes(Collections.singletonList(
                        MigrationSchema.IndexDef.builder()
                                .fieldName("embedding")
                                .indexName("idx_embedding")
                                .indexType(IndexParam.IndexType.HNSW)
                                .metricType(IndexParam.MetricType.COSINE)
                                .extraParams(params)
                                .build()))
                .build();

        List<String> ddls = PgVectorSchemaGenerator.generateCreateIndexDdls(
                schema, PG_SCHEMA, PG_TABLE);

        assertEquals(1, ddls.size());
        assertTrue(ddls.get(0).contains("USING hnsw"));
        assertTrue(ddls.get(0).contains("vector_cosine_ops"));
    }

    @Test
    public void testGenerateCreateIndexDdlsWithMultipleIndexes() {
        MigrationSchema schema = MigrationSchema.builder()
                .collectionName("my_collection")
                .primaryKeyName("id")
                .columns(Collections.singletonList(
                        MigrationSchema.ColumnDef.builder()
                                .name("id")
                                .dataType(DataType.Int64)
                                .build()))
                .indexes(Arrays.asList(
                        MigrationSchema.IndexDef.builder()
                                .fieldName("vec1")
                                .indexName("idx_vec1")
                                .indexType(IndexParam.IndexType.HNSW)
                                .metricType(IndexParam.MetricType.COSINE)
                                .extraParams(null)
                                .build(),
                        MigrationSchema.IndexDef.builder()
                                .fieldName("vec2")
                                .indexName("idx_vec2")
                                .indexType(IndexParam.IndexType.IVF_FLAT)
                                .metricType(IndexParam.MetricType.L2)
                                .extraParams(null)
                                .build()))
                .build();

        List<String> ddls = PgVectorSchemaGenerator.generateCreateIndexDdls(
                schema, PG_SCHEMA, PG_TABLE);

        assertEquals(2, ddls.size());
    }

    @Test
    public void testGenerateCreateIndexDdlsEmptyWhenNoIndexes() {
        MigrationSchema schema = MigrationSchema.builder()
                .collectionName("my_collection")
                .primaryKeyName("id")
                .columns(Collections.singletonList(
                        MigrationSchema.ColumnDef.builder()
                                .name("id")
                                .dataType(DataType.Int64)
                                .build()))
                .indexes(null)
                .build();

        List<String> ddls = PgVectorSchemaGenerator.generateCreateIndexDdls(
                schema, PG_SCHEMA, PG_TABLE);

        assertTrue(ddls.isEmpty());
    }

    @Test
    public void testGenerateCreateIndexDdlsSkipsNullDdls() {
        // FLAT returns null from MilvusIndexConverter → should be skipped
        MigrationSchema schema = MigrationSchema.builder()
                .collectionName("my_collection")
                .primaryKeyName("id")
                .columns(Collections.singletonList(
                        MigrationSchema.ColumnDef.builder()
                                .name("id")
                                .dataType(DataType.Int64)
                                .build()))
                .indexes(Arrays.asList(
                        MigrationSchema.IndexDef.builder()
                                .fieldName("vec")
                                .indexName("idx_flat")
                                .indexType(IndexParam.IndexType.FLAT)
                                .metricType(IndexParam.MetricType.L2)
                                .extraParams(null)
                                .build(),
                        MigrationSchema.IndexDef.builder()
                                .fieldName("vec")
                                .indexName("idx_hnsw")
                                .indexType(IndexParam.IndexType.HNSW)
                                .metricType(IndexParam.MetricType.COSINE)
                                .extraParams(null)
                                .build()))
                .build();

        List<String> ddls = PgVectorSchemaGenerator.generateCreateIndexDdls(
                schema, PG_SCHEMA, PG_TABLE);

        // FLAT is skipped, only HNSW produces a DDL
        assertEquals(1, ddls.size());
        assertTrue(ddls.get(0).contains("USING hnsw"));
    }

    @Test
    public void testCreateTableDdlWithNullSchema() {
        MigrationSchema schema = MigrationSchema.builder()
                .collectionName("my_collection")
                .primaryKeyName("id")
                .columns(Collections.singletonList(
                        MigrationSchema.ColumnDef.builder()
                                .name("id")
                                .dataType(DataType.Int64)
                                .isPrimaryKey(true)
                                .build()))
                .build();

        String ddl = PgVectorSchemaGenerator.generateCreateTableDdl(
                schema, null, PG_TABLE, false, true);

        assertTrue(ddl.contains("\"my_collection\""));
        assertFalse(ddl.contains(".\"my_collection\""));
    }

    @Test
    public void testCreateTableDdlWithEmptySchema() {
        MigrationSchema schema = MigrationSchema.builder()
                .collectionName("my_collection")
                .primaryKeyName("id")
                .columns(Collections.singletonList(
                        MigrationSchema.ColumnDef.builder()
                                .name("id")
                                .dataType(DataType.Int64)
                                .isPrimaryKey(true)
                                .build()))
                .build();

        String ddl = PgVectorSchemaGenerator.generateCreateTableDdl(
                schema, "", PG_TABLE, false, true);

        assertTrue(ddl.contains("\"my_collection\""));
        assertFalse(ddl.contains(".\"my_collection\""));
    }
}
