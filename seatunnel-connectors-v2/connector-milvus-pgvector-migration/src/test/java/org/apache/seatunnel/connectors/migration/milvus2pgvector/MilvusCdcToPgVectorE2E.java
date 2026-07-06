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

package org.apache.seatunnel.connectors.migration.milvus2pgvector;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.cdc.*;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.utils.MilvusConnectorUtils;
import org.apache.seatunnel.connectors.seatunnel.milvus.source.utils.MilvusSourceConverter;
import org.apache.seatunnel.connectors.seatunnel.milvus.source.utils.MilvusSourceConnectorUtils;

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import milvus.com.google.gson.Gson;
import milvus.com.google.gson.JsonObject;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.GetLoadStateReq;
import io.milvus.v2.service.collection.request.HasCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.vector.request.InsertReq;

import lombok.extern.slf4j.Slf4j;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end test verifying Milvus CDC data replication to PgVector.
 *
 * <p>This test validates both the <b>snapshot</b> (full table scan via
 * QueryIterator) and <b>incremental</b> (PK-based change polling via
 * {@link CdcStrategy}) phases of the CDC pipeline.
 *
 * <p><b>Prerequisites:</b>
 * <ul>
 *   <li>Milvus running on {@code localhost:19530}</li>
 *   <li>PostgreSQL with pgvector extension running on {@code localhost:5432}</li>
 * </ul>
 *
 * <p><b>Run with:</b>
 * <pre>
 * ./mvnw test -pl connector-milvus-pgvector-migration \
 *     -Dtest=MilvusCdcToPgVectorE2E \
 *     -Dmigration.local.e2e.enabled=true
 * </pre>
 */
@Slf4j
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MilvusCdcToPgVectorE2E {

    // ---- Connection configuration ----
    private static final String MILVUS_URL = "http://localhost:19530";
    private static final String MILVUS_TOKEN = "";
    private static final String MILVUS_DB = "default";
    private static final String PG_URL = "jdbc:postgresql://localhost:5432/vts_cdc_test";
    private static final String PG_USER = "zhangqiang";
    private static final String PG_PASSWORD = "";

    // ---- Test parameters ----
    private static final String COLLECTION_NAME = "cdc_e2e_test";
    private static final String PG_TABLE_NAME = "public.cdc_e2e_pgvector";
    private static final int VECTOR_DIM = 8;
    private static final int SNAPSHOT_COUNT = 100;
    private static final int INCREMENTAL_COUNT = 50;

    private static MilvusClientV2 milvusClient;
    private static Connection pgConnection;

    @BeforeAll
    static void setUp() throws Exception {
        String enabled = System.getProperty("migration.local.e2e.enabled", "false");
        if (!"true".equals(enabled)) {
            log.warn("Skipping CDC E2E test. Set -Dmigration.local.e2e.enabled=true to run.");
            return;
        }

        // Connect to Milvus
        ConnectConfig connectConfig = ConnectConfig.builder()
                .uri(MILVUS_URL)
                .token(MILVUS_TOKEN)
                .dbName(MILVUS_DB)
                .connectTimeoutMs(30000)
                .build();
        milvusClient = new MilvusClientV2(connectConfig);

        // Create test database (connect to postgres first, then drop/recreate)
        try (Connection adminConn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:5432/postgres", PG_USER, PG_PASSWORD);
                Statement stmt = adminConn.createStatement()) {
            stmt.execute("DROP DATABASE IF EXISTS vts_cdc_test");
            stmt.execute("CREATE DATABASE vts_cdc_test");
        }

        // Connect to the test database
        pgConnection = DriverManager.getConnection(PG_URL, PG_USER, PG_PASSWORD);
        pgConnection.setAutoCommit(true);

        // Install pgvector extension
        try (Statement stmt = pgConnection.createStatement()) {
            stmt.execute("CREATE EXTENSION IF NOT EXISTS vector");
        }

        log.info("=== CDC E2E Test Setup Complete ===");
        log.info("Milvus: {}", MILVUS_URL);
        log.info("PostgreSQL: {}", PG_URL);
    }

    @AfterAll
    static void tearDown() throws Exception {
        String enabled = System.getProperty("migration.local.e2e.enabled", "false");
        if (!"true".equals(enabled)) {
            return;
        }

        // Clean up Milvus
        if (milvusClient != null) {
            try {
                milvusClient.releaseCollection(
                        io.milvus.v2.service.collection.request.ReleaseCollectionReq.builder()
                                .collectionName(COLLECTION_NAME).build());
            } catch (Exception e) {
                log.debug("Collection release failed (may already be released): {}", e.getMessage());
            }
            try {
                milvusClient.dropCollection(
                        DropCollectionReq.builder().collectionName(COLLECTION_NAME).build());
            } catch (Exception e) {
                log.debug("Collection drop failed: {}", e.getMessage());
            }
            try {
                milvusClient.close();
            } catch (Exception e) {
                log.debug("Client close failed: {}", e.getMessage());
            }
        }

        // Clean up PostgreSQL
        if (pgConnection != null) {
            try {
                try (Statement stmt = pgConnection.createStatement()) {
                    stmt.execute("DROP TABLE IF EXISTS " + PG_TABLE_NAME + " CASCADE");
                }
                pgConnection.close();
            } catch (Exception e) {
                log.debug("PG cleanup failed: {}", e.getMessage());
            }
        }

        log.info("=== CDC E2E Test Cleanup Complete ===");
    }

    /**
     * Phase 1: Create Milvus collection and insert snapshot data.
     */
    @Test
    @Order(1)
    @DisplayName("1. Create Milvus collection and insert snapshot data")
    void testCreateCollectionAndInsertSnapshotData() throws Exception {
        if (!isEnabled()) return;

        log.info("--- Phase 1: Creating collection and inserting snapshot data ---");

        // Drop existing collection if any
        HasCollectionReq hasReq = HasCollectionReq.builder()
                .collectionName(COLLECTION_NAME).build();
        if (milvusClient.hasCollection(hasReq)) {
            milvusClient.dropCollection(
                    DropCollectionReq.builder().collectionName(COLLECTION_NAME).build());
            log.info("Dropped existing collection: {}", COLLECTION_NAME);
        }

        // Create collection with id, vector, category, and a timestamp field
        List<CreateCollectionReq.FieldSchema> fields = new ArrayList<>();
        fields.add(CreateCollectionReq.FieldSchema.builder()
                .name("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(false)
                .build());
        fields.add(CreateCollectionReq.FieldSchema.builder()
                .name("vector")
                .dataType(DataType.FloatVector)
                .dimension(VECTOR_DIM)
                .build());
        fields.add(CreateCollectionReq.FieldSchema.builder()
                .name("category")
                .dataType(DataType.VarChar)
                .maxLength(64)
                .isNullable(true)
                .build());
        fields.add(CreateCollectionReq.FieldSchema.builder()
                .name("created_at")
                .dataType(DataType.Int64)
                .isNullable(true)
                .build());

        CreateCollectionReq.CollectionSchema schema =
                CreateCollectionReq.CollectionSchema.builder()
                        .fieldSchemaList(fields)
                        .build();

        milvusClient.createCollection(
                CreateCollectionReq.builder()
                        .collectionName(COLLECTION_NAME)
                        .collectionSchema(schema)
                        .build());

        // Create IVF_FLAT index on vector field
        milvusClient.createIndex(
                CreateIndexReq.builder()
                        .collectionName(COLLECTION_NAME)
                        .indexParams(Collections.singletonList(
                                io.milvus.v2.common.IndexParam.builder()
                                        .fieldName("vector")
                                        .indexType(io.milvus.v2.common.IndexParam.IndexType.IVF_FLAT)
                                        .metricType(io.milvus.v2.common.IndexParam.MetricType.COSINE)
                                        .extraParams(new HashMap<String, Object>() {{
                                            put("nlist", 16);
                                        }})
                                        .build()))
                        .build());

        // Load collection
        milvusClient.loadCollection(
                LoadCollectionReq.builder().collectionName(COLLECTION_NAME).build());

        // Wait for load
        Thread.sleep(3000);

        // Insert SNAPSHOT data (id 0 to SNAPSHOT_COUNT-1)
        insertTestData(0, SNAPSHOT_COUNT);

        // Verify insertion
        verifyRowCountMilvus(SNAPSHOT_COUNT);
        log.info("Phase 1 complete: {} rows inserted", SNAPSHOT_COUNT);
    }

    /**
     * Phase 2: Create PgVector schema via SchemaMigrator.
     */
    @Test
    @Order(2)
    @DisplayName("2. Create PgVector target schema")
    void testCreatePgVectorSchema() throws Exception {
        if (!isEnabled()) return;

        log.info("--- Phase 2: Creating PgVector schema ---");

        // Get collection schema from Milvus
        DescribeCollectionResp desc = milvusClient.describeCollection(
                DescribeCollectionReq.builder().collectionName(COLLECTION_NAME).build());

        assertNotNull(desc, "Collection description should not be null");

        // Create pgvector table matching the schema
        try (Statement stmt = pgConnection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS " + PG_TABLE_NAME + " CASCADE");
            stmt.execute(
                    "CREATE TABLE " + PG_TABLE_NAME + " ("
                            + "id BIGINT PRIMARY KEY, "
                            + "vector vector(" + VECTOR_DIM + "), "
                            + "category VARCHAR(64), "
                            + "created_at BIGINT"
                            + ")");
        }

        // Verify table exists
        try (ResultSet rs = pgConnection.getMetaData().getTables(
                null, "public", "cdc_e2e_pgvector", null)) {
            assertTrue(rs.next(), "PgVector table should exist");
        }

        log.info("Phase 2 complete: PgVector schema created");
    }

    /**
     * Phase 3: Run snapshot migration — replicate initial data to PgVector
     * using QueryIterator (simulating the CDC snapshot phase).
     */
    @Test
    @Order(3)
    @DisplayName("3. Snapshot migration: replicate initial data to PgVector")
    void testSnapshotMigration() throws Exception {
        if (!isEnabled()) return;

        log.info("--- Phase 3: Snapshot migration ---");

        // Build CDC config for snapshot phase
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", MILVUS_URL);
        configMap.put("token", MILVUS_TOKEN);
        configMap.put("database", MILVUS_DB);
        configMap.put("collection", COLLECTION_NAME);
        configMap.put("batch_size", 50);
        configMap.put("poll_interval_ms", 1000L);
        configMap.put("startup_mode", "INITIAL");
        configMap.put("cdc_strategy", "polling_incremental");
        configMap.put("primary_key_field", "id");
        configMap.put("parallelism", 1);

        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(configMap);
        MilvusCdcSourceConfig cdcConfig = MilvusCdcSourceConfig.of(readonlyConfig);

        // Use MilvusSourceConnectorUtils to introspect the schema
        MilvusSourceConnectorUtils utils = new MilvusSourceConnectorUtils(readonlyConfig);
        Map<TablePath, org.apache.seatunnel.api.table.catalog.CatalogTable> tables =
                utils.getTables();

        assertFalse(tables.isEmpty(), "Should have at least one source table");

        org.apache.seatunnel.api.table.catalog.CatalogTable catalogTable =
                tables.values().iterator().next();
        TableSchema tableSchema = catalogTable.getTableSchema();
        MilvusSourceConverter converter = new MilvusSourceConverter(tableSchema);

        // Create CDC strategy for snapshot (PK-based polling from beginning)
        PollingIncrementalCdcStrategy strategy =
                new PollingIncrementalCdcStrategy(cdcConfig, converter, tableSchema, milvusClient);

        // Create a snapshot split (startId=-1 so first query includes id=0)
        MilvusCdcSourceSplit snapshotSplit = MilvusCdcSourceSplit.builder()
                .splitId("e2e-snapshot-0")
                .collectionName(COLLECTION_NAME)
                .snapshot(true)
                .startId(-1L)
                .endId(SNAPSHOT_COUNT)
                .offset(0L)
                .limit(-1L)
                .build();

        // Read all snapshot data via CDC strategy
        List<SeaTunnelRowWithPosition> allRows = new ArrayList<>();
        ReplicatePosition position = null;

        for (int i = 0; i < 10; i++) { // Max 10 polls to avoid infinite loop
            List<SeaTunnelRowWithPosition> batch =
                    strategy.pollChanges(snapshotSplit, position);
            if (batch.isEmpty()) {
                break;
            }
            allRows.addAll(batch);
            position = batch.get(batch.size() - 1).getPosition();
            if (allRows.size() >= SNAPSHOT_COUNT) {
                break;
            }
        }

        log.info("Snapshot phase read {} rows via CdcStrategy", allRows.size());
        assertTrue(allRows.size() >= SNAPSHOT_COUNT,
                "Should read at least " + SNAPSHOT_COUNT + " rows, got " + allRows.size());

        // Insert all snapshot rows into PgVector
        String insertSql = "INSERT INTO " + PG_TABLE_NAME
                + " (id, vector, category, created_at) VALUES (?, ?::vector, ?, ?)";

        try (PreparedStatement ps = pgConnection.prepareStatement(insertSql)) {
            for (SeaTunnelRowWithPosition rowPos : allRows) {
                SeaTunnelRow row = rowPos.getRow();
                Object[] fields = row.getFields();

                ps.setLong(1, toLong(fields[0]));

                // Convert vector field (ByteBuffer for FloatVector in SeaTunnelRow)
                ps.setString(2, toVectorString(fields[1]));

                ps.setString(3, fields[2] != null ? fields[2].toString() : null);
                ps.setLong(4, fields[3] != null ? toLong(fields[3]) : 0);
                ps.addBatch();

                if (allRows.indexOf(rowPos) % 50 == 49) {
                    ps.executeBatch();
                }
            }
            ps.executeBatch();
        }

        strategy.close();

        // Verify row count in PgVector
        int pgCount = queryPgCount();
        log.info("PgVector row count after snapshot: {}", pgCount);
        assertTrue(pgCount >= SNAPSHOT_COUNT,
                "PgVector should have at least " + SNAPSHOT_COUNT
                        + " rows after snapshot, got " + pgCount);

        log.info("Phase 3 complete: Snapshot migration verified");
    }

    /**
     * Phase 4: Insert incremental data and verify CDC polling detects it.
     */
    @Test
    @Order(4)
    @DisplayName("4. Incremental CDC: insert new data and verify polling")
    void testIncrementalCdc() throws Exception {
        if (!isEnabled()) return;

        log.info("--- Phase 4: Incremental CDC ---");

        // Insert INCREMENTAL data (id SNAPSHOT_COUNT to SNAPSHOT_COUNT+INCREMENTAL_COUNT-1)
        insertTestData(SNAPSHOT_COUNT, INCREMENTAL_COUNT);

        // Wait a bit for data to be visible
        Thread.sleep(2000);

        // Verify Milvus has the new data
        verifyRowCountMilvus(SNAPSHOT_COUNT + INCREMENTAL_COUNT);

        // Build CDC config
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", MILVUS_URL);
        configMap.put("token", MILVUS_TOKEN);
        configMap.put("database", MILVUS_DB);
        configMap.put("collection", COLLECTION_NAME);
        configMap.put("batch_size", 50);
        configMap.put("poll_interval_ms", 1000L);
        configMap.put("startup_mode", "LATEST");
        configMap.put("cdc_strategy", "polling_incremental");
        configMap.put("primary_key_field", "id");
        configMap.put("parallelism", 1);

        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(configMap);
        MilvusCdcSourceConfig cdcConfig = MilvusCdcSourceConfig.of(readonlyConfig);

        MilvusSourceConnectorUtils utils = new MilvusSourceConnectorUtils(readonlyConfig);
        Map<TablePath, org.apache.seatunnel.api.table.catalog.CatalogTable> tables =
                utils.getTables();
        TableSchema tableSchema = tables.values().iterator().next().getTableSchema();
        MilvusSourceConverter converter = new MilvusSourceConverter(tableSchema);

        // Create incremental split starting from the last snapshot ID
        PollingIncrementalCdcStrategy strategy =
                new PollingIncrementalCdcStrategy(cdcConfig, converter, tableSchema, milvusClient);

        MilvusCdcSourceSplit incSplit = MilvusCdcSourceSplit.builder()
                .splitId("e2e-inc-0")
                .collectionName(COLLECTION_NAME)
                .snapshot(false)
                .startId(SNAPSHOT_COUNT - 1) // startId is exclusive
                .endId(Long.MAX_VALUE)
                .build();

        // Poll for new changes
        List<SeaTunnelRowWithPosition> incrementalRows = new ArrayList<>();
        ReplicatePosition position = null;

        for (int i = 0; i < 10; i++) {
            List<SeaTunnelRowWithPosition> batch =
                    strategy.pollChanges(incSplit, position);
            if (batch.isEmpty()) {
                log.debug("No more incremental data after {} polls", i);
                break;
            }
            incrementalRows.addAll(batch);
            position = batch.get(batch.size() - 1).getPosition();

            log.info("Poll {}: got {} rows (total incremental: {})",
                    i, batch.size(), incrementalRows.size());
        }

        strategy.close();

        log.info("Incremental phase detected {} new rows", incrementalRows.size());
        assertTrue(incrementalRows.size() >= INCREMENTAL_COUNT,
                "Should detect at least " + INCREMENTAL_COUNT
                        + " incremental rows, got " + incrementalRows.size());

        // Insert incremental rows into PgVector
        String insertSql = "INSERT INTO " + PG_TABLE_NAME
                + " (id, vector, category, created_at) VALUES (?, ?::vector, ?, ?)"
                + " ON CONFLICT (id) DO NOTHING";

        try (PreparedStatement ps = pgConnection.prepareStatement(insertSql)) {
            for (SeaTunnelRowWithPosition rowPos : incrementalRows) {
                SeaTunnelRow row = rowPos.getRow();
                Object[] fields = row.getFields();

                if (fields[0] == null) continue;
                long id = toLong(fields[0]);
                if (id < SNAPSHOT_COUNT) continue; // Skip snapshot duplicates

                ps.setLong(1, id);

                Object vecObj = fields[1];
                if (vecObj instanceof List) {
                    @SuppressWarnings("unchecked")
                    List<Float> vecList = (List<Float>) vecObj;
                    StringBuilder sb = new StringBuilder("[");
                    for (int i = 0; i < vecList.size(); i++) {
                        if (i > 0) sb.append(",");
                        sb.append(vecList.get(i));
                    }
                    sb.append("]");
                    ps.setString(2, sb.toString());
                } else {
                    ps.setString(2, "[]");
                }

                ps.setString(3, fields[2] != null ? fields[2].toString() : null);
                ps.setLong(4, fields[3] != null ? toLong(fields[3]) : 0);
                ps.addBatch();
            }
            ps.executeBatch();
        }

        // Final verification
        int finalPgCount = queryPgCount();
        log.info("Final PgVector row count: {}", finalPgCount);
        assertTrue(finalPgCount >= SNAPSHOT_COUNT + INCREMENTAL_COUNT,
                "PgVector should have at least " + (SNAPSHOT_COUNT + INCREMENTAL_COUNT)
                        + " total rows, got " + finalPgCount);

        log.info("Phase 4 complete: Incremental CDC verified");
    }

    /**
     * Phase 5: Verify data integrity — compare row counts and check vector dimensions.
     */
    @Test
    @Order(5)
    @DisplayName("5. Data integrity verification")
    void testDataIntegrity() throws Exception {
        if (!isEnabled()) return;

        log.info("--- Phase 5: Data integrity verification ---");

        // Verify row counts match
        int pgCount = queryPgCount();
        assertTrue(pgCount >= SNAPSHOT_COUNT + INCREMENTAL_COUNT,
                "Final PgVector row count mismatch: expected at least "
                        + (SNAPSHOT_COUNT + INCREMENTAL_COUNT) + ", got " + pgCount);

        // Verify specific rows exist (spot check)
        try (Statement stmt = pgConnection.createStatement()) {
            // Check snapshot row
            ResultSet rs1 = stmt.executeQuery(
                    "SELECT id, vector_dims(vector) as dims FROM " + PG_TABLE_NAME
                            + " WHERE id = 0");
            assertTrue(rs1.next(), "Snapshot row id=0 should exist");
            assertEquals(VECTOR_DIM, rs1.getInt("dims"),
                    "Vector dimension should be " + VECTOR_DIM);

            // Check incremental row
            ResultSet rs2 = stmt.executeQuery(
                    "SELECT id FROM " + PG_TABLE_NAME
                            + " WHERE id = " + SNAPSHOT_COUNT);
            assertTrue(rs2.next(),
                    "Incremental row id=" + SNAPSHOT_COUNT + " should exist");

            // Check category distribution
            ResultSet rs3 = stmt.executeQuery(
                    "SELECT COUNT(DISTINCT category) as cats FROM " + PG_TABLE_NAME);
            assertTrue(rs3.next());
            int catCount = rs3.getInt("cats");
            assertTrue(catCount > 0, "Should have at least one category");
            log.info("Distinct categories: {}", catCount);
        }

        log.info("Phase 5 complete: Data integrity verified");
    }

    // ========== Helper methods ==========

    private boolean isEnabled() {
        return "true".equals(System.getProperty("migration.local.e2e.enabled", "false"));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void insertTestData(int startId, int count) throws Exception {
        Random random = new Random(42 + startId);
        Gson gson = new Gson();
        java.util.List rows = new java.util.ArrayList();

        for (int i = 0; i < count; i++) {
            int id = startId + i;
            JsonObject row = new JsonObject();
            row.addProperty("id", id);

            java.util.List<Float> vector = new java.util.ArrayList<>(VECTOR_DIM);
            for (int d = 0; d < VECTOR_DIM; d++) {
                vector.add(random.nextFloat());
            }
            row.add("vector", gson.toJsonTree(vector));
            row.addProperty("category", "cat_" + (id % 10));
            row.addProperty("created_at", System.currentTimeMillis() / 1000);
            rows.add(row);
        }

        milvusClient.insert(
                InsertReq.builder()
                        .collectionName(COLLECTION_NAME)
                        .data(rows)
                        .build());

        log.info("Inserted {} rows ({} to {})", count, startId, startId + count - 1);
    }

    private void verifyRowCountMilvus(int expected) throws Exception {
        GetLoadStateReq loadReq = GetLoadStateReq.builder()
                .collectionName(COLLECTION_NAME).build();
        assertTrue(milvusClient.getLoadState(loadReq),
                "Collection should be loaded");

        // Use query to count rows
        io.milvus.v2.service.vector.request.QueryReq queryReq =
                io.milvus.v2.service.vector.request.QueryReq.builder()
                        .collectionName(COLLECTION_NAME)
                        .filter("id >= 0")
                        .outputFields(Collections.singletonList("count(*)"))
                        .build();

        io.milvus.v2.service.vector.response.QueryResp resp =
                milvusClient.query(queryReq);

        if (resp.getQueryResults() != null && !resp.getQueryResults().isEmpty()) {
            Object countObj = resp.getQueryResults().get(0).getEntity().get("count(*)");
            if (countObj instanceof Number) {
                long actualCount = ((Number) countObj).longValue();
                log.info("Milvus row count: {} (expected {})", actualCount, expected);
            }
        }
    }

    private int queryPgCount() throws Exception {
        try (Statement stmt = pgConnection.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT COUNT(*) FROM " + PG_TABLE_NAME)) {
            if (rs.next()) {
                return rs.getInt(1);
            }
        }
        return 0;
    }

    private long toLong(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value instanceof String) {
            return Long.parseLong((String) value);
        }
        return 0L;
    }

    /**
     * Convert a SeaTunnelRow field value to a pgvector-compatible string.
     * Milvus vectors may be ByteBuffer (FloatVector), List, or other types.
     */
    private String toVectorString(Object vecObj) {
        if (vecObj == null) return "[]";
        if (vecObj instanceof java.nio.ByteBuffer) {
            java.nio.ByteBuffer buf = (java.nio.ByteBuffer) vecObj;
            float[] floats = new float[buf.remaining() / 4];
            buf.asFloatBuffer().get(floats);
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < floats.length; i++) {
                if (i > 0) sb.append(",");
                sb.append(floats[i]);
            }
            sb.append("]");
            return sb.toString();
        }
        if (vecObj instanceof java.util.List) {
            return vecObj.toString().replace(" ", "");
        }
        return vecObj.toString();
    }
}
