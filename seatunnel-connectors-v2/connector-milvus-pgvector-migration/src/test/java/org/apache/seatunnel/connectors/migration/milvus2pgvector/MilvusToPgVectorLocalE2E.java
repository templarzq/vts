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

import org.apache.seatunnel.connectors.migration.milvus2pgvector.config.MigrationConfig;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.schema.MigrationSchema;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.schema.MilvusSchemaIntrospector;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.schema.SchemaMigrator;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.validation.DataValidator;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.validation.ValidationReport;

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.vector.request.InsertReq;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;

import milvus.com.google.gson.Gson;
import milvus.com.google.gson.JsonObject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Local E2E test that connects to already-running Milvus and PostgreSQL services (no Testcontainers
 * needed).
 *
 * <p>Prerequisites:
 *
 * <ul>
 *   <li>Milvus running at {@code http://localhost:19530}
 *   <li>PostgreSQL running at {@code localhost:5432} with user {@code zhangqiang} (trust auth)
 *   <li>pgvector extension installed in PostgreSQL
 * </ul>
 *
 * <p>Run with: {@code ./mvnw test -pl connector-milvus-pgvector-migration
 * -Dtest=MilvusToPgVectorLocalE2E -Dmigration.local.e2e.enabled=true}
 */
@Slf4j
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MilvusToPgVectorLocalE2E {

    // ---- Local service config ----
    private static final String MILVUS_URL = "http://localhost:19530";
    private static final String PG_HOST = "localhost";
    private static final int PG_PORT = 5432;
    private static final String PG_USER = "zhangqiang";
    private static final String PG_PASSWORD = "";
    private static final String PG_DATABASE = "vts_migration_test";
    private static final String PG_SCHEMA = "public";

    // ---- Test data config ----
    private static final String COLLECTION = "local_e2e_collection";
    private static final String TABLE = "local_e2e_table";
    private static final int VECTOR_DIM = 128;
    private static final int ROW_COUNT = 500;

    private static MilvusClientV2 milvusClient;
    private static Connection pgConnection;
    private static final Gson gson = new Gson();

    @BeforeAll
    static void setUp() throws Exception {
        // Connect to Milvus
        milvusClient = new MilvusClientV2(ConnectConfig.builder().uri(MILVUS_URL).build());
        log.info("Connected to Milvus at {}", MILVUS_URL);

        // Connect to PostgreSQL server (not the test DB yet)
        Class.forName("org.postgresql.Driver");
        String serverUrl = "jdbc:postgresql://" + PG_HOST + ":" + PG_PORT + "/postgres";
        Connection serverConn = DriverManager.getConnection(serverUrl, PG_USER, PG_PASSWORD);

        // Create test database (drop if exists for clean state)
        try (Statement stmt = serverConn.createStatement()) {
            stmt.execute("DROP DATABASE IF EXISTS " + PG_DATABASE);
            stmt.execute("CREATE DATABASE " + PG_DATABASE);
            log.info("Created test database: {}", PG_DATABASE);
        }
        serverConn.close();

        // Connect to the test database
        String dbUrl = "jdbc:postgresql://" + PG_HOST + ":" + PG_PORT + "/" + PG_DATABASE;
        pgConnection = DriverManager.getConnection(dbUrl, PG_USER, PG_PASSWORD);
        log.info("Connected to PostgreSQL at {}", dbUrl);

        // Install pgvector extension
        try (Statement stmt = pgConnection.createStatement()) {
            stmt.execute("CREATE EXTENSION IF NOT EXISTS vector");
        }
        log.info("pgvector extension installed");

        // Clean up any existing Milvus collection
        try {
            milvusClient.dropCollection(
                    io.milvus.v2.service.collection.request.DropCollectionReq.builder()
                            .collectionName(COLLECTION)
                            .build());
        } catch (Exception ignored) {
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (milvusClient != null) {
            try {
                milvusClient.dropCollection(
                        io.milvus.v2.service.collection.request.DropCollectionReq.builder()
                                .collectionName(COLLECTION)
                                .build());
            } catch (Exception ignored) {
            }
            milvusClient.close();
        }
        if (pgConnection != null) {
            pgConnection.close();
        }
        // Drop test database
        String serverUrl = "jdbc:postgresql://" + PG_HOST + ":" + PG_PORT + "/postgres";
        Connection serverConn = DriverManager.getConnection(serverUrl, PG_USER, PG_PASSWORD);
        try (Statement stmt = serverConn.createStatement()) {
            stmt.execute("DROP DATABASE IF EXISTS " + PG_DATABASE);
            log.info("Dropped test database: {}", PG_DATABASE);
        }
        serverConn.close();
    }

    @Test
    @Order(1)
    void testCreateCollectionAndInsertData() throws Exception {
        // 1. Create Milvus collection
        List<CreateCollectionReq.FieldSchema> fields = new ArrayList<>();
        fields.add(
                CreateCollectionReq.FieldSchema.builder()
                        .name("id")
                        .dataType(DataType.Int64)
                        .isPrimaryKey(true)
                        .autoID(false)
                        .build());
        fields.add(
                CreateCollectionReq.FieldSchema.builder()
                        .name("vector")
                        .dataType(DataType.FloatVector)
                        .dimension(VECTOR_DIM)
                        .build());
        fields.add(
                CreateCollectionReq.FieldSchema.builder()
                        .name("category")
                        .dataType(DataType.VarChar)
                        .maxLength(64)
                        .isNullable(true)
                        .build());

        CreateCollectionReq.CollectionSchema schema =
                CreateCollectionReq.CollectionSchema.builder()
                        .fieldSchemaList(fields)
                        .build();

        milvusClient.createCollection(
                CreateCollectionReq.builder()
                        .collectionName(COLLECTION)
                        .collectionSchema(schema)
                        .build());
        log.info("Created Milvus collection '{}' with dim={}", COLLECTION, VECTOR_DIM);

        // 2. Create IVF_FLAT index
        milvusClient.createIndex(
                CreateIndexReq.builder()
                        .collectionName(COLLECTION)
                        .indexParams(
                                Collections.singletonList(
                                        IndexParam.builder()
                                                .fieldName("vector")
                                                .indexType(IndexParam.IndexType.IVF_FLAT)
                                                .metricType(IndexParam.MetricType.COSINE)
                                                .extraParams(
                                                        new java.util.HashMap<String, Object>() {
                                                            {
                                                                put("nlist", 128);
                                                            }
                                                        })
                                                .build()))
                        .build());
        log.info("Created IVF_FLAT index on vector field");

        // 3. Insert test data
        Random random = new Random(42);
        List<JsonObject> rows = new ArrayList<>();
        for (int i = 0; i < ROW_COUNT; i++) {
            JsonObject row = new JsonObject();
            row.addProperty("id", i);
            List<Float> vector = new ArrayList<>(VECTOR_DIM);
            for (int d = 0; d < VECTOR_DIM; d++) {
                vector.add(random.nextFloat());
            }
            row.add("vector", gson.toJsonTree(vector));
            row.addProperty("category", "cat_" + (i % 10));
            rows.add(row);
        }
        milvusClient.insert(
                InsertReq.builder().collectionName(COLLECTION).data(rows).build());
        log.info("Inserted {} rows into '{}'", ROW_COUNT, COLLECTION);

        // 4. Load collection
        milvusClient.loadCollection(
                LoadCollectionReq.builder().collectionName(COLLECTION).build());
        log.info("Loaded collection '{}'", COLLECTION);
    }

    @Test
    @Order(2)
    void testSchemaMigration() throws Exception {
        MigrationConfig config = buildConfig();
        MigrationSchema schema;
        try (SchemaMigrator migrator = new SchemaMigrator(config)) {
            schema = migrator.migrate();
        }
        assertNotNull(schema, "Schema should be introspected");
        assertEquals(COLLECTION, schema.getCollectionName());
        assertTrue(schema.getColumns().size() >= 3, "Should have at least 3 columns");
        log.info(
                "Schema migration completed: {} columns, {} indexes",
                schema.getColumns().size(),
                schema.getIndexes() != null ? schema.getIndexes().size() : 0);

        // Verify the pgvector table was created
        try (Statement stmt = pgConnection.createStatement();
                java.sql.ResultSet rs =
                        stmt.executeQuery(
                                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '"
                                        + TABLE
                                        + "'")) {
            assertTrue(rs.next() && rs.getInt(1) > 0, "pgvector table should exist");
        }
        log.info("pgvector table '{}' created successfully", TABLE);
    }

    @Test
    @Order(3)
    void testDataInsertionAndValidation() throws Exception {
        MigrationConfig config = buildConfig();

        // Insert data into pgvector via JDBC (simulating SeaTunnel data migration)
        insertDataIntoPgVector(TABLE, ROW_COUNT, VECTOR_DIM);

        // Verify row count
        long pgCount = getPgRowCount(TABLE);
        assertEquals(ROW_COUNT, pgCount, "PG row count should match inserted count");
        log.info("pgvector row count: {}", pgCount);

        // Run validation
        // Need to introspect schema again for the validator
        MigrationSchema schema;
        try (MilvusSchemaIntrospector introspector = new MilvusSchemaIntrospector(MILVUS_URL, "")) {
            schema = introspector.introspect(COLLECTION);
        }

        try (DataValidator validator = new DataValidator(config, schema)) {
            ValidationReport report = validator.validate();
            assertNotNull(report, "Validation report should not be null");
            log.info(
                    "Validation result: {}",
                    report.isOverallPassed() ? "PASSED" : "FAILED");
            log.info("Validation details:\n{}", report.formatConsole());
            assertTrue(report.isOverallPassed(), "Validation should pass");
        }
    }

    private MigrationConfig buildConfig() {
        String jdbcUrl = "jdbc:postgresql://" + PG_HOST + ":" + PG_PORT + "/" + PG_DATABASE;
        return MigrationConfig.builder()
                .milvusUrl(MILVUS_URL)
                .milvusToken("")
                .milvusDatabase("default")
                .milvusCollection(COLLECTION)
                .pgUrl(jdbcUrl)
                .pgUser(PG_USER)
                .pgPassword(PG_PASSWORD)
                .pgSchema(PG_SCHEMA)
                .pgTable(TABLE)
                .batchSize(100)
                .parallelism(1)
                .rateLimitRowsPerSecond(0)
                .skipIndexMigration(false)
                .dropExistingTable(true)
                .allowPrecisionLoss(true)
                .validationSampleSize(50)
                .similarityThreshold(0.999)
                .auditLogDir(java.nio.file.Paths.get("target/migration-logs").toString())
                .build();
    }

    private void insertDataIntoPgVector(String tableName, int count, int dim) throws Exception {
        Random random = new Random(42); // Same seed as testCreateCollectionAndInsertData
        String sql =
                "INSERT INTO \"" + tableName + "\" (id, vector, category) VALUES (?, ?::vector, ?)";

        try (PreparedStatement ps = pgConnection.prepareStatement(sql)) {
            for (int i = 0; i < count; i++) {
                StringBuilder vecStr = new StringBuilder("[");
                for (int d = 0; d < dim; d++) {
                    if (d > 0) {
                        vecStr.append(",");
                    }
                    vecStr.append(random.nextFloat());
                }
                vecStr.append("]");

                ps.setLong(1, i);
                ps.setString(2, vecStr.toString());
                ps.setString(3, "cat_" + (i % 10));
                ps.addBatch();

                if ((i + 1) % 100 == 0) {
                    ps.executeBatch();
                }
            }
            ps.executeBatch();
        }
        log.info("Inserted {} rows into pgvector table '{}'", count, tableName);
    }

    private long getPgRowCount(String tableName) throws Exception {
        try (Statement stmt = pgConnection.createStatement();
                java.sql.ResultSet rs =
                        stmt.executeQuery("SELECT COUNT(*) FROM \"" + tableName + "\"")) {
            rs.next();
            return rs.getLong(1);
        }
    }
}
