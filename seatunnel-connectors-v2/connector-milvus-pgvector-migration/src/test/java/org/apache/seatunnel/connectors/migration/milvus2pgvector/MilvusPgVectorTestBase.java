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

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.index.request.CreateIndexReq;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.milvus.MilvusContainer;
import org.testcontainers.utility.DockerImageName;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Base class for E2E tests. Manages Milvus + PostgreSQL (pgvector) containers and provides helper
 * methods for creating collections, inserting data, and building migration configs.
 */
@Slf4j
public abstract class MilvusPgVectorTestBase {

    protected static final Network NETWORK = Network.newNetwork();
    protected static final int VECTOR_DIM = 128;
    protected static final String PG_IMAGE = "pgvector/pgvector:pg16";
    protected static final String MILVUS_IMAGE = "milvusdb/milvus:v2.6-latest";
    protected static final String PG_DATABASE = "vectordb";
    protected static final String PG_USER = "postgres";
    protected static final String PG_PASSWORD = "postgres";

    protected static MilvusContainer milvusContainer;
    protected static PostgreSQLContainer<?> pgContainer;
    protected static MilvusClientV2 milvusClient;
    protected static Connection pgConnection;
    protected static final Gson gson = new Gson();

    @BeforeAll
    static void startContainers() throws Exception {
        milvusContainer =
                new MilvusContainer(DockerImageName.parse(MILVUS_IMAGE)).withNetwork(NETWORK);
        pgContainer =
                new PostgreSQLContainer<>(DockerImageName.parse(PG_IMAGE))
                        .withNetwork(NETWORK)
                        .withDatabase(PG_DATABASE)
                        .withUsername(PG_USER)
                        .withPassword(PG_PASSWORD);

        Startables.deepStart(milvusContainer, pgContainer).get(2, TimeUnit.MINUTES);

        // Connect to Milvus
        milvusClient =
                new MilvusClientV2(
                        ConnectConfig.builder().uri(milvusContainer.getEndpoint()).build());
        log.info("Milvus started at {}", milvusContainer.getEndpoint());

        // Connect to PostgreSQL
        Class.forName("org.postgresql.Driver");
        pgConnection =
                DriverManager.getConnection(
                        pgContainer.getJdbcUrl(), PG_USER, PG_PASSWORD);
        log.info("PostgreSQL started at {}", pgContainer.getJdbcUrl());

        // Install pgvector extension
        try (Statement stmt = pgConnection.createStatement()) {
            stmt.execute("CREATE EXTENSION IF NOT EXISTS vector");
        }
        log.info("pgvector extension installed");
    }

    @AfterAll
    static void stopContainers() throws Exception {
        if (milvusClient != null) {
            milvusClient.close();
        }
        if (pgConnection != null) {
            pgConnection.close();
        }
        if (milvusContainer != null) {
            milvusContainer.stop();
        }
        if (pgContainer != null) {
            pgContainer.stop();
        }
    }

    /** Create a simple Milvus collection with an Int64 PK and a 128-dim float vector. */
    protected void createSimpleCollection(String collectionName, int dim) throws Exception {
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
                        .dimension(dim)
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
                        .collectionName(collectionName)
                        .collectionSchema(schema)
                        .build());
        log.info("Created Milvus collection '{}' with dim={}", collectionName, dim);
    }

    /** Create an IVF_FLAT index on the vector field. */
    protected void createVectorIndex(String collectionName, String fieldName) {
        milvusClient.createIndex(
                CreateIndexReq.builder()
                        .collectionName(collectionName)
                        .indexParams(
                                java.util.Collections.singletonList(
                                        IndexParam.builder()
                                                .fieldName(fieldName)
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
        log.info("Created IVF_FLAT index on {}.{}", collectionName, fieldName);
    }

    /** Insert random test data into the collection. */
    protected void insertTestData(String collectionName, int count, int dim) throws Exception {
        Random random = new Random(42);
        List<JsonObject> rows = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            JsonObject row = new JsonObject();
            row.addProperty("id", i);
            List<Float> vector = new ArrayList<>(dim);
            for (int d = 0; d < dim; d++) {
                vector.add(random.nextFloat());
            }
            row.add("vector", gson.toJsonTree(vector));
            row.addProperty("category", "cat_" + (i % 10));
            rows.add(row);
        }
        milvusClient.insert(
                io.milvus.v2.service.vector.request.InsertReq.builder()
                        .collectionName(collectionName)
                        .data(rows)
                        .build());
        log.info("Inserted {} rows into '{}'", count, collectionName);
    }

    /** Load collection into memory so it can be queried. */
    protected void loadCollection(String collectionName) {
        milvusClient.loadCollection(
                LoadCollectionReq.builder().collectionName(collectionName).build());
        log.info("Loaded collection '{}'", collectionName);
    }

    /** Build a MigrationConfig pointing to the test containers. */
    protected MigrationConfig buildConfig(String collectionName, String tableName) {
        return MigrationConfig.builder()
                .milvusUrl(milvusContainer.getEndpoint())
                .milvusToken("")
                .milvusDatabase("default")
                .milvusCollection(collectionName)
                .pgUrl(pgContainer.getJdbcUrl())
                .pgUser(PG_USER)
                .pgPassword(PG_PASSWORD)
                .pgSchema("public")
                .pgTable(tableName)
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

    /** Get the count of rows in a pgvector table. */
    protected long getPgRowCount(String tableName) throws SQLException {
        try (Statement stmt = pgConnection.createStatement();
                java.sql.ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM \"" + tableName + "\"")) {
            rs.next();
            return rs.getLong(1);
        }
    }
}
