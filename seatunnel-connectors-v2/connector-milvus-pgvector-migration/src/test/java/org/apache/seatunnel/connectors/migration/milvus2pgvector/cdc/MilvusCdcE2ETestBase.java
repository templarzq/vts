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

package org.apache.seatunnel.connectors.migration.milvus2pgvector.cdc;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.cdc.metrics.CdcMetricsCollector;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.cdc.metrics.CdcTestReport;
import org.apache.seatunnel.connectors.seatunnel.milvus.source.utils.MilvusSourceConverter;
import org.apache.seatunnel.connectors.seatunnel.milvus.source.utils.MilvusSourceConnectorUtils;

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.HasCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.collection.request.ReleaseCollectionReq;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.vector.request.DeleteReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.UpsertReq;
import io.milvus.v2.service.vector.response.QueryResp;

import lombok.extern.slf4j.Slf4j;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import milvus.com.google.gson.Gson;
import milvus.com.google.gson.JsonObject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Comprehensive base class for Milvus CDC → pgvector E2E tests.
 *
 * <p>This base class manages:
 *
 * <ul>
 *   <li><b>Test environment setup</b> — connects to a pre-deployed Milvus 2.6.x instance (docker
 *       compose at /home/zhangqiang/project/vts/vts.yaml) and PostgreSQL 16 with pgvector (data
 *       dir /home/zhangqiang/pg/data). It creates and tears down the test database automatically.
 *   <li><b>Multi-scale data preparation</b> — small (100 rows × 8 dim), medium (1000 rows × 128
 *       dim), large (5000 rows × 256 dim) datasets with both vector and scalar fields.
 *   <li><b>CDC strategy wiring</b> — helper to construct {@link PollingIncrementalCdcStrategy}
 *       with proper schema introspection.
 *   <li><b>Validation helpers</b> — row count, vector similarity, scalar field comparison.
 *   <li><b>Metrics & report generation</b> — aggregated {@link CdcTestReport} written to
 *       {@code target/cdc-e2e-reports/}.
 * </ul>
 *
 * <p><b>Test execution steps (common to all scenarios):</b>
 *
 * <ol>
 *   <li>Environment initialization — connect to Milvus + PostgreSQL, install pgvector extension.
 *   <li>Create Milvus collection with vector + scalar fields, build IVF_FLAT index, load.
 *   <li>Insert test data at the requested scale.
 *   <li>Trigger CDC snapshot/incremental via {@link CdcStrategy#pollChanges}.
 *   <li>Apply polled rows into pgvector via JDBC.
 *   <li>Verify row count, vector cosine similarity, and scalar field equality.
 *   <li>Record metrics & generate report.
 * </ol>
 *
 * <p>Enable tests with {@code -Dmigration.cdc.e2e.enabled=true}.
 */
@Slf4j
public abstract class MilvusCdcE2ETestBase {

    // ---- Connection configuration (matches /home/zhangqiang/project/vts/vts.yaml) ----
    protected static final String MILVUS_URL = "http://localhost:19530";
    protected static final String MILVUS_TOKEN = "";
    protected static final String MILVUS_DB = "default";

    // ---- PostgreSQL configuration (matches /home/zhangqiang/pg/data) ----
    protected static final String PG_HOST = "localhost";
    protected static final int PG_PORT = 5432;
    protected static final String PG_USER = "zhangqiang";
    protected static final String PG_PASSWORD = "";
    protected static final String PG_ADMIN_DB = "postgres";
    protected static final String PG_TEST_DATABASE = "vts_cdc_e2e";
    protected static final String PG_SCHEMA = "public";

    // ---- Validation thresholds (mirror cdc-e2e-test-config.yaml) ----
    protected static final double SIMILARITY_THRESHOLD = 0.9999;
    protected static final double MIN_SUCCESS_RATE = 0.99;
    protected static final long SNAPSHOT_THROUGHPUT_MIN_RPS = 50;
    protected static final long INCREMENTAL_P99_MAX_MS = 5000;
    protected static final int SAMPLE_SIZE = 100;
    protected static final String REPORT_DIR = "target/cdc-e2e-reports";

    // ---- Shared state ----
    protected static MilvusClientV2 milvusClient;
    protected static Connection pgConnection;
    protected static String pgJdbcUrl;
    protected static final Gson gson = new Gson();
    protected static final CdcMetricsCollector metrics = new CdcMetricsCollector();

    /** Subclasses override to provide their scenario label for the report. */
    protected abstract String scenarioLabel();

    @BeforeAll
    static void setUpBase() throws Exception {
        if (!isEnabled()) {
            log.warn("CDC E2E tests disabled. Set -Dmigration.cdc.e2e.enabled=true to run.");
            return;
        }
        metrics.start();

        // Connect to Milvus
        milvusClient =
                new MilvusClientV2(
                        ConnectConfig.builder()
                                .uri(MILVUS_URL)
                                .token(MILVUS_TOKEN)
                                .dbName(MILVUS_DB)
                                .connectTimeoutMs(30000)
                                .build());
        log.info("Connected to Milvus at {}", MILVUS_URL);

        // Recreate test database
        Class.forName("org.postgresql.Driver");
        String adminUrl = "jdbc:postgresql://" + PG_HOST + ":" + PG_PORT + "/" + PG_ADMIN_DB;
        try (Connection admin = DriverManager.getConnection(adminUrl, PG_USER, PG_PASSWORD);
                Statement stmt = admin.createStatement()) {
            stmt.execute("DROP DATABASE IF EXISTS " + PG_TEST_DATABASE);
            stmt.execute("CREATE DATABASE " + PG_TEST_DATABASE);
            log.info("Recreated test database: {}", PG_TEST_DATABASE);
        }

        // Connect to test database and install pgvector
        pgJdbcUrl = "jdbc:postgresql://" + PG_HOST + ":" + PG_PORT + "/" + PG_TEST_DATABASE;
        pgConnection = DriverManager.getConnection(pgJdbcUrl, PG_USER, PG_PASSWORD);
        pgConnection.setAutoCommit(true);
        try (Statement stmt = pgConnection.createStatement()) {
            stmt.execute("CREATE EXTENSION IF NOT EXISTS vector");
        }
        log.info("Connected to PostgreSQL & installed pgvector: {}", pgJdbcUrl);
    }

    @AfterAll
    static void tearDownBase() throws Exception {
        if (!isEnabled()) {
            return;
        }
        // Generate aggregated report
        CdcTestReport report =
                new CdcTestReport(
                        metrics,
                        "milvus-2.6.9 + pg16-pgvector",
                        REPORT_DIR,
                        SIMILARITY_THRESHOLD,
                        MIN_SUCCESS_RATE,
                        SNAPSHOT_THROUGHPUT_MIN_RPS,
                        INCREMENTAL_P99_MAX_MS);
        report.generate();

        // Close connections
        if (milvusClient != null) {
            try {
                milvusClient.close();
            } catch (Exception e) {
                log.debug("Milvus client close failed: {}", e.getMessage());
            }
        }
        if (pgConnection != null) {
            try {
                pgConnection.close();
            } catch (Exception e) {
                log.debug("PG connection close failed: {}", e.getMessage());
            }
        }
        // Drop test database
        try {
            String adminUrl = "jdbc:postgresql://" + PG_HOST + ":" + PG_PORT + "/" + PG_ADMIN_DB;
            try (Connection admin = DriverManager.getConnection(adminUrl, PG_USER, PG_PASSWORD);
                    Statement stmt = admin.createStatement()) {
                stmt.execute("DROP DATABASE IF EXISTS " + PG_TEST_DATABASE);
                log.info("Dropped test database: {}", PG_TEST_DATABASE);
            }
        } catch (Exception e) {
            log.debug("Database drop failed: {}", e.getMessage());
        }
    }

    // ==================================================================
    // Milvus collection management
    // ==================================================================

    /**
     * Create a Milvus collection with a multi-field schema: Int64 PK, FloatVector, VarChar
     * scalar, Int64 scalar (timestamp). Drop existing collection first.
     */
    protected void createCollection(String collectionName, int vectorDim) throws Exception {
        // Drop if exists
        if (milvusClient.hasCollection(
                HasCollectionReq.builder().collectionName(collectionName).build())) {
            milvusClient.releaseCollection(
                    ReleaseCollectionReq.builder().collectionName(collectionName).build());
            milvusClient.dropCollection(
                    DropCollectionReq.builder().collectionName(collectionName).build());
            log.info("Dropped existing collection: {}", collectionName);
        }

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
                        .dimension(vectorDim)
                        .build());
        fields.add(
                CreateCollectionReq.FieldSchema.builder()
                        .name("category")
                        .dataType(DataType.VarChar)
                        .maxLength(64)
                        .isNullable(true)
                        .build());
        fields.add(
                CreateCollectionReq.FieldSchema.builder()
                        .name("created_at")
                        .dataType(DataType.Int64)
                        .isNullable(true)
                        .build());

        milvusClient.createCollection(
                CreateCollectionReq.builder()
                        .collectionName(collectionName)
                        .collectionSchema(
                                CreateCollectionReq.CollectionSchema.builder()
                                        .fieldSchemaList(fields)
                                        .build())
                        .build());

        // Create IVF_FLAT index
        milvusClient.createIndex(
                CreateIndexReq.builder()
                        .collectionName(collectionName)
                        .indexParams(
                                Collections.singletonList(
                                        IndexParam.builder()
                                                .fieldName("vector")
                                                .indexType(IndexParam.IndexType.IVF_FLAT)
                                                .metricType(IndexParam.MetricType.COSINE)
                                                .extraParams(
                                                        new HashMap<String, Object>() {
                                                            {
                                                                put("nlist", 128);
                                                            }
                                                        })
                                                .build()))
                        .build());

        // Load collection
        milvusClient.loadCollection(
                LoadCollectionReq.builder().collectionName(collectionName).build());
        // Wait for load completion
        Thread.sleep(3000);
        log.info("Created & loaded collection '{}' with dim={}", collectionName, vectorDim);
    }

    /** Insert test data with deterministic vectors (seeded Random). */
    protected long insertData(String collectionName, int startId, int count, int dim, long seed)
            throws Exception {
        Random random = new Random(seed);
        List<JsonObject> rows = new ArrayList<>();
        long firstWriteTs = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            int id = startId + i;
            JsonObject row = new JsonObject();
            row.addProperty("id", id);
            List<Float> vector = new ArrayList<>(dim);
            for (int d = 0; d < dim; d++) {
                vector.add(random.nextFloat());
            }
            row.add("vector", gson.toJsonTree(vector));
            row.addProperty("category", "cat_" + (id % 10));
            row.addProperty("created_at", System.currentTimeMillis() / 1000);
            rows.add(row);
        }
        milvusClient.insert(
                InsertReq.builder().collectionName(collectionName).data((List) rows).build());
        log.info("Inserted {} rows (id {}..{}) into '{}'", count, startId, startId + count - 1,
                collectionName);
        return firstWriteTs;
    }

    /** Upsert rows — used for incremental update tests. */
    protected void upsertData(String collectionName, int startId, int count, int dim, long seed)
            throws Exception {
        Random random = new Random(seed);
        List<JsonObject> rows = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            int id = startId + i;
            JsonObject row = new JsonObject();
            row.addProperty("id", id);
            List<Float> vector = new ArrayList<>(dim);
            for (int d = 0; d < dim; d++) {
                vector.add(random.nextFloat() * 2.0f); // different vector values
            }
            row.add("vector", gson.toJsonTree(vector));
            row.addProperty("category", "updated_" + (id % 5));
            row.addProperty("created_at", System.currentTimeMillis() / 1000);
            rows.add(row);
        }
        milvusClient.upsert(
                UpsertReq.builder().collectionName(collectionName).data((List) rows).build());
        log.info("Upserted {} rows (id {}..{}) into '{}'", count, startId, startId + count - 1,
                collectionName);
    }

    /** Delete rows by PK filter — used for incremental delete tests. */
    protected void deleteData(String collectionName, int startId, int count) throws Exception {
        String filter = "id >= " + startId + " && id < " + (startId + count);
        milvusClient.delete(
                DeleteReq.builder().collectionName(collectionName).filter(filter).build());
        log.info("Deleted {} rows (id {}..{}) from '{}'", count, startId, startId + count - 1,
                collectionName);
    }

    /** Drop collection at end of test. */
    protected void dropCollection(String collectionName) {
        try {
            milvusClient.releaseCollection(
                    ReleaseCollectionReq.builder().collectionName(collectionName).build());
        } catch (Exception ignored) {
        }
        try {
            milvusClient.dropCollection(
                    DropCollectionReq.builder().collectionName(collectionName).build());
            log.info("Dropped collection: {}", collectionName);
        } catch (Exception e) {
            log.debug("Collection drop failed: {}", e.getMessage());
        }
    }

    // ==================================================================
    // PgVector target helpers
    // ==================================================================

    protected void createPgTable(String tableName, int vectorDim) throws SQLException {
        try (Statement stmt = pgConnection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS " + PG_SCHEMA + "." + tableName + " CASCADE");
            stmt.execute(
                    "CREATE TABLE "
                            + PG_SCHEMA
                            + "."
                            + tableName
                            + " ("
                            + "id BIGINT PRIMARY KEY, "
                            + "vector vector("
                            + vectorDim
                            + "), "
                            + "category VARCHAR(64), "
                            + "created_at BIGINT"
                            + ")");
        }
        log.info("Created pgvector table: {}.{}", PG_SCHEMA, tableName);
    }

    protected void dropPgTable(String tableName) {
        try (Statement stmt = pgConnection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS " + PG_SCHEMA + "." + tableName + " CASCADE");
        } catch (Exception e) {
            log.debug("PG table drop failed: {}", e.getMessage());
        }
    }

    /** Insert a batch of CDC-polled rows into pgvector. */
    protected int applyRowsToPg(
            String tableName, List<SeaTunnelRowWithPosition> rows, int vectorDim)
            throws Exception {
        String sql =
                "INSERT INTO "
                        + PG_SCHEMA
                        + "."
                        + tableName
                        + " (id, vector, category, created_at) VALUES (?, ?::vector, ?, ?)"
                        + " ON CONFLICT (id) DO UPDATE SET vector = EXCLUDED.vector, "
                        + "category = EXCLUDED.category, created_at = EXCLUDED.created_at";
        int applied = 0;
        try (PreparedStatement ps = pgConnection.prepareStatement(sql)) {
            for (SeaTunnelRowWithPosition rowPos : rows) {
                SeaTunnelRow row = rowPos.getRow();
                Object[] fields = row.getFields();
                if (fields[0] == null) continue;
                ps.setLong(1, toLong(fields[0]));
                ps.setString(2, toVectorString(fields[1], vectorDim));
                ps.setString(3, fields[2] != null ? fields[2].toString() : null);
                ps.setLong(4, fields[3] != null ? toLong(fields[3]) : 0);
                ps.addBatch();
                applied++;
                if (applied % 100 == 0) {
                    ps.executeBatch();
                }
            }
            ps.executeBatch();
        }
        return applied;
    }

    protected int queryPgCount(String tableName) throws SQLException {
        try (Statement stmt = pgConnection.createStatement();
                ResultSet rs =
                        stmt.executeQuery(
                                "SELECT COUNT(*) FROM " + PG_SCHEMA + "." + tableName)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    protected long queryMilvusCount(String collectionName) throws Exception {
        try {
            QueryResp resp =
                    milvusClient.query(
                            QueryReq.builder()
                                    .collectionName(collectionName)
                                    .filter("id >= 0")
                                    .outputFields(Collections.singletonList("count(*)"))
                                    .build());
            if (resp.getQueryResults() != null && !resp.getQueryResults().isEmpty()) {
                Object c = resp.getQueryResults().get(0).getEntity().get("count(*)");
                if (c instanceof Number) {
                    return ((Number) c).longValue();
                }
            }
            return -1L;
        } catch (Exception e) {
            // Collection may not exist or not loaded — treat as -1 (used for health checks
            // where the caller only cares that Milvus is reachable, not the actual count).
            log.debug("queryMilvusCount({}) failed: {}", collectionName, e.getMessage());
            return -1L;
        }
    }

    // ==================================================================
    // CDC strategy wiring
    // ==================================================================

    /**
     * Build a {@link PollingIncrementalCdcStrategy} for the given collection, introspecting the
     * schema via MilvusSourceConnectorUtils.
     */
    protected PollingIncrementalCdcStrategy buildPollingStrategy(
            String collectionName, int batchSize) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", MILVUS_URL);
        configMap.put("token", MILVUS_TOKEN);
        configMap.put("database", MILVUS_DB);
        configMap.put("collection", collectionName);
        // MilvusSourceConfig.COLLECTION uses key "collections" (plural, list type).
        // Specifying it ensures MilvusSourceConnectorUtils.getTables() only introspects
        // the target collection instead of listing all collections in the database
        // (which could pick up residual collections with different schemas).
        configMap.put("collections", Collections.singletonList(collectionName));
        configMap.put("batch_size", batchSize);
        configMap.put("poll_interval_ms", 500L);
        configMap.put("startup_mode", "INITIAL");
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
        return new PollingIncrementalCdcStrategy(cdcConfig, converter, tableSchema);
    }

    /**
     * Build a {@link GrpcReplicateCdcStrategy} for the given collection. The gRPC strategy
     * creates its own Milvus client and gRPC channel internally; callers must close it after
     * use (try-with-resources or explicit close).
     */
    protected GrpcReplicateCdcStrategy buildGrpcStrategy(
            String collectionName, int batchSize) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", MILVUS_URL);
        configMap.put("token", MILVUS_TOKEN);
        configMap.put("database", MILVUS_DB);
        configMap.put("collection", collectionName);
        configMap.put("batch_size", batchSize);
        configMap.put("incremental_batch_size", 500L);
        configMap.put("poll_interval_ms", 500L);
        configMap.put("startup_mode", "INITIAL");
        configMap.put("cdc_strategy", "grpc_replicate");
        configMap.put("primary_key_field", "id");
        configMap.put("channel_timeout_ms", 10000L);
        configMap.put("parallelism", 1);
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(configMap);
        MilvusCdcSourceConfig cdcConfig = MilvusCdcSourceConfig.of(readonlyConfig);
        return new GrpcReplicateCdcStrategy(cdcConfig);
    }

    /**
     * Run a snapshot read via any {@link CdcStrategy}: poll all rows with id &gt; -1 until
     * exhausted. Strategy-agnostic version of {@link #runSnapshot}.
     */
    protected List<SeaTunnelRowWithPosition> runCdcSnapshot(
            CdcStrategy strategy, String collectionName, int expectedRows) throws Exception {
        MilvusCdcSourceSplit split =
                MilvusCdcSourceSplit.builder()
                        .splitId("snap-" + collectionName)
                        .collectionName(collectionName)
                        .snapshot(true)
                        .startId(-1L)
                        .endId(Long.MAX_VALUE)
                        .offset(0L)
                        .limit(-1L)
                        .build();

        List<SeaTunnelRowWithPosition> all = new ArrayList<>();
        ReplicatePosition pos = null;
        for (int poll = 0; poll < 50; poll++) {
            List<SeaTunnelRowWithPosition> batch = strategy.pollChanges(split, pos);
            if (batch.isEmpty()) {
                break;
            }
            all.addAll(batch);
            pos = batch.get(batch.size() - 1).getPosition();
            if (all.size() >= expectedRows) {
                break;
            }
        }
        return all;
    }

    /**
     * Run a single incremental poll via any {@link CdcStrategy} from the given watermark.
     * Strategy-agnostic version of {@link #runIncrementalPoll}.
     */
    protected List<SeaTunnelRowWithPosition> runCdcIncrementalPoll(
            CdcStrategy strategy, String collectionName, long watermark) throws Exception {
        MilvusCdcSourceSplit split =
                MilvusCdcSourceSplit.builder()
                        .splitId("inc-" + collectionName)
                        .collectionName(collectionName)
                        .snapshot(false)
                        .startId(watermark)
                        .endId(Long.MAX_VALUE)
                        .build();
        return strategy.pollChanges(split, null);
    }

    /**
     * Run a snapshot read: poll all rows with id &gt; -1 until exhausted. Returns all collected
     * rows.
     */
    protected List<SeaTunnelRowWithPosition> runSnapshot(
            PollingIncrementalCdcStrategy strategy, String collectionName, int expectedRows)
            throws Exception {
        MilvusCdcSourceSplit split =
                MilvusCdcSourceSplit.builder()
                        .splitId("snap-" + collectionName)
                        .collectionName(collectionName)
                        .snapshot(true)
                        .startId(-1L)
                        .endId(Long.MAX_VALUE)
                        .offset(0L)
                        .limit(-1L)
                        .build();

        List<SeaTunnelRowWithPosition> all = new ArrayList<>();
        ReplicatePosition pos = null;
        for (int poll = 0; poll < 50; poll++) {
            List<SeaTunnelRowWithPosition> batch = strategy.pollChanges(split, pos);
            if (batch.isEmpty()) {
                break;
            }
            all.addAll(batch);
            pos = batch.get(batch.size() - 1).getPosition();
            if (all.size() >= expectedRows) {
                break;
            }
        }
        return all;
    }

    /**
     * Run a single incremental poll from the given watermark. Updates split.startId in-place.
     */
    protected List<SeaTunnelRowWithPosition> runIncrementalPoll(
            PollingIncrementalCdcStrategy strategy, String collectionName, long watermark)
            throws Exception {
        MilvusCdcSourceSplit split =
                MilvusCdcSourceSplit.builder()
                        .splitId("inc-" + collectionName)
                        .collectionName(collectionName)
                        .snapshot(false)
                        .startId(watermark)
                        .endId(Long.MAX_VALUE)
                        .build();
        return strategy.pollChanges(split, null);
    }

    /**
     * Poll repeatedly with backoff until either {@code expectedRows} are collected or {@code
     * maxPolls} are exhausted. Returns all collected rows.
     */
    protected List<SeaTunnelRowWithPosition> pollUntil(
            PollingIncrementalCdcStrategy strategy,
            String collectionName,
            long initialWatermark,
            int expectedRows,
            int maxPolls,
            long backoffMs)
            throws Exception {
        List<SeaTunnelRowWithPosition> all = new ArrayList<>();
        long watermark = initialWatermark;
        for (int i = 0; i < maxPolls; i++) {
            List<SeaTunnelRowWithPosition> batch =
                    runIncrementalPoll(strategy, collectionName, watermark);
            if (!batch.isEmpty()) {
                all.addAll(batch);
                // Get last row's id as new watermark (polling strategy uses timeTick)
                SeaTunnelRowWithPosition last = batch.get(batch.size() - 1);
                if (last.getPosition() != null && last.getPosition().getTimeTick() > 0) {
                    watermark = last.getPosition().getTimeTick();
                }
                if (all.size() >= expectedRows) {
                    break;
                }
            } else {
                Thread.sleep(backoffMs);
            }
        }
        return all;
    }

    // ==================================================================
    // Validation helpers
    // ==================================================================

    /** Verify Milvus row count matches expected. */
    protected void verifyMilvusCount(String collectionName, long expected) throws Exception {
        // Wait for Milvus to make inserts visible
        for (int i = 0; i < 5; i++) {
            long actual = queryMilvusCount(collectionName);
            if (actual >= expected) {
                return;
            }
            Thread.sleep(1000);
        }
        long actual = queryMilvusCount(collectionName);
        assertTrue(
                actual >= expected,
                "Milvus count=" + actual + " < expected=" + expected);
    }

    /** Verify pgvector row count. */
    protected void verifyPgCount(String tableName, int expected) throws Exception {
        int actual = queryPgCount(tableName);
        assertTrue(
                actual >= expected,
                "pgvector count=" + actual + " < expected=" + expected);
    }

    /**
     * Verify vector cosine similarity between Milvus and pgvector for a sample of PKs. Records
     * each similarity to the metrics collector.
     */
    protected void verifyVectorSimilarity(
            String scenarioName, String collectionName, String tableName, int dim, int sampleSize)
            throws Exception {
        // Sample PKs from Milvus
        QueryResp resp =
                milvusClient.query(
                        QueryReq.builder()
                                .collectionName(collectionName)
                                .filter("id >= 0")
                                .outputFields(Collections.singletonList("*"))
                                .limit((long) sampleSize)
                                .build());

        List<QueryResp.QueryResult> results = resp.getQueryResults();
        if (results == null || results.isEmpty()) {
            log.warn("No results from Milvus for similarity check");
            return;
        }

        for (QueryResp.QueryResult r : results) {
            Map<String, Object> entity = r.getEntity();
            Object idObj = entity.get("id");
            if (idObj == null) continue;
            long id = toLong(idObj);

            float[] milvusVec = extractMilvusVector(entity.get("vector"), dim);
            if (milvusVec == null) continue;

            // Query pgvector
            float[] pgVec = queryPgVector(tableName, id);
            if (pgVec == null) {
                metrics.recordAnomaly(
                        scenarioName,
                        "MISSING_IN_PGVECTOR",
                        "id=" + id + " not found in pgvector");
                continue;
            }

            double sim = cosineSimilarity(milvusVec, pgVec);
            metrics.recordSimilarity(scenarioName, sim);
            if (sim < SIMILARITY_THRESHOLD) {
                metrics.recordAnomaly(
                        scenarioName,
                        "SIMILARITY_BELOW_THRESHOLD",
                        "id=" + id + " sim=" + sim + " < " + SIMILARITY_THRESHOLD);
            }
        }
    }

    /** Verify scalar field equality for a sample of rows. */
    protected void verifyScalarFields(
            String scenarioName, String collectionName, String tableName, int sampleSize)
            throws Exception {
        QueryResp resp =
                milvusClient.query(
                        QueryReq.builder()
                                .collectionName(collectionName)
                                .filter("id >= 0")
                                .outputFields(Collections.singletonList("*"))
                                .limit((long) sampleSize)
                                .build());

        for (QueryResp.QueryResult r : resp.getQueryResults()) {
            Map<String, Object> entity = r.getEntity();
            long id = toLong(entity.get("id"));
            String milvusCategory = entity.get("category") != null ? entity.get("category").toString() : null;

            try (PreparedStatement ps =
                    pgConnection.prepareStatement(
                            "SELECT category FROM "
                                    + PG_SCHEMA
                                    + "."
                                    + tableName
                                    + " WHERE id = ?")) {
                ps.setLong(1, id);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        metrics.recordAnomaly(
                                scenarioName,
                                "SCALAR_MISSING_IN_PG",
                                "id=" + id + " not in pgvector");
                        continue;
                    }
                    String pgCategory = rs.getString(1);
                    if (!stringEquals(milvusCategory, pgCategory)) {
                        metrics.recordAnomaly(
                                scenarioName,
                                "SCALAR_MISMATCH",
                                "id=" + id + " milvus=" + milvusCategory + " pg=" + pgCategory);
                    }
                }
            }
        }
    }

    // ==================================================================
    // Utility methods
    // ==================================================================

    protected static boolean isEnabled() {
        return "true".equals(System.getProperty("migration.cdc.e2e.enabled", "false"));
    }

    protected long toLong(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value instanceof String) {
            return Long.parseLong((String) value);
        }
        return 0L;
    }

    @SuppressWarnings("unchecked")
    protected float[] extractMilvusVector(Object vecObj, int dim) {
        if (vecObj == null) return null;
        if (vecObj instanceof List) {
            List<Number> list = (List<Number>) vecObj;
            float[] out = new float[list.size()];
            for (int i = 0; i < list.size(); i++) {
                out[i] = list.get(i).floatValue();
            }
            return out;
        }
        if (vecObj instanceof java.nio.ByteBuffer) {
            java.nio.ByteBuffer buf = (java.nio.ByteBuffer) vecObj;
            buf.rewind();
            float[] out = new float[buf.remaining() / 4];
            buf.asFloatBuffer().get(out);
            return out;
        }
        log.warn("Unknown Milvus vector type: {}", vecObj.getClass());
        return null;
    }

    protected float[] queryPgVector(String tableName, long id) throws SQLException {
        try (PreparedStatement ps =
                pgConnection.prepareStatement(
                        "SELECT vector::text FROM "
                                + PG_SCHEMA
                                + "."
                                + tableName
                                + " WHERE id = ?")) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                String text = rs.getString(1);
                if (text == null) return null;
                return parsePgVectorText(text);
            }
        }
    }

    protected static float[] parsePgVectorText(String text) {
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
        throw new IllegalArgumentException("Unrecognized pgvector text: " + text);
    }

    protected String toVectorString(Object vecObj, int dim) {
        if (vecObj == null) return "[]";
        float[] floats = extractMilvusVector(vecObj, dim);
        if (floats == null) return "[]";
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < floats.length; i++) {
            if (i > 0) sb.append(",");
            sb.append(floats[i]);
        }
        sb.append("]");
        return sb.toString();
    }

    protected static double cosineSimilarity(float[] a, float[] b) {
        if (a.length != b.length) return -1.0;
        double dot = 0.0, na = 0.0, nb = 0.0;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
            na += a[i] * a[i];
            nb += b[i] * b[i];
        }
        if (na == 0 || nb == 0) return 0.0;
        return dot / (Math.sqrt(na) * Math.sqrt(nb));
    }

    protected static boolean stringEquals(String a, String b) {
        if (a == null && b == null) return true;
        if (a == null || b == null) return false;
        return a.equals(b);
    }
}
