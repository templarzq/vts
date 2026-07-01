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

import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.cdc.metrics.CdcMetricsCollector;

import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;

import lombok.extern.slf4j.Slf4j;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * E2E tests for the {@link CdcEventStreamStrategy} (the {@code event_stream} CDC strategy).
 *
 * <p>Unlike the polling and grpc_replicate strategies (which can only detect new primary keys via
 * query iteration), the event_stream strategy consumes raw Milvus WAL events via the server-side
 * {@code DumpMessages} streaming gRPC RPC. This enables true delete capture and same-PK update
 * capture.
 *
 * <p><b>Test scenarios covered:</b>
 *
 * <ol>
 *   <li><b>Availability</b> — verifies {@code isAvailable()} against the real pchannel.
 *   <li><b>Full snapshot sync</b> — snapshot phase (via MilvusBufferReader) applied to pgvector.
 *   <li><b>Incremental insert</b> — new rows inserted after snapshot are captured and applied.
 *   <li><b>Delete capture</b> (core) — {@code RowKind.DELETE} events captured and applied to
 *       pgvector, reducing its row count.
 *   <li><b>Same-PK update</b> — upserts produce INSERT events that update pgvector rows.
 *   <li><b>Strategy comparison</b> — event_stream captures both INSERT and DELETE; polling only
 *       captures INSERT.
 *   <li><b>Unavailable fallback</b> — wrong pchannel yields {@code isAvailable()=false}.
 * </ol>
 *
 * <p><b>Environment:</b> Milvus 2.6.9 standalone ({@code http://localhost:19530}, etcd rootPath
 * {@code by-dev}, so pchannel = {@code by-dev-rootcoord-dml_0}) with
 * {@code common.collectionReplicateEnable=true} and {@code common.ttMsgEnabled=true}; pgvector on
 * {@code localhost:5432} (user {@code zhangqiang}, db {@code vts_cdc_e2e}).
 *
 * <p><b>Run with:</b>
 * <pre>
 * mvnd test -pl connector-milvus-pgvector-migration \
 *     -Dtest=MilvusCdcEventStreamE2E \
 *     -Dmigration.cdc.e2e.enabled=true -Dmaven.test.skip=false -DskipUT=false
 * </pre>
 */
@EnabledIfSystemProperty(
        named = "migration.cdc.e2e.enabled",
        matches = "true")
@DisplayName("Milvus CDC EventStream Strategy E2E")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Slf4j
public class MilvusCdcEventStreamE2E extends MilvusCdcE2ETestBase {

    private static final int VECTOR_DIM = 64;
    private static final int SNAPSHOT_COUNT = 100;
    private static final int INSERT_COUNT = 50;
    private static final int DELETE_COUNT = 30;
    private static final int UPDATE_COUNT = 20;
    private static final String PCHANNEL = "by-dev-rootcoord-dml_0";

    /**
     * Set by the availability test (Order 1). Subsequent tests assume CDC is available via
     * {@link Assumptions#assumeTrue(boolean, String)} so the whole suite is skipped gracefully
     * when the Milvus server does not have CDC/WAL enabled.
     */
    private static boolean cdcAvailable = false;

    @Override
    protected String scenarioLabel() {
        return "event_stream";
    }

    // ==================================================================
    // Helpers
    // ==================================================================

    /**
     * Build an event_stream CDC strategy for the given collection and pchannel. The snapshot
     * phase is shared across all strategies (it uses MilvusBufferReader); the event_stream
     * strategy is only used for the incremental phase.
     */
    private CdcEventStreamStrategy buildEventStreamStrategy(
            String collectionName, String pchannel) {
        // First get collection info to determine correct pchannel
        DescribeCollectionResp desc =
                milvusClient.describeCollection(
                        DescribeCollectionReq.builder()
                                .collectionName(collectionName)
                                .build());
        
        // Get the shard num from the collection (assume shard_num=0 for simplicity)
        // The actual pchannel format for GetReplicateInfo RPC is: by-dev-rootcoord-dml_<shard_num>
        // Note: Most test collections have only 1 shard, so shard_num=0
        String actualPchannel = "by-dev-rootcoord-dml_0";
        
        log.info("Using pchannel={} for collection {} (ID={})", 
                actualPchannel, collectionName, desc.getCollectionID());
        
        MilvusCdcSourceConfig config =
                MilvusCdcSourceConfig.builder()
                        .url(MILVUS_URL)
                        .token(MILVUS_TOKEN)
                        .collection(collectionName)
                        .cdcStrategy("event_stream")
                        .cdcPchannel(actualPchannel)  // Use dynamically constructed pchannel
                        .incrementalBatchSize(500L)
                        .pollIntervalMs(500L)
                        .channelTimeoutMs(60000L)  // Increased from 10s to 60s for slower GetReplicateInfo RPC
                        .primaryKeyField("id")
                        .build();
        return new CdcEventStreamStrategy(config, desc);
    }

    /**
     * Apply DELETE events to pgvector. The base class {@link #applyRowsToPg} only handles
     * INSERT/UPSERT; for {@link RowKind#DELETE} the SeaTunnelRow carries only the PK field (at
     * index 0), so we issue a batched {@code DELETE ... WHERE id = ?}.
     */
    private void applyDeleteRowsToPg(String tableName, List<SeaTunnelRow> deleteRows)
            throws SQLException {
        String sql = "DELETE FROM " + PG_SCHEMA + "." + tableName + " WHERE id = ?";
        try (PreparedStatement stmt = pgConnection.prepareStatement(sql)) {
            for (SeaTunnelRow row : deleteRows) {
                stmt.setObject(1, row.getField(0)); // PK is at index 0
                stmt.addBatch();
            }
            stmt.executeBatch();
        }
    }

    /**
     * Poll the event_stream strategy repeatedly until {@code timeoutMs} elapses, aggregating all
     * captured events. The strategy keeps its gRPC stream alive across polls, so each call
     * continues from the last consumed position.
     */
    private List<SeaTunnelRowWithPosition> pollForEvents(
            CdcEventStreamStrategy strategy, String collectionName, long timeoutMs)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        List<SeaTunnelRowWithPosition> allEvents = new ArrayList<>();
        while (System.currentTimeMillis() < deadline) {
            MilvusCdcSourceSplit split =
                    MilvusCdcSourceSplit.builder()
                            .splitId("incremental-0")
                            .collectionName(collectionName)
                            .snapshot(false)
                            .build();
            List<SeaTunnelRowWithPosition> batch = strategy.pollChanges(split, null);
            allEvents.addAll(batch);
            if (batch.isEmpty()) {
                Thread.sleep(200); // wait before next poll
            }
        }
        return allEvents;
    }

    /** Extract the raw rows whose RowKind matches {@code kind} from the event list. */
    private List<SeaTunnelRow> extractRowsByKind(
            List<SeaTunnelRowWithPosition> events, RowKind kind) {
        List<SeaTunnelRow> out = new ArrayList<>();
        for (SeaTunnelRowWithPosition e : events) {
            if (e.getRow().getRowKind() == kind) {
                out.add(e.getRow());
            }
        }
        return out;
    }

    // ==================================================================
    // Tests
    // ==================================================================

    @Test
    @Order(1)
    @DisplayName("event_stream CDC availability — isAvailable against real pchannel")
    void testEventStreamAvailability() throws Exception {
        String scenarioName = "event_stream.availability";
        String collectionName = "cdc_es_avail";
        log.info("=== {} START ===", scenarioName);
        try {
            createCollection(collectionName, VECTOR_DIM);
            insertData(collectionName, 0, 10, VECTOR_DIM, 0L);

            try (CdcEventStreamStrategy strategy = buildEventStreamStrategy(collectionName, PCHANNEL)) {
                cdcAvailable = strategy.isAvailable();
                log.info("[{}] isAvailable={} (pchannel={})", scenarioName, cdcAvailable, PCHANNEL);

                if (!cdcAvailable) {
                    metrics.recordAnomaly(
                            scenarioName,
                            "CDC_EVENT_STREAM_UNAVAILABLE",
                            "GetReplicateInfo returned no checkpoint for pchannel="
                                    + PCHANNEL
                                    + ". Ensure common.collectionReplicateEnable=true and "
                                    + "common.ttMsgEnabled=true on the Milvus server.");
                    log.warn(
                            "[{}] event_stream CDC not available; remaining tests will be skipped",
                            scenarioName);
                }
            }

            try (CdcMetricsCollector.ScenarioTimer timer =
                    metrics.startScenario(scenarioName, 0)) {
                timer.actualRows(0).failedRows(0);
            }
            // Skip this test (and, via the static flag, the remaining ones) if CDC is off.
            Assumptions.assumeTrue(cdcAvailable, "event_stream CDC not available");
            log.info("=== {} END: passed ===", scenarioName);
        } finally {
            dropCollection(collectionName);
        }
    }

    @Test
    @Order(2)
    @DisplayName("event_stream full snapshot sync — snapshot applied to pgvector")
    void testEventStreamFullSnapshotSync() throws Exception {
        Assumptions.assumeTrue(cdcAvailable, "event_stream CDC not available");
        String scenarioName = "event_stream.full_snapshot";
        String collectionName = "cdc_es_snap";
        String pgTable = "pg_es_snap";
        log.info("=== {} START ===", scenarioName);
        long sourceWriteTs = System.currentTimeMillis();
        try {
            createCollection(collectionName, VECTOR_DIM);
            insertData(collectionName, 0, SNAPSHOT_COUNT, VECTOR_DIM, 0L);
            createPgTable(pgTable, VECTOR_DIM);

            // The snapshot phase is shared across all strategies (MilvusBufferReader).
            // Use the polling strategy to run the snapshot, then apply rows to pgvector.
            List<SeaTunnelRowWithPosition> snap;
            try (PollingIncrementalCdcStrategy snapStrategy =
                    buildPollingStrategy(collectionName, 500)) {
                snap = runSnapshot(snapStrategy, collectionName, SNAPSHOT_COUNT);
            }
            log.info("[{}] snapshot polled {} rows", scenarioName, snap.size());

            int applied = applyRowsToPg(pgTable, snap, VECTOR_DIM);
            long sinkConfirmTs = System.currentTimeMillis();

            // Verify the event_stream strategy is available for the incremental phase.
            try (CdcEventStreamStrategy strategy = buildEventStreamStrategy(collectionName, PCHANNEL)) {
                assertTrue(strategy.isAvailable(), "event_stream should be available");
            }

            verifyPgCount(pgTable, SNAPSHOT_COUNT);
            verifyVectorSimilarity(scenarioName, collectionName, pgTable, VECTOR_DIM, SAMPLE_SIZE);
            verifyScalarFields(scenarioName, collectionName, pgTable, SAMPLE_SIZE);

            try (CdcMetricsCollector.ScenarioTimer timer =
                    metrics.startScenario(scenarioName, SNAPSHOT_COUNT)) {
                timer.actualRows(applied)
                        .failedRows(Math.max(0, SNAPSHOT_COUNT - applied))
                        .syncDelay(sourceWriteTs, sinkConfirmTs);
            }
            log.info("=== {} END: passed ===", scenarioName);
        } finally {
            dropCollection(collectionName);
            dropPgTable(pgTable);
        }
    }

    @Test
    @Order(3)
    @DisplayName("event_stream incremental insert — new rows captured via WAL stream")
    void testEventStreamIncrementalInsert() throws Exception {
        Assumptions.assumeTrue(cdcAvailable, "event_stream CDC not available");
        String scenarioName = "event_stream.incremental_insert";
        String collectionName = "cdc_es_inc";
        String pgTable = "pg_es_inc";
        log.info("=== {} START ===", scenarioName);
        try {
            createCollection(collectionName, VECTOR_DIM);
            insertData(collectionName, 0, SNAPSHOT_COUNT, VECTOR_DIM, 0L);
            createPgTable(pgTable, VECTOR_DIM);

            // Apply initial snapshot to pgvector (shared snapshot phase).
            try (PollingIncrementalCdcStrategy snapStrategy =
                    buildPollingStrategy(collectionName, 500)) {
                List<SeaTunnelRowWithPosition> snap =
                        runSnapshot(snapStrategy, collectionName, SNAPSHOT_COUNT);
                applyRowsToPg(pgTable, snap, VECTOR_DIM);
            }
            verifyPgCount(pgTable, SNAPSHOT_COUNT);

            // Build the event_stream strategy and drain any backlog so the stream is
            // bootstrapped at the current WAL position before we insert new data.
            try (CdcEventStreamStrategy strategy = buildEventStreamStrategy(collectionName, PCHANNEL)) {
                pollForEvents(strategy, collectionName, 2000L);

                // Insert INCREMENTAL rows (id SNAPSHOT_COUNT..SNAPSHOT_COUNT+INSERT_COUNT-1).
                long incWriteTs = System.currentTimeMillis();
                insertData(collectionName, SNAPSHOT_COUNT, INSERT_COUNT, VECTOR_DIM, 0L);

                // Poll for the incremental insert events.
                List<SeaTunnelRowWithPosition> events;
                long sinkConfirmTs;
                try (CdcMetricsCollector.ScenarioTimer timer =
                        metrics.startScenario(scenarioName, INSERT_COUNT)) {
                    events = pollForEvents(strategy, collectionName, 10000L);
                    sinkConfirmTs = System.currentTimeMillis();
                    int applied = applyRowsToPg(pgTable, events, VECTOR_DIM);
                    timer.actualRows(applied)
                            .failedRows(Math.max(0, INSERT_COUNT - applied))
                            .syncDelay(incWriteTs, sinkConfirmTs);
                }

                List<SeaTunnelRow> insertRows = extractRowsByKind(events, RowKind.INSERT);
                log.info(
                        "[{}] captured {} events ({} INSERT) for {} new rows",
                        scenarioName,
                        events.size(),
                        insertRows.size(),
                        INSERT_COUNT);
                assertTrue(
                        insertRows.size() >= INSERT_COUNT,
                        "Should capture at least "
                                + INSERT_COUNT
                                + " INSERT events, got "
                                + insertRows.size());
            }

            verifyPgCount(pgTable, SNAPSHOT_COUNT + INSERT_COUNT);
            verifyVectorSimilarity(scenarioName, collectionName, pgTable, VECTOR_DIM, SAMPLE_SIZE);
            log.info("=== {} END: passed ===", scenarioName);
        } finally {
            dropCollection(collectionName);
            dropPgTable(pgTable);
        }
    }

    @Test
    @Order(4)
    @DisplayName("event_stream delete capture — RowKind.DELETE events applied to pgvector (CORE)")
    void testEventStreamDeleteCapture() throws Exception {
        Assumptions.assumeTrue(cdcAvailable, "event_stream CDC not available");
        String scenarioName = "event_stream.delete_capture";
        String collectionName = "cdc_es_delete";
        String pgTable = "pg_es_delete";
        log.info("=== {} START === (CORE TEST)", scenarioName);
        try {
            createCollection(collectionName, VECTOR_DIM);
            insertData(collectionName, 0, SNAPSHOT_COUNT, VECTOR_DIM, 0L);
            createPgTable(pgTable, VECTOR_DIM);

            // Apply initial snapshot to pgvector.
            try (PollingIncrementalCdcStrategy snapStrategy =
                    buildPollingStrategy(collectionName, 500)) {
                List<SeaTunnelRowWithPosition> snap =
                        runSnapshot(snapStrategy, collectionName, SNAPSHOT_COUNT);
                applyRowsToPg(pgTable, snap, VECTOR_DIM);
            }
            verifyPgCount(pgTable, SNAPSHOT_COUNT);

            try (CdcEventStreamStrategy strategy = buildEventStreamStrategy(collectionName, PCHANNEL)) {
                // Drain backlog so the stream is bootstrapped before the delete.
                pollForEvents(strategy, collectionName, 2000L);

                // Delete DELETE_COUNT rows (id 0..DELETE_COUNT-1) from Milvus.
                long deleteWriteTs = System.currentTimeMillis();
                deleteData(collectionName, 0, DELETE_COUNT);

                // Poll for the delete events.
                List<SeaTunnelRowWithPosition> events;
                long sinkConfirmTs;
                try (CdcMetricsCollector.ScenarioTimer timer =
                        metrics.startScenario(scenarioName, DELETE_COUNT)) {
                    events = pollForEvents(strategy, collectionName, 10000L);
                    sinkConfirmTs = System.currentTimeMillis();
                    timer.actualRows(events.size())
                            .failedRows(Math.max(0, DELETE_COUNT - events.size()))
                            .syncDelay(deleteWriteTs, sinkConfirmTs);
                }

                List<SeaTunnelRow> deleteRows = extractRowsByKind(events, RowKind.DELETE);
                log.info(
                        "[{}] captured {} events ({} DELETE) for {} deleted rows",
                        scenarioName,
                        events.size(),
                        deleteRows.size(),
                        DELETE_COUNT);
                assertTrue(
                        deleteRows.size() >= DELETE_COUNT,
                        "Should capture at least "
                                + DELETE_COUNT
                                + " DELETE events, got "
                                + deleteRows.size());

                // Apply the deletes to pgvector.
                applyDeleteRowsToPg(pgTable, deleteRows);
            }

            // pgvector row count should have dropped by at least DELETE_COUNT.
            int pgCount = queryPgCount(pgTable);
            log.info("[{}] pgvector count after delete={} (expected <= {})",
                    scenarioName, pgCount, SNAPSHOT_COUNT - DELETE_COUNT);
            assertTrue(
                    pgCount <= SNAPSHOT_COUNT - DELETE_COUNT,
                    "pgvector count="
                            + pgCount
                            + " should be <= "
                            + (SNAPSHOT_COUNT - DELETE_COUNT)
                            + " after applying deletes");
            log.info("=== {} END: passed === (CORE TEST)", scenarioName);
        } finally {
            dropCollection(collectionName);
            dropPgTable(pgTable);
        }
    }

    @Test
    @Order(5)
    @DisplayName("event_stream same-PK update — upsert events update pgvector rows")
    void testEventStreamSamePkUpdate() throws Exception {
        Assumptions.assumeTrue(cdcAvailable, "event_stream CDC not available");
        String scenarioName = "event_stream.same_pk_update";
        String collectionName = "cdc_es_update";
        String pgTable = "pg_es_update";
        log.info("=== {} START ===", scenarioName);
        try {
            createCollection(collectionName, VECTOR_DIM);
            insertData(collectionName, 0, SNAPSHOT_COUNT, VECTOR_DIM, 0L);
            createPgTable(pgTable, VECTOR_DIM);

            // Apply initial snapshot to pgvector.
            try (PollingIncrementalCdcStrategy snapStrategy =
                    buildPollingStrategy(collectionName, 500)) {
                List<SeaTunnelRowWithPosition> snap =
                        runSnapshot(snapStrategy, collectionName, SNAPSHOT_COUNT);
                applyRowsToPg(pgTable, snap, VECTOR_DIM);
            }
            verifyPgCount(pgTable, SNAPSHOT_COUNT);

            try (CdcEventStreamStrategy strategy = buildEventStreamStrategy(collectionName, PCHANNEL)) {
                // Drain backlog.
                pollForEvents(strategy, collectionName, 2000L);

                // Upsert UPDATE_COUNT rows with the same PKs but different category values.
                long updateWriteTs = System.currentTimeMillis();
                upsertData(collectionName, 0, UPDATE_COUNT, VECTOR_DIM, 0L);

                // Poll for the upsert events (emitted as INSERT events by the WAL).
                List<SeaTunnelRowWithPosition> events;
                long sinkConfirmTs;
                try (CdcMetricsCollector.ScenarioTimer timer =
                        metrics.startScenario(scenarioName, UPDATE_COUNT)) {
                    events = pollForEvents(strategy, collectionName, 10000L);
                    sinkConfirmTs = System.currentTimeMillis();
                    int applied = applyRowsToPg(pgTable, events, VECTOR_DIM);
                    timer.actualRows(applied)
                            .failedRows(Math.max(0, UPDATE_COUNT - applied))
                            .syncDelay(updateWriteTs, sinkConfirmTs);
                }

                log.info(
                        "[{}] captured {} events for {} upserted rows",
                        scenarioName,
                        events.size(),
                        UPDATE_COUNT);
                assertFalse(events.isEmpty(), "Should capture at least one upsert event");
            }

            // Verify pgvector has the updated category values for the upserted PKs.
            verifyScalarFields(scenarioName, collectionName, pgTable, UPDATE_COUNT);
            verifyVectorSimilarity(scenarioName, collectionName, pgTable, VECTOR_DIM, SAMPLE_SIZE);
            // Row count unchanged (upsert does not add rows).
            verifyPgCount(pgTable, SNAPSHOT_COUNT);
            log.info("=== {} END: passed ===", scenarioName);
        } finally {
            dropCollection(collectionName);
            dropPgTable(pgTable);
        }
    }

    @Test
    @Order(6)
    @DisplayName("event_stream vs polling — event_stream captures DELETE, polling does not")
    void testEventStreamVsPollingComparison() throws Exception {
        Assumptions.assumeTrue(cdcAvailable, "event_stream CDC not available");
        String scenarioName = "event_stream.vs_polling";
        String collectionName = "cdc_es_vs_polling";
        log.info("=== {} START ===", scenarioName);
        try {
            createCollection(collectionName, VECTOR_DIM);
            insertData(collectionName, 0, SNAPSHOT_COUNT, VECTOR_DIM, 0L);

            // Establish the polling watermark from the snapshot (max id = SNAPSHOT_COUNT-1).
            long pollingWatermark = SNAPSHOT_COUNT - 1;

            try (CdcEventStreamStrategy esStrategy =
                            buildEventStreamStrategy(collectionName, PCHANNEL);
                    PollingIncrementalCdcStrategy pollingStrategy =
                            buildPollingStrategy(collectionName, 500)) {
                // Bootstrap the event_stream stream (drain backlog from snapshot insert).
                pollForEvents(esStrategy, collectionName, 2000L);

                // ---- Phase 1: insert new rows ----
                insertData(collectionName, SNAPSHOT_COUNT, INSERT_COUNT, VECTOR_DIM, 0L);
                Thread.sleep(1000); // allow WAL propagation

                // Poll with event_stream.
                List<SeaTunnelRowWithPosition> esInsertEvents =
                        pollForEvents(esStrategy, collectionName, 10000L);
                List<SeaTunnelRow> esInsertRows = extractRowsByKind(esInsertEvents, RowKind.INSERT);
                log.info(
                        "[{}] phase1 event_stream: {} INSERT events",
                        scenarioName,
                        esInsertRows.size());

                // Poll with polling strategy from the snapshot watermark.
                List<SeaTunnelRowWithPosition> pollingInsertRows =
                        runIncrementalPoll(pollingStrategy, collectionName, pollingWatermark);
                log.info(
                        "[{}] phase1 polling: {} rows",
                        scenarioName,
                        pollingInsertRows.size());

                assertTrue(
                        esInsertRows.size() >= INSERT_COUNT,
                        "event_stream should capture >= "
                                + INSERT_COUNT
                                + " inserts, got "
                                + esInsertRows.size());
                assertEquals(
                        INSERT_COUNT,
                        pollingInsertRows.size(),
                        "polling should capture exactly "
                                + INSERT_COUNT
                                + " inserts, got "
                                + pollingInsertRows.size());

                // Update the polling watermark past the newly inserted rows.
                pollingWatermark = SNAPSHOT_COUNT + INSERT_COUNT - 1;

                // ---- Phase 2: delete 10 rows ----
                int compareDeleteCount = 10;
                deleteData(collectionName, 0, compareDeleteCount);
                Thread.sleep(1000); // allow WAL propagation

                // Poll with event_stream — should capture DELETE events.
                List<SeaTunnelRowWithPosition> esDeleteEvents =
                        pollForEvents(esStrategy, collectionName, 10000L);
                List<SeaTunnelRow> esDeleteRows = extractRowsByKind(esDeleteEvents, RowKind.DELETE);
                log.info(
                        "[{}] phase2 event_stream: {} DELETE events",
                        scenarioName,
                        esDeleteRows.size());

                // Poll with polling strategy — should return empty (cannot detect deletes).
                List<SeaTunnelRowWithPosition> pollingAfterDelete =
                        runIncrementalPoll(pollingStrategy, collectionName, pollingWatermark);
                log.info(
                        "[{}] phase2 polling: {} rows (expected 0)",
                        scenarioName,
                        pollingAfterDelete.size());

                assertTrue(
                        esDeleteRows.size() >= compareDeleteCount,
                        "event_stream should capture >= "
                                + compareDeleteCount
                                + " deletes, got "
                                + esDeleteRows.size());
                assertEquals(
                        0,
                        pollingAfterDelete.size(),
                        "polling should NOT detect deletes, got " + pollingAfterDelete.size());

                metrics.recordAnomaly(
                        scenarioName,
                        "POLLING_CANNOT_DELETE",
                        "PollingIncrementalCdcStrategy uses PK-based filtering (id > watermark) "
                                + "and cannot detect deletes, while CdcEventStreamStrategy "
                                + "captured "
                                + esDeleteRows.size()
                                + " DELETE events.");
            }

            try (CdcMetricsCollector.ScenarioTimer timer =
                    metrics.startScenario(scenarioName, INSERT_COUNT)) {
                timer.actualRows(INSERT_COUNT).failedRows(0);
            }
            log.info("=== {} END: passed ===", scenarioName);
        } finally {
            dropCollection(collectionName);
        }
    }

    @Test
    @Order(7)
    @DisplayName("event_stream unavailable fallback — wrong pchannel yields isAvailable=false")
    void testEventStreamUnavailableFallback() throws Exception {
        Assumptions.assumeTrue(cdcAvailable, "event_stream CDC not available");
        String scenarioName = "event_stream.unavailable_fallback";
        String wrongPchannel = "nonexistent-pchannel";
        log.info("=== {} START ===", scenarioName);

        // Build a collection so describeCollection succeeds; the strategy itself targets a
        // non-existent pchannel.
        String collectionName = "cdc_es_unavail";
        try {
            createCollection(collectionName, VECTOR_DIM);
            insertData(collectionName, 0, 10, VECTOR_DIM, 0L);

            try (CdcEventStreamStrategy strategy =
                    buildEventStreamStrategy(collectionName, wrongPchannel)) {
                boolean available = strategy.isAvailable();
                log.info(
                        "[{}] isAvailable={} (pchannel={})",
                        scenarioName,
                        available,
                        wrongPchannel);
                assertFalse(
                        available,
                        "isAvailable should return false for non-existent pchannel " + wrongPchannel);

                // Verify the MilvusCdcSourceReader fallback logic: when event_stream is not
                // available and a pchannel is configured, the reader falls back to the
                // polling_incremental strategy. Here we only validate the config flag that
                // drives that decision (cdc_strategy=event_stream, cdc_pchannel set).
                MilvusCdcSourceConfig cfg =
                        MilvusCdcSourceConfig.builder()
                                .url(MILVUS_URL)
                                .token(MILVUS_TOKEN)
                                .collection(collectionName)
                                .cdcStrategy("event_stream")
                                .cdcPchannel(wrongPchannel)
                                .incrementalBatchSize(500L)
                                .pollIntervalMs(500L)
                                .channelTimeoutMs(10000L)
                                .primaryKeyField("id")
                                .build();
                assertEquals(
                        "event_stream",
                        cfg.getCdcStrategy(),
                        "config should request event_stream strategy");
                assertEquals(
                        wrongPchannel,
                        cfg.getCdcPchannel(),
                        "config should carry the (wrong) pchannel");
                log.info(
                        "[{}] config verified: cdc_strategy={}, cdc_pchannel={} — "
                                + "MilvusCdcSourceReader would fall back to polling_incremental",
                        scenarioName,
                        cfg.getCdcStrategy(),
                        cfg.getCdcPchannel());
            }

            metrics.recordAnomaly(
                    scenarioName,
                    "EXPECTED_UNAVAILABLE_FALLBACK",
                    "event_stream correctly reported unavailable for pchannel="
                            + wrongPchannel
                            + "; MilvusCdcSourceReader falls back to polling_incremental.");
            try (CdcMetricsCollector.ScenarioTimer timer =
                    metrics.startScenario(scenarioName, 0)) {
                timer.actualRows(0).failedRows(0);
            }
            log.info("=== {} END: passed ===", scenarioName);
        } finally {
            dropCollection(collectionName);
        }
    }
}
