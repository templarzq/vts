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
import org.apache.seatunnel.connectors.migration.milvus2pgvector.cdc.streaming.CdcEventStreamStrategyV2;

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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * E2E tests for {@link CdcEventStreamStrategyV2} — the StreamingNode gRPC based event_stream
 * strategy that works in standalone Milvus mode (no replication topology required).
 *
 * <p>Test scenarios:
 *
 * <ol>
 *   <li><b>Availability</b> — verifies {@code isAvailable()} against real pchannel via
 *       StreamingNode gRPC.
 *   <li><b>Full snapshot sync</b> — DeliverPolicy.all reads all historical WAL messages.
 *   <li><b>Incremental insert</b> — new rows inserted after snapshot are captured.
 *   <li><b>Delete capture</b> (core) — {@code RowKind.DELETE} events captured from WAL.
 *   <li><b>Checkpoint recovery</b> — DeliverPolicy.startAfter resumes from checkpoint.
 * </ol>
 *
 * <p><b>Environment:</b> Milvus 2.6.9 standalone ({@code http://localhost:19530}),
 * pchannel = {@code by-dev-rootcoord-dml_0}; pgvector on {@code localhost:5432}.
 *
 * <p><b>Run with:</b>
 * <pre>
 * mvn test -pl connector-milvus-pgvector-migration \
 *     -Dtest=MilvusCdcStreamingNodeE2E \
 *     -Dmigration.cdc.e2e.enabled=true -Dmaven.test.skip=false -DskipUT=false
 * </pre>
 */
@EnabledIfSystemProperty(
        named = "migration.cdc.e2e.enabled",
        matches = "true")
@DisplayName("Milvus CDC StreamingNode Strategy V2 E2E")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Slf4j
public class MilvusCdcStreamingNodeE2E extends MilvusCdcE2ETestBase {

    private static final int VECTOR_DIM = 64;
    private static final int SNAPSHOT_COUNT = 100;
    private static final int INSERT_COUNT = 50;
    private static final int DELETE_COUNT = 30;
    private static final String PCHANNEL = "by-dev-rootcoord-dml_0";
    private static final String STREAMING_NODE_ADDR = "localhost:22222";

    /** Set by availability test; subsequent tests assume CDC is available. */
    private static boolean cdcAvailable = false;

    @Override
    protected String scenarioLabel() {
        return "streaming_node";
    }

    // ==================================================================
    // Helpers
    // ==================================================================

    /**
     * Build a CdcEventStreamStrategyV2 for the given collection.
     * Uses StreamingNode gRPC with DeliverPolicy for WAL access.
     */
    private CdcEventStreamStrategyV2 buildStreamingNodeStrategy(String collectionName) {
        DescribeCollectionResp desc = milvusClient.describeCollection(
                DescribeCollectionReq.builder()
                        .collectionName(collectionName)
                        .build());

        long collectionId = desc.getCollectionID();
        String actualPchannel = findPchannelForCollection(collectionId);
        log.info("[buildStreamingNodeStrategy] collection={}, collectionId={}, pchannel={}",
                collectionName, collectionId, actualPchannel);

        MilvusCdcSourceConfig config = MilvusCdcSourceConfig.builder()
                .url(MILVUS_URL)
                .token(MILVUS_TOKEN)
                .collection(collectionName)
                .cdcStrategy("event_stream")
                .cdcUseStreamingNode(true)
                .cdcPchannel(actualPchannel != null ? actualPchannel : PCHANNEL)
                .streamingNodeAddress(STREAMING_NODE_ADDR)
                .incrementalBatchSize(500L)
                .pollIntervalMs(500L)
                .channelTimeoutMs(60000L)
                .primaryKeyField("id")
                .build();

        return new CdcEventStreamStrategyV2(config, desc);
    }

    /**
     * Find the pchannel for a collection by querying etcd.
     * Searches for datacoord channel-cp keys matching the collection ID.
     */
    private String findPchannelForCollection(long collectionId) {
        try {
            ProcessBuilder pb = new ProcessBuilder(
                    "docker", "exec", "milvus-etcd",
                    "etcdctl", "--endpoints=http://127.0.0.1:2379",
                    "get", "--prefix", "", "--keys-only");
            pb.redirectError(ProcessBuilder.Redirect.INHERIT);
            Process process = pb.start();
            java.io.BufferedReader reader = new java.io.BufferedReader(
                    new java.io.InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.contains("datacoord-meta/channel-cp/") && line.contains(String.valueOf(collectionId))) {
                    String[] parts = line.split("/");
                    String vchannel = parts[parts.length - 1];
                    String[] vchannelParts = vchannel.split("_");
                    if (vchannelParts.length >= 2) {
                        return vchannelParts[0] + "_" + vchannelParts[1];
                    }
                }
            }
            process.waitFor();
        } catch (Exception e) {
            log.warn("Failed to find pchannel for collectionId={}: {}", collectionId, e.getMessage());
        }
        return null;
    }

    /**
     * Apply DELETE events to pgvector.
     */
    private void applyDeleteRowsToPg(String tableName, List<SeaTunnelRow> deleteRows)
            throws SQLException {
        String sql = "DELETE FROM " + PG_SCHEMA + "." + tableName + " WHERE id = ?";
        try (PreparedStatement stmt = pgConnection.prepareStatement(sql)) {
            for (SeaTunnelRow row : deleteRows) {
                stmt.setObject(1, row.getField(0));
                stmt.addBatch();
            }
            stmt.executeBatch();
        }
    }

    /**
     * Poll the V2 strategy repeatedly until timeoutMs, aggregating all captured events.
     * Uses startPosition for the first poll (null = DeliverPolicy.all, non-null = startAfter).
     * Stops early after {@code maxEmptyPolls} consecutive empty polls to avoid waiting
     * for the full timeout when all data has been consumed.
     */
    private List<SeaTunnelRowWithPosition> pollForEvents(
            CdcEventStreamStrategyV2 strategy, String collectionName,
            long timeoutMs, ReplicatePosition startPosition)
            throws Exception {
        return pollForEvents(strategy, collectionName, timeoutMs, startPosition, 3);
    }

    /**
     * Poll the V2 strategy repeatedly with explicit maxEmptyPolls for early exit.
     */
    private List<SeaTunnelRowWithPosition> pollForEvents(
            CdcEventStreamStrategyV2 strategy, String collectionName,
            long timeoutMs, ReplicatePosition startPosition, int maxEmptyPolls)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        List<SeaTunnelRowWithPosition> allEvents = new ArrayList<>();
        boolean firstPoll = true;
        int consecutiveEmpty = 0;
        while (System.currentTimeMillis() < deadline) {
            MilvusCdcSourceSplit split = MilvusCdcSourceSplit.builder()
                    .splitId("incremental-0")
                    .collectionName(collectionName)
                    .snapshot(false)
                    .build();
            // Only pass startPosition on first poll; subsequent polls use null (continue stream)
            List<SeaTunnelRowWithPosition> batch = strategy.pollChanges(split,
                    firstPoll ? startPosition : null);
            firstPoll = false;
            allEvents.addAll(batch);
            if (batch.isEmpty()) {
                consecutiveEmpty++;
                if (consecutiveEmpty >= maxEmptyPolls) {
                    log.info("[pollForEvents] Stopping early after {} consecutive empty polls "
                            + "(collected {} events)", consecutiveEmpty, allEvents.size());
                    break;
                }
                Thread.sleep(200);
            } else {
                consecutiveEmpty = 0;
            }
        }
        return allEvents;
    }

    /** Extract rows by RowKind. */
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
    @DisplayName("streaming_node availability — isAvailable via StreamingNode gRPC")
    void testStreamingNodeAvailability() throws Exception {
        String scenarioName = "streaming_node.availability";
        String collectionName = "cdc_sn_avail";
        log.info("=== {} START ===", scenarioName);
        try {
            createCollection(collectionName, VECTOR_DIM);
            insertData(collectionName, 0, 10, VECTOR_DIM, 0L);

            try (CdcEventStreamStrategyV2 strategy = buildStreamingNodeStrategy(collectionName)) {
                cdcAvailable = strategy.isAvailable();
                log.info("[{}] isAvailable={} (pchannel={})", scenarioName, cdcAvailable, PCHANNEL);

                if (!cdcAvailable) {
                    metrics.recordAnomaly(
                            scenarioName,
                            "STREAMING_NODE_UNAVAILABLE",
                            "StreamingNode gRPC not reachable for pchannel=" + PCHANNEL);
                    log.warn("[{}] StreamingNode not available; remaining tests will be skipped",
                            scenarioName);
                }
            }

            try (CdcMetricsCollector.ScenarioTimer timer =
                    metrics.startScenario(scenarioName, 0)) {
                timer.actualRows(0).failedRows(0);
            }
            log.info("=== {} END (available={}) ===", scenarioName, cdcAvailable);
        } finally {
            dropCollection(collectionName);
        }
    }

    @Test
    @Order(2)
    @DisplayName("streaming_node full snapshot — DeliverPolicy.all reads all historical data")
    void testFullSnapshot() throws Exception {
        Assumptions.assumeTrue(cdcAvailable, "StreamingNode CDC not available, skipping");
        String scenarioName = "streaming_node.full_snapshot";
        String collectionName = "cdc_sn_snapshot";
        String pgTable = "cdc_sn_snapshot";
        log.info("=== {} START ===", scenarioName);
        try {
            // Setup: insert SNAPSHOT_COUNT rows
            createCollection(collectionName, VECTOR_DIM);
            insertData(collectionName, 0, SNAPSHOT_COUNT, VECTOR_DIM, 42L);
            createPgTable(pgTable, VECTOR_DIM);

            // Use V2 strategy with DeliverPolicy.all (null startPosition) to read all WAL data
            try (CdcEventStreamStrategyV2 strategy = buildStreamingNodeStrategy(collectionName);
                    CdcMetricsCollector.ScenarioTimer timer =
                            metrics.startScenario(scenarioName, SNAPSHOT_COUNT)) {

                List<SeaTunnelRowWithPosition> snapshotRows =
                        pollForEvents(strategy, collectionName, 30000, null);
                List<SeaTunnelRow> insertRows = extractRowsByKind(snapshotRows, RowKind.INSERT);
                log.info("[{}] Snapshot captured {} rows (INSERT events: {})",
                        scenarioName, snapshotRows.size(), insertRows.size());

                int applied = applyRowsToPg(pgTable, snapshotRows, VECTOR_DIM);
                timer.actualRows(applied);

                verifyPgCount(pgTable, SNAPSHOT_COUNT);
                verifyVectorSimilarity(scenarioName, collectionName, pgTable, VECTOR_DIM, SAMPLE_SIZE);
                verifyScalarFields(scenarioName, collectionName, pgTable, SAMPLE_SIZE);

                assertTrue(applied >= SNAPSHOT_COUNT,
                        "Snapshot row count mismatch: applied=" + applied
                                + " < expected=" + SNAPSHOT_COUNT);
                log.info("[{}] Snapshot verified: {} rows synced to pgvector", scenarioName, applied);
            }
            log.info("=== {} END ===", scenarioName);
        } finally {
            dropCollection(collectionName);
            dropPgTable(pgTable);
        }
    }

    @Test
    @Order(3)
    @DisplayName("streaming_node incremental insert — new rows captured via WAL stream")
    void testIncrementalInsert() throws Exception {
        Assumptions.assumeTrue(cdcAvailable, "StreamingNode CDC not available, skipping");
        String scenarioName = "streaming_node.incremental_insert";
        String collectionName = "cdc_sn_insert";
        String pgTable = "cdc_sn_insert";
        log.info("=== {} START ===", scenarioName);
        try {
            // Setup: initial data
            createCollection(collectionName, VECTOR_DIM);
            insertData(collectionName, 0, SNAPSHOT_COUNT, VECTOR_DIM, 100L);
            createPgTable(pgTable, VECTOR_DIM);

            // Phase 1: Snapshot via V2 strategy (DeliverPolicy.all) — also obtains checkpoint
            ReplicatePosition checkpoint;
            try (CdcEventStreamStrategyV2 strategy = buildStreamingNodeStrategy(collectionName)) {
                List<SeaTunnelRowWithPosition> snapshotRows =
                        pollForEvents(strategy, collectionName, 30000, null);
                assertFalse(snapshotRows.isEmpty(), "Snapshot should have events");
                checkpoint = snapshotRows.get(snapshotRows.size() - 1).getPosition();
                int snapshotApplied = applyRowsToPg(pgTable, snapshotRows, VECTOR_DIM);
                verifyPgCount(pgTable, SNAPSHOT_COUNT);
                log.info("[{}] Snapshot phase: {} rows applied, checkpoint messageId={}",
                        scenarioName, snapshotApplied,
                        checkpoint != null ? checkpoint.getMessageId() : "null");
            }

            // Insert INSERT_COUNT new rows (incremental)
            insertData(collectionName, SNAPSHOT_COUNT, INSERT_COUNT, VECTOR_DIM, 200L);

            // Phase 2: Incremental via V2 strategy (DeliverPolicy.startAfter checkpoint)
            try (CdcEventStreamStrategyV2 strategy = buildStreamingNodeStrategy(collectionName);
                    CdcMetricsCollector.ScenarioTimer timer =
                            metrics.startScenario(scenarioName, INSERT_COUNT)) {

                List<SeaTunnelRowWithPosition> events =
                        pollForEvents(strategy, collectionName, 30000, checkpoint);

                List<SeaTunnelRow> insertRows = extractRowsByKind(events, RowKind.INSERT);
                log.info("[{}] Captured {} INSERT events (expected {})",
                        scenarioName, insertRows.size(), INSERT_COUNT);

                int applied = applyRowsToPg(pgTable, events, VECTOR_DIM);
                timer.actualRows(applied);

                // Verify: pgvector should have snapshot + incremental rows
                verifyPgCount(pgTable, SNAPSHOT_COUNT + INSERT_COUNT);
                log.info("[{}] Incremental insert verified: {} rows applied, total pgvector count >= {}",
                        scenarioName, applied, SNAPSHOT_COUNT + INSERT_COUNT);
            }
            log.info("=== {} END ===", scenarioName);
        } finally {
            dropCollection(collectionName);
            dropPgTable(pgTable);
        }
    }

    @Test
    @Order(4)
    @DisplayName("streaming_node delete capture — RowKind.DELETE events from WAL (core)")
    void testDeleteCapture() throws Exception {
        Assumptions.assumeTrue(cdcAvailable, "StreamingNode CDC not available, skipping");
        String scenarioName = "streaming_node.delete_capture";
        String collectionName = "cdc_sn_delete";
        String pgTable = "cdc_sn_delete";
        log.info("=== {} START ===", scenarioName);
        try {
            // Setup: initial data
            createCollection(collectionName, VECTOR_DIM);
            insertData(collectionName, 0, SNAPSHOT_COUNT, VECTOR_DIM, 300L);
            createPgTable(pgTable, VECTOR_DIM);

            // Phase 1: Snapshot via V2 strategy (DeliverPolicy.all) — also obtains checkpoint
            ReplicatePosition checkpoint;
            try (CdcEventStreamStrategyV2 strategy = buildStreamingNodeStrategy(collectionName)) {
                List<SeaTunnelRowWithPosition> snapshotRows =
                        pollForEvents(strategy, collectionName, 30000, null);
                assertFalse(snapshotRows.isEmpty(), "Snapshot should have events");
                checkpoint = snapshotRows.get(snapshotRows.size() - 1).getPosition();
                applyRowsToPg(pgTable, snapshotRows, VECTOR_DIM);
                verifyPgCount(pgTable, SNAPSHOT_COUNT);
                log.info("[{}] Snapshot phase complete, checkpoint messageId={}",
                        scenarioName, checkpoint != null ? checkpoint.getMessageId() : "null");
            }

            int pgCountBefore = queryPgCount(pgTable);
            log.info("[{}] pgvector count before delete: {}", scenarioName, pgCountBefore);

            // Delete DELETE_COUNT rows from Milvus
            deleteData(collectionName, 0, DELETE_COUNT);
            log.info("[{}] Deleted {} rows from Milvus", scenarioName, DELETE_COUNT);

            // Phase 2: Incremental via V2 strategy (DeliverPolicy.startAfter checkpoint)
            try (CdcEventStreamStrategyV2 strategy = buildStreamingNodeStrategy(collectionName);
                    CdcMetricsCollector.ScenarioTimer timer =
                            metrics.startScenario(scenarioName, DELETE_COUNT)) {

                List<SeaTunnelRowWithPosition> events =
                        pollForEvents(strategy, collectionName, 30000, checkpoint);

                List<SeaTunnelRow> deleteRows = extractRowsByKind(events, RowKind.DELETE);
                log.info("[{}] Captured {} DELETE events (expected {})",
                        scenarioName, deleteRows.size(), DELETE_COUNT);

                // Apply deletes to pgvector
                applyDeleteRowsToPg(pgTable, deleteRows);
                timer.actualRows(deleteRows.size());

                // Verify: pgvector count should decrease
                int pgCountAfter = queryPgCount(pgTable);
                log.info("[{}] pgvector count after delete: {}", scenarioName, pgCountAfter);
                assertTrue(pgCountAfter < pgCountBefore,
                        "pgvector count should decrease after deletes: before="
                                + pgCountBefore + ", after=" + pgCountAfter);

                log.info("[{}] Delete capture verified: {} DELETE events applied",
                        scenarioName, deleteRows.size());
            }
            log.info("=== {} END ===", scenarioName);
        } finally {
            dropCollection(collectionName);
            dropPgTable(pgTable);
        }
    }

    @Test
    @Order(5)
    @DisplayName("streaming_node checkpoint recovery — DeliverPolicy.startAfter")
    void testCheckpointRecovery() throws Exception {
        Assumptions.assumeTrue(cdcAvailable, "StreamingNode CDC not available, skipping");
        String scenarioName = "streaming_node.checkpoint_recovery";
        String collectionName = "cdc_sn_checkpoint";
        String pgTable = "cdc_sn_checkpoint";
        log.info("=== {} START ===", scenarioName);
        try {
            createCollection(collectionName, VECTOR_DIM);
            insertData(collectionName, 0, 50, VECTOR_DIM, 500L);
            createPgTable(pgTable, VECTOR_DIM);

            // First poll: get initial events and a checkpoint
            ReplicatePosition checkpoint;
            try (CdcEventStreamStrategyV2 strategy = buildStreamingNodeStrategy(collectionName)) {
                List<SeaTunnelRowWithPosition> firstBatch =
                        pollForEvents(strategy, collectionName, 10000, null);
                assertFalse(firstBatch.isEmpty(), "First batch should have events");
                checkpoint = firstBatch.get(firstBatch.size() - 1).getPosition();
                log.info("[{}] First batch: {} events, checkpoint messageId={}",
                        scenarioName, firstBatch.size(),
                        checkpoint != null ? checkpoint.getMessageId() : "null");
            }

            // Insert more data after checkpoint
            insertData(collectionName, 50, 30, VECTOR_DIM, 600L);

            // Second poll: resume from checkpoint
            try (CdcEventStreamStrategyV2 strategy = buildStreamingNodeStrategy(collectionName);
                    CdcMetricsCollector.ScenarioTimer timer =
                            metrics.startScenario(scenarioName, 30)) {
                List<SeaTunnelRowWithPosition> secondBatch =
                        pollForEvents(strategy, collectionName, 15000, checkpoint);
                log.info("[{}] Second batch (after checkpoint): {} events",
                        scenarioName, secondBatch.size());

                assertFalse(secondBatch.isEmpty(),
                        "Second batch should have events after checkpoint");
                timer.actualRows(secondBatch.size());

                log.info("[{}] Checkpoint recovery verified: resumed from messageId={}",
                        scenarioName, checkpoint.getMessageId());
            }
            log.info("=== {} END ===", scenarioName);
        } finally {
            dropCollection(collectionName);
            dropPgTable(pgTable);
        }
    }

    @Test
    @Order(6)
    @DisplayName("streaming_node unavailable fallback — wrong pchannel yields isAvailable=false")
    void testUnavailableFallback() throws Exception {
        String scenarioName = "streaming_node.unavailable_fallback";
        String collectionName = "cdc_sn_fallback";
        log.info("=== {} START ===", scenarioName);
        try {
            createCollection(collectionName, VECTOR_DIM);
            insertData(collectionName, 0, 5, VECTOR_DIM, 700L);

            DescribeCollectionResp desc = milvusClient.describeCollection(
                    DescribeCollectionReq.builder().collectionName(collectionName).build());

            // Use an invalid pchannel name — isAvailable() should return false because
            // the Consume stream probe will fail with FAILED_PRECONDITION for all terms.
            MilvusCdcSourceConfig config = MilvusCdcSourceConfig.builder()
                    .url(MILVUS_URL)
                    .token(MILVUS_TOKEN)
                    .collection(collectionName)
                    .cdcStrategy("event_stream")
                    .cdcUseStreamingNode(true)
                    .cdcPchannel("invalid-pchannel-name")
                    .streamingNodeAddress(STREAMING_NODE_ADDR)
                    .incrementalBatchSize(500L)
                    .channelTimeoutMs(5000L)
                    .primaryKeyField("id")
                    .build();

            try (CdcEventStreamStrategyV2 strategy = new CdcEventStreamStrategyV2(config, desc)) {
                boolean available = strategy.isAvailable();
                assertFalse(available, "isAvailable should be false for invalid pchannel");
                log.info("[{}] Invalid pchannel correctly returned isAvailable=false", scenarioName);
            }

            try (CdcMetricsCollector.ScenarioTimer timer =
                    metrics.startScenario(scenarioName, 0)) {
                timer.actualRows(0).failedRows(0);
            }
            log.info("=== {} END ===", scenarioName);
        } finally {
            dropCollection(collectionName);
        }
    }
}