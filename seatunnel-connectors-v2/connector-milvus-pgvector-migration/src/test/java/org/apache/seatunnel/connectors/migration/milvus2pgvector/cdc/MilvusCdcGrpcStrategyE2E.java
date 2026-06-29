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

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.cdc.metrics.CdcMetricsCollector;

import lombok.extern.slf4j.Slf4j;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * E2E tests for the {@link GrpcReplicateCdcStrategy}. Validates gRPC CDC availability
 * detection, full snapshot sync, incremental insert capture, cross-strategy consistency
 * (gRPC vs polling), and the documented delete-detection limitation.
 *
 * <p>Enabled only when {@code -Dmigration.cdc.e2e.enabled=true} is set.
 */
@org.junit.jupiter.api.condition.EnabledIfSystemProperty(
        named = "migration.cdc.e2e.enabled",
        matches = "true")
@DisplayName("Milvus CDC GrpcReplicateCdcStrategy E2E")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Slf4j
public class MilvusCdcGrpcStrategyE2E extends MilvusCdcE2ETestBase {

    @Override
    protected String scenarioLabel() {
        return "grpc";
    }

    private static final int VECTOR_DIM = 64;
    private static final int SNAPSHOT_COUNT = 200;
    private static final int INSERT_COUNT = 100;
    private static final int UPDATE_COUNT = 50;
    private static final int DELETE_COUNT = 30;

    @Test
    @Order(1)
    @DisplayName("gRPC CDC 可用性检测 — isAvailable 返回布尔值且不影响 pollChanges")
    void testGrpcCdcAvailability() throws Exception {
        String scenarioName = "grpc.availability";
        String collectionName = "cdc_grpc_avail";
        log.info("=== {} START ===", scenarioName);
        try {
            createCollection(collectionName, VECTOR_DIM);
            insertData(collectionName, 0, 50, VECTOR_DIM, 0L);

            try (GrpcReplicateCdcStrategy strategy = buildGrpcStrategy(collectionName, 100)) {
                boolean available = strategy.isAvailable();
                log.info("[{}] isAvailable={} (GetReplicateInfo may return false on "
                        + "standalone Milvus without CDC configured)", scenarioName, available);

                // isAvailable() checks GetReplicateInfo which is a CDC-specific API.
                // On standard standalone Milvus without CDC enabled, it may return false.
                // This is expected — the strategy still works via PK-based polling.
                // We only verify the call doesn't throw and returns a boolean.

                // Verify pollChanges still works regardless of isAvailable() result
                List<SeaTunnelRowWithPosition> snap =
                        runCdcSnapshot(strategy, collectionName, 50);
                assertEquals(50, snap.size(), "pollChanges should work even if isAvailable=false");

                if (!available) {
                    metrics.recordAnomaly(
                            scenarioName,
                            "CDC_METADATA_UNAVAILABLE",
                            "GetReplicateInfo returned false — standard standalone Milvus "
                                    + "may not have CDC configured. Strategy falls back to "
                                    + "PK-based polling which still works correctly.");
                }

                try (CdcMetricsCollector.ScenarioTimer timer =
                        metrics.startScenario(scenarioName, 50)) {
                    timer.actualRows(snap.size()).failedRows(0);
                }
            }
            log.info("=== {} END: passed ===", scenarioName);
        } finally {
            dropCollection(collectionName);
        }
    }

    @Test
    @Order(2)
    @DisplayName("gRPC 策略全量快照同步 — 验证行数和向量相似度")
    void testGrpcFullSnapshot() throws Exception {
        String scenarioName = "grpc.full_snapshot";
        String collectionName = "cdc_grpc_snap";
        String pgTable = "pg_grpc_snap";
        log.info("=== {} START ===", scenarioName);
        long sourceWriteTs = System.currentTimeMillis();
        try {
            createCollection(collectionName, VECTOR_DIM);
            insertData(collectionName, 0, SNAPSHOT_COUNT, VECTOR_DIM, 0L);
            createPgTable(pgTable, VECTOR_DIM);

            try (GrpcReplicateCdcStrategy strategy = buildGrpcStrategy(collectionName, 500)) {
                // isAvailable() may return false on standalone Milvus without CDC configured,
                // but pollChanges() still works via PK-based polling.
                log.info("[{}] isAvailable={}", scenarioName, strategy.isAvailable());

                List<SeaTunnelRowWithPosition> snap =
                        runCdcSnapshot(strategy, collectionName, SNAPSHOT_COUNT);
                log.info("[{}] snapshot polled {} rows", scenarioName, snap.size());
                assertEquals(SNAPSHOT_COUNT, snap.size(), "Snapshot should read all rows");

                int applied = applyRowsToPg(pgTable, snap, VECTOR_DIM);
                assertEquals(SNAPSHOT_COUNT, applied, "All rows should be applied to pgvector");
            }

            long sinkConfirmTs = System.currentTimeMillis();
            verifyPgCount(pgTable, SNAPSHOT_COUNT);
            verifyVectorSimilarity(scenarioName, collectionName, pgTable, VECTOR_DIM, 50);
            verifyScalarFields(scenarioName, collectionName, pgTable, 50);

            try (CdcMetricsCollector.ScenarioTimer timer =
                    metrics.startScenario(scenarioName, SNAPSHOT_COUNT)) {
                timer.actualRows(SNAPSHOT_COUNT)
                        .failedRows(0)
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
    @DisplayName("gRPC 策略增量插入捕获 — 水位线轮询应捕获新 PK 行")
    void testGrpcIncrementalInsert() throws Exception {
        String scenarioName = "grpc.incremental_insert";
        String collectionName = "cdc_grpc_inc";
        String pgTable = "pg_grpc_inc";
        log.info("=== {} START ===", scenarioName);
        try {
            createCollection(collectionName, VECTOR_DIM);
            insertData(collectionName, 0, SNAPSHOT_COUNT, VECTOR_DIM, 0L);
            createPgTable(pgTable, VECTOR_DIM);

            long watermark = SNAPSHOT_COUNT - 1;

            try (GrpcReplicateCdcStrategy strategy = buildGrpcStrategy(collectionName, 100)) {
                log.info("[{}] isAvailable={}", scenarioName, strategy.isAvailable());

                // Snapshot phase
                List<SeaTunnelRowWithPosition> snap =
                        runCdcSnapshot(strategy, collectionName, SNAPSHOT_COUNT);
                applyRowsToPg(pgTable, snap, VECTOR_DIM);

                // Insert new rows
                insertData(collectionName, SNAPSHOT_COUNT, INSERT_COUNT, VECTOR_DIM, 0L);

                // Incremental poll
                List<SeaTunnelRowWithPosition> inc =
                        runCdcIncrementalPoll(strategy, collectionName, watermark);
                log.info(
                        "[{}] incremental poll captured {} rows (expected {})",
                        scenarioName,
                        inc.size(),
                        INSERT_COUNT);
                assertEquals(INSERT_COUNT, inc.size(), "Should capture all newly inserted rows");

                applyRowsToPg(pgTable, inc, VECTOR_DIM);
            }

            verifyPgCount(pgTable, SNAPSHOT_COUNT + INSERT_COUNT);
            verifyVectorSimilarity(scenarioName, collectionName, pgTable, VECTOR_DIM, 50);

            try (CdcMetricsCollector.ScenarioTimer timer =
                        metrics.startScenario(scenarioName, INSERT_COUNT)) {
                timer.actualRows(INSERT_COUNT).failedRows(0);
            }
            log.info("=== {} END: passed ===", scenarioName);
        } finally {
            dropCollection(collectionName);
            dropPgTable(pgTable);
        }
    }

    @Test
    @Order(4)
    @DisplayName("gRPC vs Polling 策略一致性对比 — 两种策略应返回相同数据")
    void testGrpcVsPollingConsistency() throws Exception {
        String scenarioName = "grpc.vs_polling";
        String collectionName = "cdc_grpc_vs_polling";
        log.info("=== {} START ===", scenarioName);
        try {
            createCollection(collectionName, VECTOR_DIM);
            insertData(collectionName, 0, SNAPSHOT_COUNT, VECTOR_DIM, 0L);

            // Read with polling strategy
            PollingIncrementalCdcStrategy pollingStrategy =
                    buildPollingStrategy(collectionName, 500);
            List<SeaTunnelRowWithPosition> pollingRows =
                    runSnapshot(pollingStrategy, collectionName, SNAPSHOT_COUNT);
            pollingStrategy.close();

            // Read with gRPC strategy
            List<SeaTunnelRowWithPosition> grpcRows;
            try (GrpcReplicateCdcStrategy grpcStrategy = buildGrpcStrategy(collectionName, 500)) {
                grpcRows = runCdcSnapshot(grpcStrategy, collectionName, SNAPSHOT_COUNT);
            }

            log.info(
                    "[{}] polling={} rows, grpc={} rows",
                    scenarioName,
                    pollingRows.size(),
                    grpcRows.size());
            assertEquals(
                    pollingRows.size(),
                    grpcRows.size(),
                    "Both strategies should return same number of rows");

            // Compare IDs
            long pollingMaxId = extractMaxId(pollingRows);
            long grpcMaxId = extractMaxId(grpcRows);
            assertEquals(pollingMaxId, grpcMaxId, "Both strategies should see same max ID");

            // Compare a sample of vectors
            int sampleSize = Math.min(20, pollingRows.size());
            double minSim = 1.0;
            for (int i = 0; i < sampleSize; i++) {
                SeaTunnelRow pRow = pollingRows.get(i).getRow();
                SeaTunnelRow gRow = grpcRows.get(i).getRow();
                Object pId = pRow.getFields()[0];
                Object gId = gRow.getFields()[0];
                assertEquals(toLong(pId), toLong(gId), "IDs should match at index " + i);

                float[] pVec = extractMilvusVector(pRow.getFields()[1], VECTOR_DIM);
                float[] gVec = extractMilvusVector(gRow.getFields()[1], VECTOR_DIM);
                double sim = cosineSimilarity(pVec, gVec);
                minSim = Math.min(minSim, sim);
                assertTrue(sim >= 0.9999, "Vector similarity should be >= 0.9999, got " + sim);
            }

            log.info("[{}] min vector similarity = {}", scenarioName, minSim);

            try (CdcMetricsCollector.ScenarioTimer timer =
                        metrics.startScenario(scenarioName, SNAPSHOT_COUNT)) {
                timer.actualRows(grpcRows.size()).failedRows(0);
            }
            log.info("=== {} END: passed ===", scenarioName);
        } finally {
            dropCollection(collectionName);
        }
    }

    @Test
    @Order(5)
    @DisplayName("gRPC 策略删除检测限制 — PK 轮询无法捕获删除（与 polling 一致）")
    void testGrpcDeleteLimitation() throws Exception {
        String scenarioName = "grpc.delete_limitation";
        String collectionName = "cdc_grpc_delete";
        String pgTable = "pg_grpc_delete";
        log.info("=== {} START ===", scenarioName);
        try {
            createCollection(collectionName, VECTOR_DIM);
            insertData(collectionName, 0, SNAPSHOT_COUNT, VECTOR_DIM, 0L);
            createPgTable(pgTable, VECTOR_DIM);

            long watermark;
            try (GrpcReplicateCdcStrategy strategy = buildGrpcStrategy(collectionName, 500)) {
                // Snapshot
                List<SeaTunnelRowWithPosition> snap =
                        runCdcSnapshot(strategy, collectionName, SNAPSHOT_COUNT);
                applyRowsToPg(pgTable, snap, VECTOR_DIM);
                watermark = extractMaxId(snap);

                // Delete some rows
                deleteData(collectionName, SNAPSHOT_COUNT - DELETE_COUNT, DELETE_COUNT);
                log.info(
                        "[{}] deleted {} rows (id {}..{})",
                        scenarioName,
                        DELETE_COUNT,
                        SNAPSHOT_COUNT - DELETE_COUNT,
                        SNAPSHOT_COUNT - 1);

                // Incremental poll — should detect 0 new rows
                List<SeaTunnelRowWithPosition> inc =
                        runCdcIncrementalPoll(strategy, collectionName, watermark);
                log.info(
                        "[{}] incremental poll after delete: {} rows (expected 0)",
                        scenarioName,
                        inc.size());
                assertEquals(0, inc.size(), "gRPC strategy (PK polling) cannot detect deletes");
            }

            // pgvector still has all rows — delete not propagated
            verifyPgCount(pgTable, SNAPSHOT_COUNT);

            metrics.recordAnomaly(
                    scenarioName,
                    "DELETE_NOT_DETECTED_GRPC",
                    "GrpcReplicateCdcStrategy uses PK-based polling (same as "
                            + "PollingIncrementalCdcStrategy) and cannot capture deletes. "
                            + DELETE_COUNT
                            + " rows deleted from Milvus but pgvector unchanged.");
            try (CdcMetricsCollector.ScenarioTimer timer =
                        metrics.startScenario(scenarioName, 0)) {
                timer.actualRows(0).failedRows(0);
            }
            log.info("=== {} END ===", scenarioName);
        } finally {
            dropCollection(collectionName);
            dropPgTable(pgTable);
        }
    }

    @Test
    @Order(6)
    @DisplayName("gRPC 策略同 PK 更新限制 — PK 轮询无法检测同 PK 更新")
    void testGrpcUpdateLimitation() throws Exception {
        String scenarioName = "grpc.update_limitation";
        String collectionName = "cdc_grpc_update";
        log.info("=== {} START ===", scenarioName);
        try {
            createCollection(collectionName, VECTOR_DIM);
            insertData(collectionName, 0, SNAPSHOT_COUNT, VECTOR_DIM, 0L);

            long watermark = SNAPSHOT_COUNT - 1;

            try (GrpcReplicateCdcStrategy strategy = buildGrpcStrategy(collectionName, 500)) {
                // Snapshot
                runCdcSnapshot(strategy, collectionName, SNAPSHOT_COUNT);

                // Upsert (same PK update) on first UPDATE_COUNT rows
                upsertData(collectionName, 0, UPDATE_COUNT, VECTOR_DIM, 0L);
                log.info("[{}] upserted {} rows (id 0..{})", scenarioName, UPDATE_COUNT, UPDATE_COUNT - 1);

                // Incremental poll — PK polling cannot detect same-PK updates
                List<SeaTunnelRowWithPosition> inc =
                        runCdcIncrementalPoll(strategy, collectionName, watermark);
                log.info(
                        "[{}] incremental poll after upsert: {} rows (expected 0 — same PK)",
                        scenarioName,
                        inc.size());
                assertEquals(
                        0,
                        inc.size(),
                        "gRPC strategy (PK polling) cannot detect same-PK updates");
            }

            metrics.recordAnomaly(
                    scenarioName,
                    "UPDATE_NOT_DETECTED_GRPC",
                    "GrpcReplicateCdcStrategy uses PK-based polling and cannot detect "
                            + "same-PK updates. "
                            + UPDATE_COUNT
                            + " rows upserted but not captured by incremental poll.");
            try (CdcMetricsCollector.ScenarioTimer timer =
                        metrics.startScenario(scenarioName, 0)) {
                timer.actualRows(0).failedRows(0);
            }
            log.info("=== {} END ===", scenarioName);
        } finally {
            dropCollection(collectionName);
        }
    }

    @Test
    @Order(7)
    @DisplayName("gRPC 策略不可用降级 — 连接无效端点时 isAvailable 返回 false")
    void testGrpcCdcUnavailable() throws Exception {
        String scenarioName = "grpc.unavailable";
        log.info("=== {} START ===", scenarioName);

        // Build a gRPC strategy pointing to an invalid endpoint
        java.util.Map<String, Object> configMap = new java.util.HashMap<>();
        configMap.put("url", "http://192.0.2.1:19530"); // TEST-NET-1, non-routable
        configMap.put("token", "");
        configMap.put("database", "default");
        configMap.put("collection", "test_unavailable");
        configMap.put("batch_size", 100);
        configMap.put("incremental_batch_size", 500L);
        configMap.put("poll_interval_ms", 500L);
        configMap.put("startup_mode", "INITIAL");
        configMap.put("cdc_strategy", "grpc_replicate");
        configMap.put("primary_key_field", "id");
        configMap.put("channel_timeout_ms", 3000L);
        configMap.put("parallelism", 1);
        org.apache.seatunnel.api.configuration.ReadonlyConfig readonlyConfig =
                org.apache.seatunnel.api.configuration.ReadonlyConfig.fromMap(configMap);
        MilvusCdcSourceConfig cdcConfig = MilvusCdcSourceConfig.of(readonlyConfig);

        // The Milvus SDK's MilvusClientV2 constructor may eagerly attempt to
        // connect and throw a shaded gRPC StatusRuntimeException when the endpoint
        // is unreachable. Both outcomes (constructor throws OR isAvailable()=false)
        // are acceptable — the strategy is correctly unavailable in either case.
        GrpcReplicateCdcStrategy strategy = null;
        boolean constructorThrew = false;
        try {
            strategy = new GrpcReplicateCdcStrategy(cdcConfig);
        } catch (Exception e) {
            constructorThrew = true;
            log.info(
                    "[{}] constructor threw expected exception for unreachable endpoint: {}",
                    scenarioName,
                    e.getMessage());
        }

        if (!constructorThrew) {
            try {
                boolean available = strategy.isAvailable();
                log.info("[{}] isAvailable={} (expected false)", scenarioName, available);
                assertFalse(available, "isAvailable should return false for unreachable endpoint");
            } finally {
                try {
                    strategy.close();
                } catch (Exception ignored) {
                }
            }
        }

        metrics.recordAnomaly(
                scenarioName,
                "EXPECTED_UNAVAILABLE",
                "gRPC CDC correctly reported unavailable for unreachable endpoint "
                        + "http://192.0.2.1:19530"
                        + (constructorThrew ? " (constructor threw connection error)" : ""));
        try (CdcMetricsCollector.ScenarioTimer timer =
                    metrics.startScenario(scenarioName, 0)) {
            timer.actualRows(0).failedRows(0);
        }
        log.info("=== {} END: passed ===", scenarioName);
    }

    // ==================================================================
    // Helpers
    // ==================================================================

    private long extractMaxId(List<SeaTunnelRowWithPosition> rows) {
        long max = -1;
        for (SeaTunnelRowWithPosition r : rows) {
            Object id = r.getRow().getFields()[0];
            if (id instanceof Number) {
                max = Math.max(max, ((Number) id).longValue());
            }
        }
        return max;
    }
}
