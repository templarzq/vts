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

import org.apache.seatunnel.connectors.migration.milvus2pgvector.cdc.metrics.CdcMetricsCollector;

import lombok.extern.slf4j.Slf4j;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Incremental CDC E2E tests — validates change capture after the initial snapshot.
 *
 * <p><b>Test scenarios covered:</b>
 *
 * <ol>
 *   <li><b>Insert detection</b> — new rows inserted into Milvus after snapshot must be picked up
 *       by the polling strategy and applied to pgvector.
 *   <li><b>Update detection</b> — upserted rows (PK unchanged, vector/scalar changed) must be
 *       detected and overwrite the corresponding pgvector rows.
 *   <li><b>Delete detection limitation</b> — documents a known limitation: the
 *       {@link PollingIncrementalCdcStrategy} uses PK-based filtering ({@code id > watermark}),
 *       which cannot detect deletions. The test records this as an anomaly in the report for
 *       problem analysis. To capture deletes, the {@link GrpcReplicateCdcStrategy} or a separate
 *       tombstone mechanism is required.
 *   <li><b>Mixed workload</b> — interleaved insert + update + delete operations.
 * </ol>
 *
 * <p><b>Validation metrics:</b>
 *
 * <ul>
 *   <li>Incremental latency P99 (≤ 5000ms budget)
 *   <li>Row count after each operation
 *   <li>Vector similarity of updated rows
 *   <li>Success rate ≥ 99%
 * </ul>
 */
@org.junit.jupiter.api.condition.EnabledIfSystemProperty(
        named = "migration.cdc.e2e.enabled",
        matches = "true")
@DisplayName("Milvus CDC Incremental Sync E2E")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Slf4j
public class MilvusCdcIncrementalE2E extends MilvusCdcE2ETestBase {

    private static final int VECTOR_DIM = 64;
    private static final int SNAPSHOT_COUNT = 200;
    private static final int INSERT_COUNT = 100;
    private static final int UPDATE_COUNT = 50;
    private static final int DELETE_COUNT = 30;

    @Override
    protected String scenarioLabel() {
        return "incremental";
    }

    @Test
    @Order(1)
    @DisplayName("Incremental CDC — insert detection")
    void testIncrementalInsert() throws Exception {
        String collectionName = "cdc_inc_insert";
        String tableName = "pg_inc_insert";
        String scenarioName = "incremental.insert";

        log.info("=== {} START ===", scenarioName);
        createCollection(collectionName, VECTOR_DIM);
        try {
            // 1. Insert snapshot baseline
            long baselineWriteTs = insertData(collectionName, 0, SNAPSHOT_COUNT, VECTOR_DIM, 100L);
            verifyMilvusCount(collectionName, SNAPSHOT_COUNT);

            // 2. Apply initial snapshot to pgvector
            createPgTable(tableName, VECTOR_DIM);
            try {
                PollingIncrementalCdcStrategy strategy = buildPollingStrategy(collectionName, 100);
                List<SeaTunnelRowWithPosition> snap = runSnapshot(strategy, collectionName,
                        SNAPSHOT_COUNT);
                int snapApplied = applyRowsToPg(tableName, snap, VECTOR_DIM);
                metrics.recordScenario("incremental.insert.snapshot", 0L, SNAPSHOT_COUNT,
                        snapApplied, Math.max(0, SNAPSHOT_COUNT - snapApplied),
                        baselineWriteTs, System.currentTimeMillis());
                verifyPgCount(tableName, SNAPSHOT_COUNT);

                // 3. Insert INCREMENTAL rows (id SNAPSHOT_COUNT..SNAPSHOT_COUNT+INSERT_COUNT-1)
                long incWriteTs = insertData(collectionName, SNAPSHOT_COUNT, INSERT_COUNT,
                        VECTOR_DIM, 200L);
                Thread.sleep(2000); // wait for visibility
                verifyMilvusCount(collectionName, SNAPSHOT_COUNT + INSERT_COUNT);

                // 4. Poll incremental changes from watermark = SNAPSHOT_COUNT - 1 (exclusive)
                long sinkConfirmTs;
                List<SeaTunnelRowWithPosition> incRows;
                try (CdcMetricsCollector.ScenarioTimer timer =
                                metrics.startScenario(scenarioName, INSERT_COUNT)) {
                    incRows = pollUntil(strategy, collectionName,
                            SNAPSHOT_COUNT - 1, INSERT_COUNT, 10, 1000L);
                    int applied = applyRowsToPg(tableName, incRows, VECTOR_DIM);
                    sinkConfirmTs = System.currentTimeMillis();
                    timer.actualRows(applied)
                            .failedRows(Math.max(0, INSERT_COUNT - applied))
                            .syncDelay(incWriteTs, sinkConfirmTs);
                }
                strategy.close();

                log.info("[{}] detected {} incremental rows", scenarioName, incRows.size());
                assertTrue(incRows.size() >= INSERT_COUNT,
                        "Should detect at least " + INSERT_COUNT + " inserts, got "
                                + incRows.size());

                // 5. Verify final row count
                int finalCount = queryPgCount(tableName);
                assertTrue(finalCount >= SNAPSHOT_COUNT + INSERT_COUNT,
                        "pgvector should have at least " + (SNAPSHOT_COUNT + INSERT_COUNT)
                                + " rows, got " + finalCount);

                // 6. Verify similarity on inserted rows
                verifyVectorSimilarity(scenarioName, collectionName, tableName, VECTOR_DIM,
                        Math.min(SAMPLE_SIZE, INSERT_COUNT));
            } finally {
                dropPgTable(tableName);
            }
        } finally {
            dropCollection(collectionName);
        }
        log.info("=== {} END ===", scenarioName);
    }

    @Test
    @Order(2)
    @DisplayName("Incremental CDC — update detection via upsert")
    void testIncrementalUpdate() throws Exception {
        String collectionName = "cdc_inc_update";
        String tableName = "pg_inc_update";
        String scenarioName = "incremental.update";

        log.info("=== {} START ===", scenarioName);
        createCollection(collectionName, VECTOR_DIM);
        try {
            // 1. Insert snapshot baseline
            insertData(collectionName, 0, SNAPSHOT_COUNT, VECTOR_DIM, 300L);
            verifyMilvusCount(collectionName, SNAPSHOT_COUNT);

            createPgTable(tableName, VECTOR_DIM);
            try {
                PollingIncrementalCdcStrategy strategy = buildPollingStrategy(collectionName, 100);

                // 2. Apply initial snapshot
                List<SeaTunnelRowWithPosition> snap = runSnapshot(strategy, collectionName,
                        SNAPSHOT_COUNT);
                applyRowsToPg(tableName, snap, VECTOR_DIM);
                verifyPgCount(tableName, SNAPSHOT_COUNT);

                // 3. Capture baseline vector for first UPDATE_COUNT rows
                float[][] baselineVectors = new float[UPDATE_COUNT][];
                for (int i = 0; i < UPDATE_COUNT; i++) {
                    baselineVectors[i] = queryPgVector(tableName, i);
                    assertTrue(baselineVectors[i] != null, "Baseline vector " + i + " missing");
                }

                // 4. Upsert (update) the first UPDATE_COUNT rows with new vectors
                long updateWriteTs = System.currentTimeMillis();
                upsertData(collectionName, 0, UPDATE_COUNT, VECTOR_DIM, 400L);
                Thread.sleep(2000);

                // 5. Poll for changes. Note: polling strategy uses id > watermark; since
                //    upserted rows retain the same PK, they will NOT be re-captured unless the
                //    watermark is reset. We document this as an expected behavior.
                //    To detect updates with PK-polling, the watermark must be set to -1 (re-scan)
                //    or a dedicated update-detection mechanism is needed.
                List<SeaTunnelRowWithPosition> incRows;
                int applied;
                try (CdcMetricsCollector.ScenarioTimer timer =
                                metrics.startScenario(scenarioName, UPDATE_COUNT)) {
                    // Re-scan from -1 to pick up the upserted rows
                    incRows = runSnapshot(strategy, collectionName, SNAPSHOT_COUNT);
                    applied = applyRowsToPg(tableName, incRows, VECTOR_DIM);
                    long sinkConfirmTs = System.currentTimeMillis();
                    timer.actualRows(applied)
                            .failedRows(Math.max(0, UPDATE_COUNT - applied))
                            .syncDelay(updateWriteTs, sinkConfirmTs);
                }
                strategy.close();

                log.info("[{}] re-scan captured {} rows", scenarioName, incRows.size());

                // 6. Verify the upserted vectors were applied (vectors should differ from baseline)
                int changedCount = 0;
                for (int i = 0; i < UPDATE_COUNT; i++) {
                    float[] newVec = queryPgVector(tableName, i);
                    if (newVec != null && !vectorsEqual(baselineVectors[i], newVec, 1e-6f)) {
                        changedCount++;
                    } else {
                        metrics.recordAnomaly(scenarioName, "UPDATE_NOT_APPLIED",
                                "id=" + i + " vector unchanged after upsert");
                    }
                }
                log.info("[{}] {} of {} updated vectors differ from baseline",
                        scenarioName, changedCount, UPDATE_COUNT);
                assertTrue(changedCount > 0,
                        "At least one updated vector should differ from baseline");

                // Record the change-detection method as an anomaly for problem analysis
                metrics.recordAnomaly(scenarioName, "UPDATE_DETECTION_METHOD",
                        "PK-polling cannot detect same-PK updates; re-scan required. "
                                + "Use GrpcReplicateCdcStrategy for true CDC update capture.");

                // 7. Verify similarity on updated rows
                verifyVectorSimilarity(scenarioName, collectionName, tableName, VECTOR_DIM,
                        Math.min(SAMPLE_SIZE, UPDATE_COUNT));
            } finally {
                dropPgTable(tableName);
            }
        } finally {
            dropCollection(collectionName);
        }
        log.info("=== {} END ===", scenarioName);
    }

    @Test
    @Order(3)
    @DisplayName("Incremental CDC — delete detection limitation")
    void testIncrementalDeleteLimitation() throws Exception {
        String collectionName = "cdc_inc_delete";
        String tableName = "pg_inc_delete";
        String scenarioName = "incremental.delete";

        log.info("=== {} START ===", scenarioName);
        createCollection(collectionName, VECTOR_DIM);
        try {
            // 1. Insert baseline
            insertData(collectionName, 0, SNAPSHOT_COUNT, VECTOR_DIM, 500L);
            verifyMilvusCount(collectionName, SNAPSHOT_COUNT);

            createPgTable(tableName, VECTOR_DIM);
            try {
                PollingIncrementalCdcStrategy strategy = buildPollingStrategy(collectionName, 100);
                List<SeaTunnelRowWithPosition> snap = runSnapshot(strategy, collectionName,
                        SNAPSHOT_COUNT);
                int snapApplied = applyRowsToPg(tableName, snap, VECTOR_DIM);
                verifyPgCount(tableName, SNAPSHOT_COUNT);

                // 2. Delete DELETE_COUNT rows from Milvus (id SNAPSHOT_COUNT-DELETE_COUNT .. SNAPSHOT_COUNT-1)
                long deleteWriteTs = System.currentTimeMillis();
                int deleteStart = SNAPSHOT_COUNT - DELETE_COUNT;
                deleteData(collectionName, deleteStart, DELETE_COUNT);
                Thread.sleep(2000);

                // 3. Verify Milvus row count dropped
                long milvusAfterDelete = queryMilvusCount(collectionName);
                assertEquals(SNAPSHOT_COUNT - DELETE_COUNT, milvusAfterDelete,
                        "Milvus should have " + (SNAPSHOT_COUNT - DELETE_COUNT)
                                + " rows after delete");

                // 4. Poll for changes — expect zero rows captured (deletes are NOT detected by
                //    PK-polling strategy). This is the documented limitation.
                List<SeaTunnelRowWithPosition> incRows;
                try (CdcMetricsCollector.ScenarioTimer timer =
                                metrics.startScenario(scenarioName, 0)) {
                    // Watermark = SNAPSHOT_COUNT - 1 (last known PK)
                    incRows = pollUntil(strategy, collectionName,
                            SNAPSHOT_COUNT - 1, 1, 3, 500L);
                    long sinkConfirmTs = System.currentTimeMillis();
                    timer.actualRows(incRows.size())
                            .failedRows(0)
                            .syncDelay(deleteWriteTs, sinkConfirmTs);
                }
                strategy.close();

                log.info("[{}] polling detected {} rows after delete (expected 0)",
                        scenarioName, incRows.size());
                assertTrue(incRows.isEmpty(),
                        "PK-polling strategy should NOT detect deletes; got " + incRows.size());

                // 5. Verify pgvector still has all original rows (delete not propagated)
                int pgCountAfter = queryPgCount(tableName);
                assertEquals(SNAPSHOT_COUNT, pgCountAfter,
                        "pgvector should still have all " + SNAPSHOT_COUNT
                                + " rows — delete not propagated by polling");

                // 6. Record the limitation as a known issue for problem analysis
                metrics.recordAnomaly(scenarioName, "DELETE_NOT_DETECTED",
                        "PollingIncrementalCdcStrategy does not capture deletes. "
                                + DELETE_COUNT + " rows deleted from Milvus but pgvector unchanged. "
                                + "Use GrpcReplicateCdcStrategy or external tombstone mechanism "
                                + "for delete propagation.");
            } finally {
                dropPgTable(tableName);
            }
        } finally {
            dropCollection(collectionName);
        }
        log.info("=== {} END ===", scenarioName);
    }

    @Test
    @Order(4)
    @DisplayName("Incremental CDC — mixed insert + update + delete workload")
    void testIncrementalMixed() throws Exception {
        String collectionName = "cdc_inc_mixed";
        String tableName = "pg_inc_mixed";
        String scenarioName = "incremental.mixed";

        log.info("=== {} START ===", scenarioName);
        createCollection(collectionName, VECTOR_DIM);
        try {
            // 1. Insert baseline
            insertData(collectionName, 0, SNAPSHOT_COUNT, VECTOR_DIM, 600L);
            verifyMilvusCount(collectionName, SNAPSHOT_COUNT);

            createPgTable(tableName, VECTOR_DIM);
            try {
                PollingIncrementalCdcStrategy strategy = buildPollingStrategy(collectionName, 100);
                List<SeaTunnelRowWithPosition> snap = runSnapshot(strategy, collectionName,
                        SNAPSHOT_COUNT);
                applyRowsToPg(tableName, snap, VECTOR_DIM);
                verifyPgCount(tableName, SNAPSHOT_COUNT);

                // 2. Mixed workload:
                //    a. Insert 50 new rows (id SNAPSHOT_COUNT..SNAPSHOT_COUNT+49)
                //    b. Upsert (update) 30 existing rows (id 0..29)
                //    c. Delete 20 rows (id 30..49)
                long startTs = System.currentTimeMillis();
                insertData(collectionName, SNAPSHOT_COUNT, 50, VECTOR_DIM, 700L);
                upsertData(collectionName, 0, 30, VECTOR_DIM, 800L);
                deleteData(collectionName, 30, 20);
                Thread.sleep(2000);

                int expectedMilvusCount = SNAPSHOT_COUNT + 50 - 20;
                verifyMilvusCount(collectionName, expectedMilvusCount);

                // 3. Re-scan from id > -1 to capture inserts + upserts (deletes will be missed)
                List<SeaTunnelRowWithPosition> incRows;
                int applied;
                try (CdcMetricsCollector.ScenarioTimer timer =
                                metrics.startScenario(scenarioName, 80)) {
                    // Expect 50 inserts + 30 upserts = 80 row events (deletes missed)
                    incRows = runSnapshot(strategy, collectionName, expectedMilvusCount);
                    applied = applyRowsToPg(tableName, incRows, VECTOR_DIM);
                    long sinkConfirmTs = System.currentTimeMillis();
                    timer.actualRows(applied)
                            .failedRows(Math.max(0, expectedMilvusCount - applied))
                            .syncDelay(startTs, sinkConfirmTs);
                }
                strategy.close();

                log.info("[{}] mixed re-scan captured {} rows (Milvus has {})",
                        scenarioName, incRows.size(), expectedMilvusCount);

                // 4. pgvector should have all snapshot + inserted rows (deletes NOT propagated)
                //    = 200 (snapshot) + 50 (inserts) = 250 rows
                int pgCount = queryPgCount(tableName);
                assertTrue(pgCount >= SNAPSHOT_COUNT + 50,
                        "pgvector should have at least " + (SNAPSHOT_COUNT + 50)
                                + " rows after mixed workload, got " + pgCount);

                // 5. Record mixed-workload analysis
                metrics.recordAnomaly(scenarioName, "MIXED_DELETE_GAP",
                        "Mixed workload: 20 deletes from Milvus not propagated to pgvector. "
                                + "Inserts + upserts captured via re-scan.");

                // 6. Verify similarity on a sample
                verifyVectorSimilarity(scenarioName, collectionName, tableName, VECTOR_DIM,
                        Math.min(SAMPLE_SIZE, expectedMilvusCount));
            } finally {
                dropPgTable(tableName);
            }
        } finally {
            dropCollection(collectionName);
        }
        log.info("=== {} END ===", scenarioName);
    }

    private static boolean vectorsEqual(float[] a, float[] b, float epsilon) {
        if (a == null || b == null) return a == b;
        if (a.length != b.length) return false;
        for (int i = 0; i < a.length; i++) {
            if (Math.abs(a[i] - b[i]) > epsilon) return false;
        }
        return true;
    }
}
