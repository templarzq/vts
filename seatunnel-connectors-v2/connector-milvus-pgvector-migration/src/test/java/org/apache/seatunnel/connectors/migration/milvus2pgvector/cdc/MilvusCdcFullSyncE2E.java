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

/**
 * Full-snapshot synchronization E2E tests — validates that the CDC pipeline correctly replicates a
 * one-time snapshot of Milvus data into pgvector.
 *
 * <p><b>Test scenarios covered:</b>
 *
 * <ol>
 *   <li><b>Small dataset</b> (100 rows × 8-dim) — smoke test for end-to-end flow.
 *   <li><b>Medium dataset</b> (1000 rows × 128-dim) — typical workload.
 *   <li><b>Large dataset</b> (5000 rows × 256-dim) — exercises pagination and back-pressure.
 * </ol>
 *
 * <p><b>Validation metrics per scenario:</b>
 *
 * <ul>
 *   <li>Row count match (Milvus vs pgvector)
 *   <li>Vector cosine similarity (sample of 100 PKs, threshold ≥ 0.9999)
 *   <li>Scalar field equality (category column)
 *   <li>Snapshot throughput (rows/sec, soft budget ≥ 50 rows/s)
 *   <li>Sync delay (write-to-confirm latency)
 * </ul>
 *
 * <p><b>Execution steps:</b>
 *
 * <ol>
 *   <li>Create Milvus collection with id, vector, category, created_at fields.
 *   <li>Insert test data at the requested scale with deterministic seed.
 *   <li>Create matching pgvector table.
 *   <li>Run CDC snapshot via {@link PollingIncrementalCdcStrategy#pollChanges}.
 *   <li>Apply polled rows to pgvector via JDBC upsert.
 *   <li>Verify row count, vector similarity, scalar fields.
 *   <li>Record metrics for the test report.
 * </ol>
 */
@org.junit.jupiter.api.condition.EnabledIfSystemProperty(
        named = "migration.cdc.e2e.enabled",
        matches = "true")
@DisplayName("Milvus CDC Full-Snapshot Sync E2E")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Slf4j
public class MilvusCdcFullSyncE2E extends MilvusCdcE2ETestBase {

    @Override
    protected String scenarioLabel() {
        return "full-sync";
    }

    @Test
    @Order(1)
    @DisplayName("Full sync — small dataset (100 rows × 8-dim)")
    void testFullSyncSmall() throws Exception {
        runFullSyncScenario("snapshot.small", "cdc_full_small", "pg_full_small",
                100, 8, 42L);
    }

    @Test
    @Order(2)
    @DisplayName("Full sync — medium dataset (1000 rows × 128-dim)")
    void testFullSyncMedium() throws Exception {
        runFullSyncScenario("snapshot.medium", "cdc_full_medium", "pg_full_medium",
                1000, 128, 4242L);
    }

    @Test
    @Order(3)
    @DisplayName("Full sync — large dataset (5000 rows × 256-dim)")
    void testFullSyncLarge() throws Exception {
        runFullSyncScenario("snapshot.large", "cdc_full_large", "pg_full_large",
                5000, 256, 424242L);
    }

    /**
     * Common scenario runner: set up → insert → snapshot → apply → verify → record.
     *
     * @param scenarioName metrics scenario key
     * @param collectionName Milvus collection name (will be dropped if exists)
     * @param tableName pgvector table name (will be dropped if exists)
     * @param rowCount number of rows to insert
     * @param vectorDim vector dimension
     * @param seed RNG seed for deterministic vector generation
     */
    private void runFullSyncScenario(
            String scenarioName,
            String collectionName,
            String tableName,
            int rowCount,
            int vectorDim,
            long seed)
            throws Exception {

        log.info("=== {} START: collection={}, table={}, rows={}, dim={} ===",
                scenarioName, collectionName, tableName, rowCount, vectorDim);

        // 1. Create Milvus collection
        createCollection(collectionName, vectorDim);

        try {
            // 2. Insert snapshot data (id 0..rowCount-1)
            long sourceWriteTs = insertData(collectionName, 0, rowCount, vectorDim, seed);
            verifyMilvusCount(collectionName, rowCount);

            // 3. Create pgvector target table
            createPgTable(tableName, vectorDim);

            try {
                // 4. Build CDC strategy & run snapshot
                PollingIncrementalCdcStrategy strategy = buildPollingStrategy(collectionName, 500);
                long sinkConfirmTs;
                List<SeaTunnelRowWithPosition> snapshotRows;
                try (CdcMetricsCollector.ScenarioTimer timer =
                                metrics.startScenario(scenarioName, rowCount)) {
                    snapshotRows = runSnapshot(strategy, collectionName, rowCount);
                    int applied = applyRowsToPg(tableName, snapshotRows, vectorDim);
                    sinkConfirmTs = System.currentTimeMillis();
                    timer.actualRows(applied)
                            .failedRows(Math.max(0, rowCount - applied))
                            .syncDelay(sourceWriteTs, sinkConfirmTs);
                }
                strategy.close();

                log.info("[{}] snapshot polled {} rows, applied to pgvector",
                        scenarioName, snapshotRows.size());

                // 5. Verify row count
                verifyPgCount(tableName, rowCount);

                // 6. Verify vector similarity (sample)
                verifyVectorSimilarity(scenarioName, collectionName, tableName, vectorDim,
                        Math.min(SAMPLE_SIZE, rowCount));

                // 7. Verify scalar fields
                verifyScalarFields(scenarioName, collectionName, tableName,
                        Math.min(SAMPLE_SIZE, rowCount));

                log.info("=== {} END: passed ===", scenarioName);
            } finally {
                dropPgTable(tableName);
            }
        } finally {
            dropCollection(collectionName);
        }
    }
}
