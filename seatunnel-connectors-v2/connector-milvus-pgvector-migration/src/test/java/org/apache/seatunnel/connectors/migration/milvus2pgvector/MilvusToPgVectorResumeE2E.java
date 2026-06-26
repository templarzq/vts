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

import org.apache.seatunnel.connectors.migration.milvus2pgvector.audit.MigrationLogger;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.audit.MigrationProgressTracker;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.cli.MigrationOrchestrator;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.config.MigrationConfig;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.schema.MigrationSchema;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.schema.SchemaMigrator;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.validation.DataValidator;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.validation.ValidationReport;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.PreparedStatement;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * E2E test: breakpoint resume flow.
 *
 * <p>Simulates a scenario where schema migration succeeds, data migration fails, then resume skips
 * the completed schema phase and runs validation.
 *
 * <p>Requires Docker. Enable with {@code -Dmigration.e2e.enabled=true}.
 */
@Slf4j
@EnabledIfSystemProperty(named = "migration.e2e.enabled", matches = "true")
public class MilvusToPgVectorResumeE2E extends MilvusPgVectorTestBase {

    private static final String COLLECTION = "e2e_resume_collection";
    private static final String TABLE = "e2e_resume_table";
    private static final int ROW_COUNT = 300;

    @Test
    void testResumeSkipsCompletedSchemaPhase() throws Exception {
        Path logDir = Paths.get("target/migration-logs");

        // 1. Create Milvus collection + insert data + create index
        createSimpleCollection(COLLECTION, VECTOR_DIM);
        insertTestData(COLLECTION, ROW_COUNT, VECTOR_DIM);
        createVectorIndex(COLLECTION, "vector");
        loadCollection(COLLECTION);

        // 2. Run schema migration manually (phase 1)
        MigrationConfig config = buildConfig(COLLECTION, TABLE);
        MigrationSchema schema;
        try (SchemaMigrator migrator = new SchemaMigrator(config)) {
            schema = migrator.migrate();
        }
        assertNotNull(schema, "Schema should be introspected");
        log.info("Schema migration completed successfully");

        // 3. Record schema phase as complete in progress tracker
        try (MigrationProgressTracker tracker =
                new MigrationProgressTracker(logDir, COLLECTION)) {
            tracker.markPhaseComplete(MigrationProgressTracker.Phase.SCHEMA);
            tracker.markPhaseFailed(MigrationProgressTracker.Phase.DATA);
            log.info("Marked SCHEMA=SUCCESS, DATA=FAILED in progress tracker");
        }

        // 4. Simulate data migration recovery: insert data into pgvector
        insertDataIntoPgVector(TABLE, ROW_COUNT, VECTOR_DIM);
        long pgCount = getPgRowCount(TABLE);
        assertEquals(ROW_COUNT, pgCount, "PG row count should match after data insertion");

        // 5. Run orchestrator with resume=true, skipping schema and data, running validation only
        try (MigrationLogger logger = new MigrationLogger(logDir, COLLECTION);
                MigrationProgressTracker tracker =
                        new MigrationProgressTracker(logDir, COLLECTION)) {

            MigrationOrchestrator orchestrator =
                    new MigrationOrchestrator(
                            config,
                            logger,
                            tracker,
                            null, // no job submitter needed
                            true, // resume
                            false, // don't run schema (should be skipped anyway)
                            false, // don't run data
                            true); // run validation

            org.apache.seatunnel.connectors.migration.milvus2pgvector.audit.MigrationReport report =
                    orchestrator.run();

            assertNotNull(report, "Migration report should not be null");
            log.info("Resume migration report:\n{}", report.formatConsole());
            assertTrue(report.isSuccess(), "Resume migration should succeed");
            assertEquals("SKIPPED", report.getSchemaMigrationResult());
        }

        // 6. Verify validation report was generated and passed
        try (DataValidator validator = new DataValidator(config, schema)) {
            ValidationReport validationReport = validator.validate();
            assertTrue(validationReport.isOverallPassed(), "Validation should pass after resume");
            log.info("Post-resume validation: PASSED");
        }
    }

    /** Insert test data into pgvector table via JDBC, using the same vectors as Milvus. */
    private void insertDataIntoPgVector(String tableName, int count, int dim) throws Exception {
        Random random = new Random(42);
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
}
