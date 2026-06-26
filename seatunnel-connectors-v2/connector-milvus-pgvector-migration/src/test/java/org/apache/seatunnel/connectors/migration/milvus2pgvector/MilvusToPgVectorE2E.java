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
import org.apache.seatunnel.connectors.migration.milvus2pgvector.schema.SchemaMigrator;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.validation.DataValidator;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.validation.ValidationReport;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.sql.PreparedStatement;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * E2E test: full migration flow (schema migration + data insertion + validation).
 *
 * <p>Requires Docker. Enable with {@code -Dmigration.e2e.enabled=true}.
 */
@Slf4j
@EnabledIfSystemProperty(named = "migration.e2e.enabled", matches = "true")
public class MilvusToPgVectorE2E extends MilvusPgVectorTestBase {

    private static final String COLLECTION = "e2e_test_collection";
    private static final String TABLE = "e2e_test_table";
    private static final int ROW_COUNT = 500;

    @Test
    void testFullMigrationFlow() throws Exception {
        // 1. Create Milvus collection + insert data + create index
        createSimpleCollection(COLLECTION, VECTOR_DIM);
        insertTestData(COLLECTION, ROW_COUNT, VECTOR_DIM);
        createVectorIndex(COLLECTION, "vector");
        loadCollection(COLLECTION);

        // 2. Run schema migration
        MigrationConfig config = buildConfig(COLLECTION, TABLE);
        MigrationSchema schema;
        try (SchemaMigrator migrator = new SchemaMigrator(config)) {
            schema = migrator.migrate();
        }
        assertNotNull(schema, "Schema should be introspected");
        assertEquals(COLLECTION, schema.getCollectionName());
        log.info("Schema migration completed: {} columns", schema.getColumns().size());

        // 3. Insert data into pgvector via direct JDBC (simulating SeaTunnel data migration)
        insertDataIntoPgVector(TABLE, ROW_COUNT, VECTOR_DIM);

        // 4. Verify row count
        long pgCount = getPgRowCount(TABLE);
        assertEquals(ROW_COUNT, pgCount, "PG row count should match inserted count");

        // 5. Run validation
        try (DataValidator validator = new DataValidator(config, schema)) {
            ValidationReport report = validator.validate();
            assertNotNull(report, "Validation report should not be null");
            log.info("Validation result: {}", report.isOverallPassed() ? "PASSED" : "FAILED");
            log.info("Validation details:\n{}", report.formatConsole());
            assertTrue(report.isOverallPassed(), "Validation should pass");
        }
    }

    /** Insert test data into pgvector table via JDBC, using the same vectors as Milvus. */
    private void insertDataIntoPgVector(String tableName, int count, int dim) throws Exception {
        Random random = new Random(42); // Same seed as insertTestData
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
