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

package org.apache.seatunnel.connectors.migration.milvus2pgvector.schema;

import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.config.MigrationConfig;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.exception.MigrationErrorCode;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.exception.MigrationException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * Orchestrates the schema migration phase: introspect Milvus → generate DDL → execute on pgvector.
 *
 * <p>The pgvector extension is created with {@code CREATE EXTENSION IF NOT EXISTS vector} before
 * table DDL. Table and index DDLs are both {@code IF NOT EXISTS} to make re-runs idempotent. If
 * {@link MigrationConfig#isDropExistingTable()}, a {@code DROP TABLE IF EXISTS} is issued first.
 */
@Slf4j
public class SchemaMigrator implements AutoCloseable {

    private final MigrationConfig config;
    private final Connection pgConnection;

    public SchemaMigrator(MigrationConfig config) throws SQLException {
        this.config = config;
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new MigrationException(
                    MigrationErrorCode.DDL_EXECUTION_FAILED,
                    "PostgreSQL JDBC driver not on classpath",
                    e);
        }
        this.pgConnection =
                DriverManager.getConnection(
                        config.getPgUrl(), config.getPgUser(), config.getPgPassword());
    }

    /**
     * Run the schema migration: returns the {@link MigrationSchema} snapshot captured from Milvus
     * (useful for downstream phases that need the column list).
     */
    public MigrationSchema migrate() {
        log.info(
                "Schema migration: introspecting Milvus collection '{}'", config.getMilvusCollection());
        MigrationSchema schema;
        try (MilvusSchemaIntrospector introspector =
                new MilvusSchemaIntrospector(config.getMilvusUrl(), config.getMilvusToken())) {
            schema = introspector.introspect(config.getMilvusCollection());
        }
        log.info(
                "Introspected {} columns and {} indexes from Milvus collection '{}'",
                schema.getColumns() != null ? schema.getColumns().size() : 0,
                schema.getIndexes() != null ? schema.getIndexes().size() : 0,
                config.getMilvusCollection());

        executeDdls(schema);
        return schema;
    }

    private void executeDdls(MigrationSchema schema) {
        try {
            pgConnection.setAutoCommit(true);
            try (Statement stmt = pgConnection.createStatement()) {
                // 1. ensure pgvector extension exists
                log.info("Ensuring pgvector extension is installed");
                stmt.execute("CREATE EXTENSION IF NOT EXISTS vector;");

                // 2. create schema if not exists
                if (config.getPgSchema() != null && !config.getPgSchema().isEmpty()) {
                    stmt.execute("CREATE SCHEMA IF NOT EXISTS \"" + config.getPgSchema() + "\";");
                }

                // 3. create table
                String createTableDdl =
                        PgVectorSchemaGenerator.generateCreateTableDdl(
                                schema,
                                config.getPgSchema(),
                                config.getPgTable(),
                                config.isDropExistingTable(),
                                config.isAllowPrecisionLoss());
                log.info("Executing CREATE TABLE DDL:\n{}", createTableDdl);
                for (String sql : createTableDdl.split(";")) {
                    String trimmed = sql.trim();
                    if (!trimmed.isEmpty()) {
                        stmt.execute(trimmed + ";");
                    }
                }

                // 4. create indexes (optional)
                if (!config.isSkipIndexMigration()) {
                    List<String> indexDdls =
                            PgVectorSchemaGenerator.generateCreateIndexDdls(
                                    schema, config.getPgSchema(), config.getPgTable());
                    for (String ddl : indexDdls) {
                        log.info("Executing index DDL: {}", ddl);
                        stmt.execute(ddl);
                    }
                    log.info("Created {} vector indexes on pgvector", indexDdls.size());
                } else {
                    log.info("Skipping index migration per config (skipIndexMigration=true)");
                }
            }
        } catch (SQLException e) {
            throw new MigrationException(
                    MigrationErrorCode.DDL_EXECUTION_FAILED,
                    "Failed to execute pgvector DDL: " + e.getMessage(),
                    e);
        }
    }

    /** Expose the underlying connection for reuse by the validation phase. */
    public Connection getConnection() {
        return pgConnection;
    }

    @Override
    public void close() {
        try {
            if (pgConnection != null && !pgConnection.isClosed()) {
                pgConnection.close();
            }
        } catch (SQLException e) {
            log.warn("Failed to close pg connection", e);
        }
    }
}
