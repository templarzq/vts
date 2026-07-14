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

package org.apache.seatunnel.connectors.migration.milvus2pgvector.cli;

import org.apache.seatunnel.connectors.migration.milvus2pgvector.audit.MigrationLogger;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.audit.MigrationProgressTracker;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.audit.MigrationReport;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.config.MigrationConfig;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.schema.MigrationSchema;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.schema.MilvusSchemaIntrospector;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.schema.SchemaMigrator;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.validation.DataValidator;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.validation.ValidationReport;

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;

import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Orchestrates the three migration phases: schema → data → validation.
 *
 * <p>Supports breakpoint resume via {@link MigrationProgressTracker}: when {@code resume=true},
 * completed phases are skipped.
 */
@Slf4j
public class MigrationOrchestrator {

    private final MigrationConfig config;
    private final MigrationLogger logger;
    private final MigrationProgressTracker tracker;
    private final SeaTunnelJobSubmitter jobSubmitter;
    private final boolean resume;
    private final boolean runSchema;
    private final boolean runData;
    private final boolean runValidation;

    /**
     * @param config migration configuration
     * @param logger migration logger
     * @param tracker progress tracker for checkpoint/resume
     * @param jobSubmitter SeaTunnel job submitter (may be {@code null} if data phase is skipped)
     * @param resume whether to resume from last checkpoint
     * @param runSchema whether to run the schema phase
     * @param runData whether to run the data phase
     * @param runValidation whether to run the validation phase
     */
    public MigrationOrchestrator(
            MigrationConfig config,
            MigrationLogger logger,
            MigrationProgressTracker tracker,
            SeaTunnelJobSubmitter jobSubmitter,
            boolean resume,
            boolean runSchema,
            boolean runData,
            boolean runValidation) {
        this.config = config;
        this.logger = logger;
        this.tracker = tracker;
        this.jobSubmitter = jobSubmitter;
        this.resume = resume;
        this.runSchema = runSchema;
        this.runData = runData;
        this.runValidation = runValidation;
    }

    /** Run the migration phases and return a report. */
    public MigrationReport run() {
        Instant startTime = Instant.now();
        logger.logPhase("MIGRATION", "STARTED");
        logger.info("Configuration: " + summarizeConfig());

        MigrationSchema schema = null;
        String schemaResult = "SKIPPED";
        String dataResult = "SKIPPED";
        ValidationReport validationReport = null;
        boolean overallSuccess = true;

        // ---- Phase 1: Schema migration ----
        if (shouldRunSchema()) {
            if (resume && tracker.isPhaseComplete(MigrationProgressTracker.Phase.SCHEMA)) {
                logger.logPhase("SCHEMA", "SKIPPED (already complete)");
                schemaResult = "SKIPPED (resume)";
                // Re-introspect schema for downstream phases
                schema = introspectSchemaOnly();
            } else {
                try {
                    tracker.markPhaseStart(MigrationProgressTracker.Phase.SCHEMA);
                    logger.logPhase("SCHEMA", "STARTED");
                    try (SchemaMigrator migrator = new SchemaMigrator(config)) {
                        schema = migrator.migrate();
                    }
                    tracker.markPhaseComplete(MigrationProgressTracker.Phase.SCHEMA);
                    logger.logPhase("SCHEMA", "COMPLETED");
                    schemaResult = "SUCCESS";
                } catch (Exception e) {
                    tracker.markPhaseFailed(MigrationProgressTracker.Phase.SCHEMA);
                    logger.logPhase("SCHEMA", "FAILED");
                    logger.error("Schema migration failed", e);
                    schemaResult = "FAILED: " + e.getMessage();
                    return buildReport(
                            startTime, schemaResult, dataResult, validationReport, false);
                }
            }
        }

        // ---- Phase 2: Data migration ----
        if (shouldRunData()) {
            if (resume && tracker.isPhaseComplete(MigrationProgressTracker.Phase.DATA)) {
                logger.logPhase("DATA", "SKIPPED (already complete)");
                dataResult = "SKIPPED (resume)";
            } else {
                try {
                    tracker.markPhaseStart(MigrationProgressTracker.Phase.DATA);
                    logger.logPhase("DATA", "STARTED");
                    if (jobSubmitter == null) {
                        throw new IllegalStateException(
                                "SeaTunnelJobSubmitter is required for data migration phase");
                    }
                    List<String> collections = resolveCollections();
                    if (collections.size() > 1) {
                        logger.info(String.format("Migrating %d collections in parallel", collections.size()));
                        dataResult = migrateMultipleCollections(collections);
                    } else {
                        int exitCode = jobSubmitter.submitAndWait(config, logger);
                        if (exitCode != 0) {
                            throw new RuntimeException(
                                    "SeaTunnel job exited with code " + exitCode);
                        }
                        dataResult = "SUCCESS";
                    }
                    tracker.markPhaseComplete(MigrationProgressTracker.Phase.DATA);
                    logger.logPhase("DATA", "COMPLETED");
                } catch (Exception e) {
                    tracker.markPhaseFailed(MigrationProgressTracker.Phase.DATA);
                    logger.logPhase("DATA", "FAILED");
                    logger.error("Data migration failed", e);
                    dataResult = "FAILED: " + e.getMessage();
                    overallSuccess = false;
                }
            }
        }

        // ---- Phase 3: Validation ----
        if (shouldRunValidation()) {
            if (resume
                    && tracker.isPhaseComplete(MigrationProgressTracker.Phase.VALIDATION)) {
                logger.logPhase("VALIDATION", "SKIPPED (already complete)");
            } else {
                try {
                    tracker.markPhaseStart(MigrationProgressTracker.Phase.VALIDATION);
                    logger.logPhase("VALIDATION", "STARTED");
                    if (schema == null) {
                        schema = introspectSchemaOnly();
                    }
                    try (DataValidator validator = new DataValidator(config, schema)) {
                        validationReport = validator.validate();
                    }
                    tracker.markPhaseComplete(MigrationProgressTracker.Phase.VALIDATION);
                    logger.logPhase("VALIDATION", "COMPLETED");
                    if (!validationReport.isOverallPassed()) {
                        overallSuccess = false;
                        logger.warn("Validation reported failures");
                    }
                } catch (Exception e) {
                    tracker.markPhaseFailed(MigrationProgressTracker.Phase.VALIDATION);
                    logger.logPhase("VALIDATION", "FAILED");
                    logger.error("Validation failed", e);
                    overallSuccess = false;
                }
            }
        }

        logger.logPhase("MIGRATION", overallSuccess ? "COMPLETED" : "COMPLETED WITH ERRORS");
        return buildReport(
                startTime, schemaResult, dataResult, validationReport, overallSuccess);
    }

    /**
     * Discover all collection names from Milvus. Used when the collection config is
     * {@code *} or a comma-separated list for parallel per-collection job submission.
     */
    private List<String> resolveCollections() {
        String col = config.getMilvusCollection();
        if (col == null || col.isEmpty() || "*".equals(col)) {
            // Discover all collections from Milvus
            List<String> names = new ArrayList<>();
            try {
                ConnectConfig cc = ConnectConfig.builder()
                        .uri(config.getMilvusUrl())
                        .token(config.getMilvusToken())
                        .build();
                MilvusClientV2 client = new MilvusClientV2(cc);
                try {
                    io.milvus.v2.service.collection.response.ListCollectionsResp resp =
                            client.listCollections();
                    if (resp != null && resp.getCollectionNames() != null) {
                        names.addAll(resp.getCollectionNames());
                    }
                } finally {
                    client.close();
                }
                logger.info(String.format("Discovered %d collections from Milvus: %s",
                        names.size(), names));
            } catch (Exception e) {
                throw new RuntimeException("Failed to discover collections from Milvus", e);
            }
            return names;
        }
        // Single collection (possibly comma-separated)
        if (col.contains(",")) {
            List<String> names = new ArrayList<>();
            for (String part : col.split(",")) {
                String trimmed = part.trim();
                if (!trimmed.isEmpty()) names.add(trimmed);
            }
            return names;
        }
        return java.util.Collections.singletonList(col);
    }

    /** Submit one SeaTunnel job per collection and wait for all to complete. */
    private String migrateMultipleCollections(List<String> collections) {
        List<String> failures = new ArrayList<>();
        java.util.concurrent.ExecutorService executor =
                java.util.concurrent.Executors.newFixedThreadPool(
                        Math.min(collections.size(), config.getParallelism()));
        try {
            java.util.List<java.util.concurrent.Future<String>> futures = new ArrayList<>();
            for (String collection : collections) {
                futures.add(executor.submit(() -> {
                    logger.info(String.format("Starting migration for collection: %s", collection));
                    MigrationConfig perCollectionConfig = MigrationConfig.builder()
                            .milvusUrl(config.getMilvusUrl())
                            .milvusToken(config.getMilvusToken())
                            .milvusDatabase(config.getMilvusDatabase())
                            .milvusCollection(collection)
                            .pgUrl(config.getPgUrl())
                            .pgUser(config.getPgUser())
                            .pgPassword(config.getPgPassword())
                            .pgSchema(config.getPgSchema())
                            .pgTable(collection) // map Milvus collection to pg table of same name
                            .batchSize(config.getBatchSize())
                            .parallelism(1) // single reader per collection when parallelized
                            .rateLimitRowsPerSecond(config.getRateLimitRowsPerSecond())
                            .skipIndexMigration(config.isSkipIndexMigration())
                            .dropExistingTable(config.isDropExistingTable())
                            .enablePgPartition(config.isEnablePgPartition())
                            .validationSampleSize(config.getValidationSampleSize())
                            .passRateThreshold(config.getPassRateThreshold())
                            .auditLogDir(config.getAuditLogDir())
                            .build();
                    try {
                        int exitCode = jobSubmitter.submitAndWait(perCollectionConfig, logger);
                        if (exitCode != 0) {
                            return "FAILED: " + collection + " (exit " + exitCode + ")";
                        }
                        return "OK: " + collection;
                    } catch (Exception e) {
                        return "FAILED: " + collection + " (" + e.getMessage() + ")";
                    }
                }));
            }
            for (java.util.concurrent.Future<String> f : futures) {
                try {
                    String result = f.get();
                    if (result.startsWith("FAILED")) {
                        failures.add(result);
                    }
                    logger.info(String.format("Collection migration result: %s", result));
                } catch (Exception e) {
                    failures.add("FAILED: " + e.getMessage());
                }
            }
        } finally {
            executor.shutdownNow();
        }
        if (!failures.isEmpty()) {
            throw new RuntimeException("Multi-collection migration had failures: " + failures);
        }
        return "SUCCESS (" + collections.size() + " collections)";
    }

    /** Introspect Milvus schema without executing DDL (for resume/validation-only scenarios). */
    private MigrationSchema introspectSchemaOnly() {
        try (MilvusSchemaIntrospector introspector =
                new MilvusSchemaIntrospector(config.getMilvusUrl(), config.getMilvusToken())) {
            return introspector.introspect(config.getMilvusCollection());
        }
    }

    private MigrationReport buildReport(
            Instant startTime,
            String schemaResult,
            String dataResult,
            ValidationReport validationReport,
            boolean success) {
        Instant endTime = Instant.now();
        long durationSeconds = Duration.between(startTime, endTime).getSeconds();
        long rowsMigrated = tracker.getState().getRowsMigrated();

        return MigrationReport.builder()
                .collectionName(config.getMilvusCollection())
                .pgTable(config.getPgTable())
                .startTime(startTime)
                .endTime(endTime)
                .schemaMigrationResult(schemaResult)
                .dataMigrationResult(dataResult)
                .validationReport(validationReport)
                .totalRowsMigrated(rowsMigrated)
                .durationSeconds(durationSeconds)
                .success(success)
                .build();
    }

    private boolean shouldRunSchema() {
        return runSchema;
    }

    private boolean shouldRunData() {
        return runData && jobSubmitter != null;
    }

    private boolean shouldRunValidation() {
        return runValidation;
    }

    private String summarizeConfig() {
        return "collection="
                + config.getMilvusCollection()
                + ", pgTable="
                + config.getPgSchema()
                + "."
                + config.getPgTable()
                + ", batchSize="
                + config.getBatchSize()
                + ", rateLimit="
                + config.getRateLimitRowsPerSecond()
                + ", resume="
                + resume;
    }
}
