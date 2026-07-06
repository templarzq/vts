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

package org.apache.seatunnel.connectors.migration.milvus2pgvector.validation;

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;

import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.config.MigrationConfig;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.exception.MigrationErrorCode;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.exception.MigrationException;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.internal.TokenBucketRateLimiter;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.schema.MigrationSchema;

import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Instant;

/** Orchestrates the validators: record count (fast) and field-level comparison (sampled). */
@Slf4j
public class DataValidator implements AutoCloseable {

    private final MigrationConfig config;
    private final MigrationSchema schema;
    private MilvusClientV2 milvusClient;
    private Connection pgConnection;

    public DataValidator(MigrationConfig config, MigrationSchema schema) {
        this.config = config;
        this.schema = schema;
    }

    public ValidationReport validate() {
        connect();
        ValidationReport report = new ValidationReport();
        report.setCollectionName(schema.getCollectionName());
        report.setPgTable(config.getPgTable());
        report.setTimestamp(Instant.now());

        TokenBucketRateLimiter rateLimiter =
                new TokenBucketRateLimiter(
                        config.getRateLimitRowsPerSecond(), config.getRateLimitAcquireTimeoutSeconds());

        // Phase 1: Record count — fast, answers "did we drop rows?"
        log.info("Running record count validation...");
        ValidationResult countResult =
                new RecordCountValidator(
                                milvusClient,
                                pgConnection,
                                config.getMilvusCollection(),
                                config.getPgSchema(),
                                config.getPgTable())
                        .validate();
        report.addResult(countResult);

        // Phase 2: Field-level comparison — sampled, answers "does the data match?"
        log.info("Running field comparison validation (sample={}, passRateThreshold={})...",
                config.getValidationSampleSize(), config.getPassRateThreshold());
        ValidationResult fieldResult =
                new FieldComparisonValidator(
                                milvusClient,
                                pgConnection,
                                schema,
                                config.getPgSchema(),
                                config.getPgTable(),
                                config.getValidationSampleSize(),
                                config.getPassRateThreshold(),
                                rateLimiter)
                        .validate();
        report.addResult(fieldResult);

        log.info("Validation complete: overall={}", report.isOverallPassed() ? "PASSED" : "FAILED");
        return report;
    }

    private void connect() {
        try {
            ConnectConfig connectConfig =
                    (config.getMilvusToken() != null && !config.getMilvusToken().isEmpty())
                            ? ConnectConfig.builder()
                                    .uri(config.getMilvusUrl())
                                    .token(config.getMilvusToken())
                                    .build()
                            : ConnectConfig.builder().uri(config.getMilvusUrl()).build();
            milvusClient = new MilvusClientV2(connectConfig);
        } catch (Exception e) {
            throw new MigrationException(
                    MigrationErrorCode.VALIDATION_QUERY_FAILED,
                    "Failed to connect to Milvus: " + e.getMessage(),
                    e);
        }
        try {
            Class.forName("org.postgresql.Driver");
            pgConnection =
                    DriverManager.getConnection(
                            config.getPgUrl(), config.getPgUser(), config.getPgPassword());
        } catch (Exception e) {
            throw new MigrationException(
                    MigrationErrorCode.VALIDATION_QUERY_FAILED,
                    "Failed to connect to pgvector: " + e.getMessage(),
                    e);
        }
    }

    @Override
    public void close() {
        try {
            if (milvusClient != null) {
                milvusClient.close();
            }
        } catch (Exception e) {
            log.warn("Failed to close Milvus client", e);
        }
        try {
            if (pgConnection != null && !pgConnection.isClosed()) {
                pgConnection.close();
            }
        } catch (Exception e) {
            log.warn("Failed to close pg connection", e);
        }
    }
}
