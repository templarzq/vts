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
import org.apache.seatunnel.connectors.milvus2pgvector.exception.MigrationException;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.internal.TokenBucketRateLimiter;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.schema.MigrationSchema;

import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Instant;

/** Orchestrates the three validators: record count, vector similarity, and sampling. */
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

        log.info("Running vector similarity validation (sample={})...", config.getValidationSampleSize());
        ValidationResult simResult =
                new VectorSimilarityValidator(
                                milvusClient,
                                pgConnection,
                                schema,
                                config.getPgSchema(),
                                config.getPgTable(),
                                config.getValidationSampleSize(),
                                config.getSimilarityThreshold(),
                                rateLimiter)
                        .validate();
        report.addResult(simResult);

        log.info("Running sampling validation (sample={})...", config.getValidationSampleSize());
        ValidationResult sampleResult =
                new SamplingValidator(
                                milvusClient,
                                pgConnection,
                                schema,
                                config.getPgSchema(),
                                config.getPgTable(),
                                config.getValidationSampleSize(),
                                rateLimiter)
                        .validate();
        report.addResult(sampleResult);

        log.info("Validation complete: overall={}", report.isOverallPassed() ? "PASSED" : "FAILED");
        return report;
    }

    private void connect() {
        try {
            ConnectConfig.Builder builder = ConnectConfig.builder().uri(config.getMilvusUrl());
            if (config.getMilvusToken() != null && !config.getMilvusToken().isEmpty()) {
                builder.token(config.getMilvusToken());
            }
            milvusClient = new MilvusClientV2(builder.build());
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
