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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class MilvusCdcSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String CONNECTOR_IDENTITY = "Milvus-CDC";

    public static final Option<String> URL =
            Options.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Milvus server URL, e.g. \"http://localhost:19530\"");

    public static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .defaultValue("")
                    .withDescription("Authentication token");

    public static final Option<String> DATABASE =
            Options.key("database")
                    .stringType()
                    .defaultValue("default")
                    .withDescription("Milvus database name");

    public static final Option<String> COLLECTION =
            Options.key("collection")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Collection name to capture changes from");

    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Number of records to fetch per query");

    public static final Option<Long> INCREMENTAL_BATCH_SIZE =
            Options.key("incremental_batch_size")
                    .longType()
                    .defaultValue(500L)
                    .withDescription("Max CDC events per incremental poll");

    public static final Option<Long> POLL_INTERVAL_MS =
            Options.key("poll_interval_ms")
                    .longType()
                    .defaultValue(1000L)
                    .withDescription("Polling interval in milliseconds for incremental reads");

    public static final Option<String> STARTUP_MODE =
            Options.key("startup_mode")
                    .stringType()
                    .defaultValue("INITIAL")
                    .withDescription(
                            "Startup mode - \"INITIAL\" (snapshot + incremental) or \"LATEST\" (incremental only)");

    public static final Option<String> CDC_STRATEGY =
            Options.key("cdc_strategy")
                    .stringType()
                    .defaultValue("polling_incremental")
                    .withDescription(
                            "CDC strategy - \"grpc_replicate\" or \"polling_incremental\"");

    public static final Option<String> PRIMARY_KEY_FIELD =
            Options.key("primary_key_field")
                    .stringType()
                    .defaultValue("id")
                    .withDescription(
                            "The primary key field name used as watermark for incremental reads");

    public static final Option<Boolean> FETCH_ALL_FIELDS =
            Options.key("fetch_all_fields")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to fetch all fields");

    public static final Option<Long> CHANNEL_TIMEOUT_MS =
            Options.key("channel_timeout_ms")
                    .longType()
                    .defaultValue(30000L)
                    .withDescription("gRPC channel connect/read timeout in milliseconds");

    public static final Option<String> CLIENT_PEM_PATH =
            Options.key("client_pem_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Path to client certificate PEM file for TLS");

    public static final Option<String> CLIENT_KEY_PATH =
            Options.key("client_key_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Path to client certificate KEY file for TLS");

    public static final Option<String> CA_PEM_PATH =
            Options.key("ca_pem_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Path to CA certificate PEM file for TLS");

    public static final Option<String> SERVER_NAME =
            Options.key("server_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Server name for TLS verification");

    public static final Option<Integer> PARALLELISM =
            Options.key("parallelism")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Reader parallelism (snapshot phase only)");

    private String url;
    private String token;
    private String database;
    private String collection;
    private Integer batchSize;
    private Long incrementalBatchSize;
    private Long pollIntervalMs;
    private String startupMode;
    private String cdcStrategy;
    private String primaryKeyField;
    private Boolean fetchAllFields;
    private Long channelTimeoutMs;
    private String clientPemPath;
    private String clientKeyPath;
    private String caPemPath;
    private String serverName;
    private Integer parallelism;

    public static MilvusCdcSourceConfig of(ReadonlyConfig config) {
        return MilvusCdcSourceConfig.builder()
                .url(config.get(URL))
                .token(config.get(TOKEN))
                .database(config.get(DATABASE))
                .collection(config.get(COLLECTION))
                .batchSize(config.get(BATCH_SIZE))
                .incrementalBatchSize(config.get(INCREMENTAL_BATCH_SIZE))
                .pollIntervalMs(config.get(POLL_INTERVAL_MS))
                .startupMode(config.get(STARTUP_MODE))
                .cdcStrategy(config.get(CDC_STRATEGY))
                .primaryKeyField(config.get(PRIMARY_KEY_FIELD))
                .fetchAllFields(config.get(FETCH_ALL_FIELDS))
                .channelTimeoutMs(config.get(CHANNEL_TIMEOUT_MS))
                .clientPemPath(config.get(CLIENT_PEM_PATH))
                .clientKeyPath(config.get(CLIENT_KEY_PATH))
                .caPemPath(config.get(CA_PEM_PATH))
                .serverName(config.get(SERVER_NAME))
                .parallelism(config.get(PARALLELISM))
                .build();
    }
}
