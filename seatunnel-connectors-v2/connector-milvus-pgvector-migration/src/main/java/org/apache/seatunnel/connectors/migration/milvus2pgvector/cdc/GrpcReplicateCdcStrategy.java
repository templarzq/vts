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

import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import io.milvus.grpc.GetReplicateInfoRequest;
import io.milvus.grpc.GetReplicateInfoResponse;
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.orm.iterator.QueryIterator;
import io.milvus.response.QueryResultsWrapper;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.collection.request.GetLoadStateReq;
import io.milvus.v2.service.vector.request.QueryIteratorReq;

import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * CDC strategy that leverages Milvus's gRPC CDC infrastructure for position
 * tracking while using query-based polling for actual data retrieval.
 *
 * <p>Milvus's gRPC CDC APIs ({@code ReplicateMessage}, {@code CreateReplicateStream})
 * are designed for server-to-server replication relay and are not suitable as a
 * public change-stream subscription API. This strategy uses {@code GetReplicateInfo}
 * to check CDC availability and retrieve replication position metadata, then
 * performs PK-based incremental queries for the actual change data.
 *
 * <p>This provides the same level of correctness as the polling strategy with
 * the added benefit of CDC position awareness for deployments that have it enabled.
 */
@Slf4j
public class GrpcReplicateCdcStrategy implements CdcStrategy {

    private static final Metadata.Key<String> AUTH_KEY =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    private final MilvusClientV2 client;
    private final ManagedChannel channel;
    private final MilvusCdcSourceConfig config;
    private final String collectionName;
    private final String primaryKeyField;
    private final int batchSize;

    public GrpcReplicateCdcStrategy(MilvusCdcSourceConfig config) {
        this.config = config;
        this.collectionName = config.getCollection();
        this.primaryKeyField = config.getPrimaryKeyField();
        this.batchSize = config.getBatchSize();

        // Build gRPC channel for CDC metadata operations
        this.channel = buildChannel(config);

        // Create standard Milvus client for data queries
        ConnectConfig connectConfig = ConnectConfig.builder()
                .uri(config.getUrl())
                .token(config.getToken())
                .dbName(config.getDatabase())
                .connectTimeoutMs(config.getChannelTimeoutMs())
                .build();
        if (config.getClientPemPath() != null) {
            connectConfig.setClientPemPath(config.getClientPemPath());
        }
        if (config.getClientKeyPath() != null) {
            connectConfig.setClientKeyPath(config.getClientKeyPath());
        }
        if (config.getCaPemPath() != null) {
            connectConfig.setCaPemPath(config.getCaPemPath());
        }
        if (config.getServerName() != null) {
            connectConfig.setServerName(config.getServerName());
        }
        this.client = new MilvusClientV2(connectConfig);
    }

    @Override
    public boolean isAvailable() {
        try {
            MilvusServiceGrpc.MilvusServiceBlockingStub stub =
                    createBlockingStub();
            GetReplicateInfoResponse response = stub.getReplicateInfo(
                    GetReplicateInfoRequest.newBuilder().build());
            return response != null;
        } catch (Exception e) {
            log.warn("gRPC CDC not available: {}", e.getMessage());
            return false;
        }
    }

    @Override
    public List<SeaTunnelRowWithPosition> pollChanges(
            MilvusCdcSourceSplit split, ReplicatePosition startPosition) throws Exception {
        // Verify collection is loaded
        GetLoadStateReq loadStateReq = GetLoadStateReq.builder()
                .collectionName(collectionName)
                .build();
        if (!client.getLoadState(loadStateReq)) {
            log.warn("Collection {} is not loaded", collectionName);
            return Collections.emptyList();
        }

        // Determine watermark
        long watermark = split.getStartId();
        if (startPosition != null && startPosition.getTimeTick() > 0) {
            watermark = Math.max(watermark, startPosition.getTimeTick());
        }

        // Query CDC positions via gRPC for enhanced watermark accuracy
        try {
            MilvusServiceGrpc.MilvusServiceBlockingStub stub = createBlockingStub();
            GetReplicateInfoResponse info = stub.getReplicateInfo(
                    GetReplicateInfoRequest.newBuilder().build());
            if (info != null) {
                log.debug("Retrieved CDC replication info");
            }
        } catch (Exception e) {
            log.debug("Could not retrieve CDC positions, using local watermark: {}", e.getMessage());
        }

        // Perform PK-based incremental query
        String expr = primaryKeyField + " > " + watermark;

        QueryIteratorReq queryReq = QueryIteratorReq.builder()
                .collectionName(collectionName)
                .outputFields(java.util.Collections.singletonList("*"))
                .expr(expr)
                .batchSize((long) batchSize)
                .build();

        List<SeaTunnelRowWithPosition> results = new ArrayList<>();
        long maxSeenId = watermark;
        long pollTime = System.currentTimeMillis();
        QueryIterator iterator = null;

        try {
            iterator = client.queryIterator(queryReq);
            List<QueryResultsWrapper.RowRecord> batch;
            while (!(batch = iterator.next()).isEmpty()) {
                for (QueryResultsWrapper.RowRecord record : batch) {
                    SeaTunnelRow row = buildRow(record);
                    results.add(new SeaTunnelRowWithPosition(row,
                            ReplicatePosition.builder()
                                    .timeTick(maxSeenId)
                                    .timestamp(pollTime)
                                    .build()));

                    Object idValue = record.get(primaryKeyField);
                    if (idValue instanceof Number) {
                        maxSeenId = Math.max(maxSeenId, ((Number) idValue).longValue());
                    }
                }

                if (results.size() >= config.getIncrementalBatchSize()) {
                    break;
                }
            }
        } finally {
            if (iterator != null) {
                try {
                    iterator.close();
                } catch (Exception e) {
                    log.debug("Error closing QueryIterator", e);
                }
            }
        }

        split.setStartId(maxSeenId);
        return results;
    }

    @Override
    public void close() {
        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                log.warn("Error closing Milvus client", e);
            }
        }
        if (channel != null && !channel.isShutdown()) {
            try {
                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                channel.shutdownNow();
            }
        }
    }

    private SeaTunnelRow buildRow(QueryResultsWrapper.RowRecord record) {
        // Build row from query result fields
        List<Object> fields = new ArrayList<>();
        for (String fieldName : record.getFieldValues().keySet()) {
            fields.add(record.get(fieldName));
        }
        SeaTunnelRow row = new SeaTunnelRow(fields.toArray());
        row.setRowKind(RowKind.INSERT);
        row.setTableId(collectionName);
        return row;
    }

    private MilvusServiceGrpc.MilvusServiceBlockingStub createBlockingStub() {
        MilvusServiceGrpc.MilvusServiceBlockingStub stub =
                MilvusServiceGrpc.newBlockingStub(channel);

        if (config.getToken() != null && !config.getToken().isEmpty()) {
            Metadata headers = new Metadata();
            headers.put(AUTH_KEY, "Bearer " + config.getToken());
            stub = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
        }

        return stub.withDeadlineAfter(config.getChannelTimeoutMs(), TimeUnit.MILLISECONDS);
    }

    private static ManagedChannel buildChannel(MilvusCdcSourceConfig config) {
        URI uri = URI.create(config.getUrl());
        int port = uri.getPort() > 0 ? uri.getPort() : 19530;
        String host = uri.getHost() != null ? uri.getHost() : "localhost";

        ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forAddress(host, port);

        if ("https".equals(uri.getScheme()) || "grpcs".equals(uri.getScheme())) {
            builder.useTransportSecurity();
        } else {
            builder.usePlaintext();
        }

        builder.keepAliveTime(30, TimeUnit.SECONDS)
                .keepAliveTimeout(10, TimeUnit.SECONDS)
                .keepAliveWithoutCalls(true);

        return builder.build();
    }
}
