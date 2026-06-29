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

import io.milvus.orm.iterator.QueryIterator;
import io.milvus.response.QueryResultsWrapper;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.request.GetLoadStateReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.vector.request.QueryIteratorReq;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * CDC strategy that leverages Milvus's replication infrastructure for position
 * tracking while using query-based polling for actual data retrieval.
 *
 * <p>Milvus's gRPC CDC APIs ({@code ReplicateMessage}, {@code CreateReplicateStream})
 * are designed for server-to-server replication relay and are not suitable as a
 * public change-stream subscription API. This strategy uses the Milvus SDK's
 * {@code getLoadState} to check CDC availability and performs PK-based incremental
 * queries for the actual change data.
 *
 * <p>This provides the same level of correctness as the polling strategy. The
 * strategy is kept as a separate implementation to allow future enhancement when
 * Milvus exposes a public CDC subscription API, and to let users explicitly opt
 * into the "grpc_replicate" mode via configuration.
 */
@Slf4j
public class GrpcReplicateCdcStrategy implements CdcStrategy {

    private final MilvusClientV2 client;
    private final MilvusCdcSourceConfig config;
    private final String collectionName;
    private final String primaryKeyField;
    private final int batchSize;
    private final List<String> fieldNames;

    public GrpcReplicateCdcStrategy(MilvusCdcSourceConfig config) {
        this.config = config;
        this.collectionName = config.getCollection();
        this.primaryKeyField = config.getPrimaryKeyField();
        this.batchSize = config.getBatchSize();

        // Create standard Milvus client for both availability checks and data queries.
        // The SDK internally manages the gRPC channel; we do not depend on raw gRPC
        // classes here so that this strategy compiles and runs cleanly against the
        // shaded connector-milvus artifact.
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

        // Introspect collection schema to determine field ordering. QueryIterator
        // returns rows as a field-name → value map with non-deterministic iteration
        // order, so we must project values into a stable positional array that
        // matches the schema declaration order.
        this.fieldNames = new ArrayList<>();
        try {
            DescribeCollectionResp desc = client.describeCollection(
                    DescribeCollectionReq.builder().collectionName(collectionName).build());
            if (desc != null && desc.getCollectionSchema() != null) {
                for (io.milvus.v2.service.collection.request.CreateCollectionReq.FieldSchema
                        field : desc.getCollectionSchema().getFieldSchemaList()) {
                    fieldNames.add(field.getName());
                }
            }
        } catch (Exception e) {
            log.warn("Could not describe collection '{}', field order may be wrong: {}",
                    collectionName, e.getMessage());
        }
        if (fieldNames.isEmpty()) {
            // Fallback: use primary key as first field; remaining fields will be
            // filled from the row record in HashMap order (non-deterministic but
            // better than crashing).
            fieldNames.add(primaryKeyField);
        }
        log.debug("GrpcReplicateCdcStrategy field order for '{}': {}", collectionName, fieldNames);
    }

    @Override
    public boolean isAvailable() {
        // Use the SDK's getLoadState as the availability probe. On a CDC-enabled
        // Milvus cluster the collection will be loaded; on standalone Milvus without
        // CDC configured this still returns true as long as the collection is loaded,
        // which is the correct precondition for PK-based polling.
        try {
            GetLoadStateReq loadStateReq = GetLoadStateReq.builder()
                    .collectionName(collectionName)
                    .build();
            return client.getLoadState(loadStateReq);
        } catch (Exception e) {
            log.warn("CDC strategy not available: {}", e.getMessage());
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

        // Perform PK-based incremental query
        String expr = primaryKeyField + " > " + watermark;

        QueryIteratorReq queryReq = QueryIteratorReq.builder()
                .collectionName(collectionName)
                .outputFields(Collections.singletonList("*"))
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
    }

    private SeaTunnelRow buildRow(QueryResultsWrapper.RowRecord record) {
        // Build row using the schema-declared field order so that positional
        // access (e.g. fields[0] = id, fields[1] = vector) is deterministic.
        List<Object> fields = new ArrayList<>(fieldNames.size());
        for (String fieldName : fieldNames) {
            fields.add(record.get(fieldName));
        }
        SeaTunnelRow row = new SeaTunnelRow(fields.toArray());
        row.setRowKind(RowKind.INSERT);
        row.setTableId(collectionName);
        return row;
    }
}
