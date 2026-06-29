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

import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.milvus.source.utils.MilvusSourceConverter;

import io.milvus.orm.iterator.QueryIterator;
import io.milvus.response.QueryResultsWrapper;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.collection.request.GetLoadStateReq;
import io.milvus.v2.service.vector.request.QueryIteratorReq;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * CDC strategy that detects incremental changes by polling Milvus with a primary-key
 * based filter expression using {@link QueryIterator}.
 */
@Slf4j
public class PollingIncrementalCdcStrategy implements CdcStrategy {

    private final MilvusClientV2 client;
    private final MilvusCdcSourceConfig config;
    private final MilvusSourceConverter converter;
    private final TableSchema tableSchema;
    private final String collectionName;
    private final String primaryKeyField;

    public PollingIncrementalCdcStrategy(
            MilvusCdcSourceConfig config,
            MilvusSourceConverter converter,
            TableSchema tableSchema) {
        this.config = config;
        this.converter = converter;
        this.tableSchema = tableSchema;
        this.collectionName = config.getCollection();
        this.primaryKeyField = config.getPrimaryKeyField();

        ConnectConfig connectConfig = ConnectConfig.builder()
                .uri(config.getUrl())
                .token(config.getToken())
                .dbName(config.getDatabase())
                .connectTimeoutMs(config.getChannelTimeoutMs())
                .build();
        applyTlsConfig(connectConfig);
        this.client = new MilvusClientV2(connectConfig);
    }

    private void applyTlsConfig(ConnectConfig connectConfig) {
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
    }

    @Override
    public List<SeaTunnelRowWithPosition> pollChanges(
            MilvusCdcSourceSplit split, ReplicatePosition startPosition) throws Exception {
        GetLoadStateReq loadStateReq = GetLoadStateReq.builder()
                .collectionName(collectionName)
                .build();
        if (!client.getLoadState(loadStateReq)) {
            log.warn("Collection {} is not loaded, cannot poll for changes", collectionName);
            return Collections.emptyList();
        }

        long watermark = split.getStartId();
        if (startPosition != null && startPosition.getTimeTick() > 0) {
            watermark = Math.max(watermark, startPosition.getTimeTick());
        }

        String expr = primaryKeyField + " > " + watermark;

        QueryIteratorReq queryReq = QueryIteratorReq.builder()
                .collectionName(collectionName)
                .outputFields(java.util.Collections.singletonList("*"))
                .expr(expr)
                .batchSize(config.getBatchSize().longValue())
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
                    SeaTunnelRow row = converter.convertToSeaTunnelRow(
                            record, tableSchema, collectionName, null);
                    row.setRowKind(RowKind.INSERT);
                    row.setTableId(collectionName);

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
}
