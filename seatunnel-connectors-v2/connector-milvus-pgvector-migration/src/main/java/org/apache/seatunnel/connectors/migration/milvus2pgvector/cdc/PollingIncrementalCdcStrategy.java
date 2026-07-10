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
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.collection.request.GetLoadStateReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
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
            TableSchema tableSchema,
            MilvusClientV2 client,
            String collectionName) {
        this.config = config;
        this.converter = converter;
        this.tableSchema = tableSchema;
        this.collectionName = collectionName;
        this.primaryKeyField = config.getPrimaryKeyField();
        this.client = client;
    }

    /**
     * Ensure the collection is loaded. If not, try to load it and wait for it
     * to become ready (with retries). Retry params from {@link MilvusCdcSourceConfig}.
     */
    private void ensureCollectionLoaded() {
        final int maxRetries = config.getCdcCollectionLoadMaxRetries() != null
                ? config.getCdcCollectionLoadMaxRetries() : 60;
        final long waitMs = config.getCdcCollectionLoadRetryDelayMs() != null
                ? config.getCdcCollectionLoadRetryDelayMs() : 5000L;

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            boolean loaded;
            try {
                loaded = client.getLoadState(
                        GetLoadStateReq.builder().collectionName(collectionName).build());
            } catch (Exception e) {
                log.warn("Failed to check load state for collection '{}' (attempt {}/{}): {}",
                        collectionName, attempt, maxRetries, e.getMessage());
                loaded = false;
            }

            if (loaded) {
                return;
            }

            if (attempt == 1) {
                log.warn("Collection '{}' is not loaded, attempting to load...", collectionName);
                try {
                    client.loadCollection(
                            LoadCollectionReq.builder().collectionName(collectionName).build());
                    log.info("Load request sent for collection '{}'", collectionName);
                } catch (Exception e) {
                    log.warn("Load collection request failed for '{}': {}. Will retry...",
                            collectionName, e.getMessage());
                }
            }

            if (attempt >= maxRetries) {
                break;
            }

            try {
                Thread.sleep(waitMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while waiting for collection '{}' to load, giving up",
                        collectionName);
                return;
            }
        }

        log.warn("Collection '{}' still not loaded after {} retries, polling will retry on next cycle",
                collectionName, maxRetries);
    }

    @Override
    public List<SeaTunnelRowWithPosition> pollChanges(
            MilvusCdcSourceSplit split, ReplicatePosition startPosition) throws Exception {
        // Auto-reload collection if it was released during migration
        ensureCollectionLoaded();

        // Determine the watermark — support both numeric and string PKs.
        // Numeric PK: watermark stored in startPosition.timeTick and split.startId.
        // String PK:  watermark stored in startPosition.messageId.
        long numWatermark = split.getStartId();
        String strWatermark = null;
        if (startPosition != null) {
            if (startPosition.getTimeTick() > 0) {
                numWatermark = Math.max(numWatermark, startPosition.getTimeTick());
            }
            if (startPosition.getMessageId() != null && !startPosition.getMessageId().isEmpty()) {
                strWatermark = startPosition.getMessageId();
            }
        }

        // Build the filter expression based on the watermark type
        String expr;
        boolean isStringPk = strWatermark != null;
        if (isStringPk) {
            // String PK: use string comparison
            expr = primaryKeyField + " > '" + strWatermark.replace("'", "\\'") + "'";
        } else {
            // Numeric PK: use numeric comparison
            expr = primaryKeyField + " > " + numWatermark;
        }

        QueryIteratorReq queryReq = QueryIteratorReq.builder()
                .collectionName(collectionName)
                .outputFields(java.util.Collections.singletonList("*"))
                .expr(expr)
                .batchSize(config.getBatchSize().longValue())
                .build();

        List<SeaTunnelRowWithPosition> results = new ArrayList<>();
        long maxSeenNumId = numWatermark;
        String maxSeenStrId = strWatermark;
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

                    Object idValue = record.get(primaryKeyField);
                    ReplicatePosition pos = ReplicatePosition.builder()
                            .timeTick(maxSeenNumId)
                            .timestamp(pollTime)
                            .build();
                    if (idValue instanceof Number) {
                        maxSeenNumId = Math.max(maxSeenNumId, ((Number) idValue).longValue());
                        pos.setTimeTick(maxSeenNumId);
                    } else if (idValue != null) {
                        // String/VARCHAR PK: track the maximum string value
                        String idStr = idValue.toString();
                        if (maxSeenStrId == null || idStr.compareTo(maxSeenStrId) > 0) {
                            maxSeenStrId = idStr;
                        }
                        pos.setMessageId(maxSeenStrId);
                    }
                    results.add(new SeaTunnelRowWithPosition(row, pos));
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

        split.setStartId(maxSeenNumId);
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
