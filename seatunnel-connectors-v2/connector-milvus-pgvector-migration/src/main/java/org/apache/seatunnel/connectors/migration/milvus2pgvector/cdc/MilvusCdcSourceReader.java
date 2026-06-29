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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.utils.MilvusConnectorUtils;
import org.apache.seatunnel.connectors.seatunnel.milvus.source.MilvusBufferReader;
import org.apache.seatunnel.connectors.seatunnel.milvus.source.MilvusSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.milvus.source.utils.MilvusSourceConverter;

import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Source reader for Milvus CDC. Operates in two phases:
 *
 * <ol>
 *   <li><b>Snapshot phase:</b> Uses {@link MilvusBufferReader} to perform a full
 *       table scan of the collection using {@code queryIterator}.</li>
 *   <li><b>Incremental phase:</b> Uses a pluggable {@link CdcStrategy} to
 *       continuously poll for changes. The incremental split is re-queued after
 *       each poll to keep the stream alive indefinitely.</li>
 * </ol>
 *
 * <p>In {@link Boundedness#UNBOUNDED} mode, the reader never signals
 * {@code noMoreElement} — it keeps the incremental split alive and continues
 * polling until the job is stopped.
 */
@Slf4j
public class MilvusCdcSourceReader implements SourceReader<SeaTunnelRow, MilvusCdcSourceSplit> {

    private final Deque<MilvusCdcSourceSplit> pendingSplits = new ConcurrentLinkedDeque<>();
    private final ReadonlyConfig config;
    private final MilvusCdcSourceConfig cdcConfig;
    private final Context context;
    private final Map<TablePath, CatalogTable> sourceTables;

    private MilvusClientV2 client;
    private CdcStrategy cdcStrategy;
    private MilvusSourceConverter converter;
    private DescribeCollectionResp collectionDesc;
    private TableSchema tableSchema;
    private volatile boolean noMoreSplit;
    private volatile boolean snapshotPhase = true;

    public MilvusCdcSourceReader(
            Context context,
            ReadonlyConfig config,
            MilvusCdcSourceConfig cdcConfig,
            Map<TablePath, CatalogTable> sourceTables) {
        this.context = context;
        this.config = config;
        this.cdcConfig = cdcConfig;
        this.sourceTables = sourceTables;
    }

    @Override
    public void open() throws Exception {
        this.client = new MilvusClientV2(MilvusConnectorUtils.getConnectConfig(config));

        // Introspect the target collection schema
        CatalogTable catalogTable = sourceTables.values().iterator().next();
        this.tableSchema = catalogTable.getTableSchema();
        this.converter = new MilvusSourceConverter(tableSchema);

        // Describe collection for field-level metadata
        this.collectionDesc = client.describeCollection(
                DescribeCollectionReq.builder()
                        .collectionName(cdcConfig.getCollection())
                        .build());

        // Initialize the CDC strategy
        this.cdcStrategy = createCdcStrategy();
        log.info("MilvusCdcSourceReader opened. Strategy: {}, Collection: {}",
                cdcStrategy.getClass().getSimpleName(), cdcConfig.getCollection());
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            MilvusCdcSourceSplit split = pendingSplits.poll();
            if (split == null) {
                if (noMoreSplit && pendingSplits.isEmpty()) {
                    // In UNBOUNDED mode, we do NOT signal noMoreElement —
                    // the engine keeps calling pollNext indefinitely
                }
                return;
            }

            if (split.isSnapshot()) {
                readSnapshotSplit(split, output);
            } else {
                readIncrementalSplit(split, output);
            }
        }
    }

    // ---- Snapshot phase ----

    private void readSnapshotSplit(MilvusCdcSourceSplit split, Collector<SeaTunnelRow> output)
            throws Exception {
        log.info("Reading snapshot split: {} (offset={}, limit={})",
                split.splitId(), split.getOffset(), split.getLimit());

        // Convert to MilvusSourceSplit so we can reuse MilvusBufferReader
        MilvusSourceSplit sourceSplit = toMilvusSourceSplit(split);
        MilvusBufferReader bufferReader = new MilvusBufferReader(
                sourceSplit, output, client, tableSchema);
        bufferReader.pollData(cdcConfig.getBatchSize());

        // Notify enumerator that this snapshot split is complete
        context.sendSourceEventToEnumerator(
                new SnapshotCompletedEvent(split.splitId()));

        log.info("Snapshot split {} completed", split.splitId());
    }

    // ---- Incremental phase ----

    private void readIncrementalSplit(MilvusCdcSourceSplit split, Collector<SeaTunnelRow> output)
            throws Exception {
        // Transition from snapshot mode if needed
        if (snapshotPhase) {
            snapshotPhase = false;
            log.info("Transitioning to incremental phase");
        }

        ReplicatePosition startPosition = split.getStartPosition();

        List<SeaTunnelRowWithPosition> events =
                cdcStrategy.pollChanges(split, startPosition);

        for (SeaTunnelRowWithPosition rowPos : events) {
            output.collect(rowPos.getRow());
        }

        // Update the split's start position for the next poll
        if (!events.isEmpty()) {
            SeaTunnelRowWithPosition lastEvent = events.get(events.size() - 1);
            split.setStartPosition(lastEvent.getPosition());

            // Report position to enumerator for checkpointing
            context.sendSourceEventToEnumerator(
                    new IncrementalPositionEvent(split.splitId(), lastEvent.getPosition()));
        }

        // Re-queue the incremental split to keep the stream alive
        pendingSplits.addFirst(split);
    }

    // ---- Lifecycle ----

    @Override
    public void addSplits(List<MilvusCdcSourceSplit> splits) {
        log.debug("Received {} splits", splits.size());
        pendingSplits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        log.info("No more splits to receive");
        this.noMoreSplit = true;
    }

    @Override
    public List<MilvusCdcSourceSplit> snapshotState(long checkpointId) {
        // Return all pending splits for checkpoint serialization.
        // The incremental split carries its latest startPosition which is
        // used to resume from the correct point after recovery.
        return new ArrayList<>(pendingSplits);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // No-op: checkpoints are acknowledged asynchronously by the engine
    }

    @Override
    public void close() throws IOException {
        if (cdcStrategy != null) {
            try {
                cdcStrategy.close();
            } catch (Exception e) {
                log.warn("Error closing CDC strategy", e);
            }
        }
        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                log.warn("Error closing Milvus client", e);
            }
        }
        log.info("MilvusCdcSourceReader closed");
    }

    // ---- Private helpers ----

    /**
     * Convert a MilvusCdcSourceSplit to the format expected by MilvusBufferReader.
     */
    private MilvusSourceSplit toMilvusSourceSplit(MilvusCdcSourceSplit cdcSplit) {
        TablePath tablePath = sourceTables.keySet().iterator().next();
        return MilvusSourceSplit.builder()
                .splitId(cdcSplit.splitId())
                .collectionName(cdcSplit.getCollectionName())
                .partitionName(cdcSplit.getPartitionName())
                .offset(cdcSplit.getOffset())
                .limit(cdcSplit.getLimit())
                .tablePath(tablePath)
                .build();
    }

    /**
     * Create the appropriate CDC strategy based on configuration.
     * Falls back to polling incremental if the gRPC strategy is not available.
     */
    private CdcStrategy createCdcStrategy() {
        String strategyType = cdcConfig.getCdcStrategy();

        if ("grpc_replicate".equalsIgnoreCase(strategyType)) {
            GrpcReplicateCdcStrategy grpcStrategy =
                    new GrpcReplicateCdcStrategy(cdcConfig);
            if (grpcStrategy.isAvailable()) {
                log.info("Using gRPC ReplicateMessage CDC strategy");
                return grpcStrategy;
            }
            log.warn("gRPC CDC not available, falling back to polling incremental strategy");
            try {
                grpcStrategy.close();
            } catch (Exception e) {
                log.debug("Error closing unavailable gRPC strategy", e);
            }
        }

        log.info("Using polling incremental CDC strategy");
        return new PollingIncrementalCdcStrategy(cdcConfig, converter, tableSchema);
    }
}
