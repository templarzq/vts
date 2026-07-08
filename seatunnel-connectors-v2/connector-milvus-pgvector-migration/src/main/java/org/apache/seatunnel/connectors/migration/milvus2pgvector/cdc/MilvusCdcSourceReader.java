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
import org.apache.seatunnel.connectors.migration.milvus2pgvector.cdc.streaming.CdcEventStreamStrategyV2;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.cdc.streaming.CdcEventStreamStrategyV2;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.utils.MilvusConnectorUtils;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.internal.TokenBucketRateLimiter;
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
import java.util.HashMap;
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
    private TokenBucketRateLimiter rateLimiter;
    private volatile boolean noMoreSplit;
    private volatile boolean snapshotPhase = true;
    private volatile boolean snapStartCaptured;

    /** Per-split FAILED_PRECONDITION tracking for exponential backoff. */
    private final Map<String, FailRecord> failRecords = new HashMap<>();
    private static final long PRECONDITION_BACKOFF_WINDOW_MS = 30_000;

    private static class FailRecord {
        long firstFailTime;
        int failCount;
        FailRecord(long time) { this.firstFailTime = time; this.failCount = 1; }
    }

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
        // SSLContext initialization in MilvusConnectorUtils is guarded to run
        // only once (sslContextInitialized flag). Each reader can safely create
        // its own MilvusClientV2 in parallel.
        this.client = new MilvusClientV2(MilvusConnectorUtils.getConnectConfig(config));

        // Initialize rate limiter
        int rateLimit = cdcConfig.getCdcRateLimitRowsPerSecond() != null
                ? cdcConfig.getCdcRateLimitRowsPerSecond() : 0;
        if (rateLimit > 0) {
            this.rateLimiter = new TokenBucketRateLimiter(rateLimit, 60);
            log.info("Rate limiter enabled: {} rows/second", rateLimit);
        }

        // Introspect the target collection schema
        CatalogTable catalogTable = sourceTables.values().iterator().next();
        this.tableSchema = catalogTable.getTableSchema();
        this.converter = new MilvusSourceConverter(tableSchema);

        // Resolve collection name: use singular 'collection' if set,
        // otherwise fall back to effective collection (first in 'collections' list)
        String colName = cdcConfig.getCollection();
        if (colName == null || colName.isEmpty()) {
            colName = cdcConfig.getEffectiveCollection();
        }
        // Describe collection for field-level metadata
        this.collectionDesc = client.describeCollection(
                DescribeCollectionReq.builder()
                        .collectionName(colName)
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
        java.util.function.IntConsumer limiter = rateLimiter != null
                ? rateLimiter::acquire : null;
        MilvusBufferReader bufferReader = new MilvusBufferReader(
                sourceSplit, output, client, tableSchema, limiter);
        bufferReader.pollData(cdcConfig.getBatchSize());

        // Capture SnapStartPosition AFTER snapshot completes (at-least-once semantics).
        // This ensures all snapshot data is written before we record the incremental
        // start point, avoiding data loss from WAL GC while snapshot was running.
        // Each reader captures once; the enumerator takes the first position received.
        if (!snapStartCaptured && "INITIAL".equalsIgnoreCase(cdcConfig.getStartupMode())) {
            snapStartCaptured = true;
            String colName = cdcConfig.getEffectiveCollection();
            if (colName != null) {
                ReplicatePosition snapPos = cdcStrategy.captureCurrentPosition(colName);
                if (snapPos != null) {
                    context.sendSourceEventToEnumerator(
                            new SnapStartPositionEvent(colName, snapPos));
                }
            }
        }

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

        try {
            List<SeaTunnelRowWithPosition> events =
                    cdcStrategy.pollChanges(split, startPosition);

            // Successful poll — clear any FAILED_PRECONDITION backoff state
            failRecords.remove(split.splitId());

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

        } catch (io.grpc.StatusRuntimeException e) {
            io.grpc.Status.Code code = e.getStatus().getCode();
            if (code == io.grpc.Status.Code.FAILED_PRECONDITION) {
                // Connection-level error (term change, channel restart).
                // Use exponential backoff within a 30s window before escalating to INITIAL.
                long now = System.currentTimeMillis();
                FailRecord fr = failRecords.computeIfAbsent(split.splitId(),
                        k -> new FailRecord(now));
                if (now - fr.firstFailTime < PRECONDITION_BACKOFF_WINDOW_MS) {
                    long delay = Math.min(1000L << fr.failCount,
                            PRECONDITION_BACKOFF_WINDOW_MS);
                    fr.failCount++;
                    log.warn("Consume stream FAILED_PRECONDITION for split={} (count={}), "
                            + "re-queuing with {}ms backoff",
                            split.splitId(), fr.failCount, delay);
                    Thread.sleep(delay);
                    pendingSplits.addFirst(split);
                    return;
                }
                // Backoff window exhausted — escalate to INITIAL recovery
                log.warn("FAILED_PRECONDITION persisted beyond {}ms for split={}, "
                        + "triggering INITIAL recovery",
                        PRECONDITION_BACKOFF_WINDOW_MS, split.splitId());
                failRecords.remove(split.splitId());
                if (Boolean.TRUE.equals(cdcConfig.getCdcAutoRecoverStalePosition())) {
                    context.sendSourceEventToEnumerator(
                            new CheckpointInvalidatedEvent(split.splitId(),
                                    "FAILED_PRECONDITION persisted: " + e.getMessage()));
                } else {
                    throw e;
                }
                return;
            }
            // NOT_FOUND / INVALID_ARGUMENT: data-level error — WAL position GC'd.
            // Trigger INITIAL recovery immediately.
            if (code == io.grpc.Status.Code.NOT_FOUND
                    || code == io.grpc.Status.Code.INVALID_ARGUMENT) {
                if (Boolean.TRUE.equals(cdcConfig.getCdcAutoRecoverStalePosition())) {
                    log.warn("CDC position stale ({}: {}), triggering INITIAL recovery",
                            code, e.getMessage());
                    context.sendSourceEventToEnumerator(
                            new CheckpointInvalidatedEvent(split.splitId(),
                                    "WAL position no longer valid: " + e.getMessage()));
                } else {
                    log.error("CDC position stale ({}: {}) and auto-recovery is disabled",
                            code, e.getMessage());
                    throw e;
                }
            } else {
                throw e;
            }
        } catch (RuntimeException e) {
            String msg = e.getMessage();
            if (msg != null && msg.contains("recovery exceeded max retries")) {
                if (Boolean.TRUE.equals(cdcConfig.getCdcAutoRecoverStalePosition())) {
                    log.warn("CDC recovery exhausted, triggering INITIAL recovery: {}", msg);
                    context.sendSourceEventToEnumerator(
                            new CheckpointInvalidatedEvent(split.splitId(),
                                    "Recovery retries exhausted: " + msg));
                } else {
                    log.error("CDC recovery exhausted and auto-recovery is disabled: {}", msg);
                    throw e;
                }
            } else {
                throw e;
            }
        }
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
     * Falls back to polling incremental if the requested strategy is not available.
     */
    private CdcStrategy createCdcStrategy() {
        String strategyType = cdcConfig.getCdcStrategy();
        // Derive tableId from source table path (same format as snapshot phase),
        // so that CDC DELETE rows match snapshot INSERT rows in sink-side buffers.
        String tableId = sourceTables.keySet().iterator().next().toString();

        if ("event_stream".equalsIgnoreCase(strategyType)) {
            if (Boolean.TRUE.equals(cdcConfig.getCdcUseStreamingNode())) {
                // V2 strategy: StreamingNode gRPC (works in standalone + cluster mode).
                // cdc_pchannel is optional — auto-discovery from etcd + 0-15 scan.
                CdcEventStreamStrategyV2 v2Strategy = new CdcEventStreamStrategyV2(
                        cdcConfig, collectionDesc, tableId);
                if (v2Strategy.isAvailable()) {
                    log.info("Using event_stream V2 CDC strategy via StreamingNode gRPC");
                    return v2Strategy;
                }
                log.warn("event_stream V2 (StreamingNode) not available. Falling back to "
                        + "polling_incremental. Note: polling_incremental cannot capture "
                        + "deletes or same-PK updates.");
                try {
                    v2Strategy.close();
                } catch (Exception ex) {
                    log.debug("Error closing unavailable V2 strategy", ex);
                }
            } else {
                log.warn("cdc_use_streaming_node=false is deprecated — V2 is the only "
                        + "recommended event_stream mode. Falling back to polling_incremental.");
            }
        }

        // grpc_replicate and legacy event_stream V1 are deprecated (ADR-0001).
        // They are no longer instantiated — fall through to polling_incremental.

        log.info("Using polling incremental CDC strategy");
        return new PollingIncrementalCdcStrategy(cdcConfig, converter, tableSchema, client);
    }

}
