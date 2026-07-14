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
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.utils.MilvusConnectorUtils;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.schema.AutoCreateTableHelper;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.schema.PgVectorSchemaGenerator;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.internal.TokenBucketRateLimiter;
import org.apache.seatunnel.connectors.seatunnel.milvus.source.MilvusBufferReader;
import org.apache.seatunnel.connectors.seatunnel.milvus.source.MilvusSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.milvus.source.utils.MilvusSourceConverter;

import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.partition.request.ListPartitionsReq;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
    /** Per-collection CDC strategies — one CdcEventStreamStrategyV2 per collection. */
    private final Map<String, CdcStrategy> strategies = new ConcurrentHashMap<>();
    /** Per-collection schema descriptors. */
    private final Map<String, DescribeCollectionResp> collectionDescs = new ConcurrentHashMap<>();
    private TableSchema tableSchema;
    private TokenBucketRateLimiter rateLimiter;
    private boolean enablePgPartition;
    private volatile boolean noMoreSplit;
    private volatile boolean snapshotPhase = true;
    /** Track which collections have captured SnapStartPosition. */
    private final java.util.Set<String> snapStartCaptured = ConcurrentHashMap.newKeySet();

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

        // Use the first catalog table that matches a configured collection for the snapshot converter
        this.tableSchema = sourceTables.values().stream()
                .filter(ct -> cdcConfig.shouldSyncCollection(ct.getTableId().getTableName()))
                .findFirst()
                .map(CatalogTable::getTableSchema)
                .orElseGet(() -> sourceTables.values().iterator().next().getTableSchema());
        this.enablePgPartition = cdcConfig.isEnablePgPartition();
        if (enablePgPartition) {
            log.info("Partition mode enabled. '{}' column will be added by transform.",
                    PgVectorSchemaGenerator.PARTITION_COLUMN_NAME);
        }

        // Initialize per-collection descriptors, converters and strategies
        for (Map.Entry<TablePath, CatalogTable> entry : sourceTables.entrySet()) {
            String collectionName = entry.getValue().getTableId().getTableName();
            if (!cdcConfig.shouldSyncCollection(collectionName)) {
                continue;
            }
            // Describe collection for field-level metadata.
            // If the collection has been dropped (e.g. during CDC runtime),
            // skip it gracefully instead of crashing the job. Data already
            // synced to the sink remains intact.
            DescribeCollectionResp desc;
            try {
                desc = client.describeCollection(
                        DescribeCollectionReq.builder()
                                .collectionName(collectionName)
                                .build());
            } catch (Exception e) {
                String msg = e.getMessage() == null ? "" : e.getMessage().toLowerCase();
                if (msg.contains("can't find collection") || msg.contains("not found")
                        || msg.contains("doesn't exist") || msg.contains("not exist")) {
                    log.warn("Collection '{}' not found (possibly dropped), "
                            + "skipping CDC for this collection. "
                            + "Data already synced to sink remains intact.", collectionName);
                    continue;
                }
                throw e;
            }
            collectionDescs.put(collectionName, desc);

            // Create and validate CDC strategy for this collection
            CdcStrategy strategy = createCdcStrategy(collectionName, desc);
            if (strategy != null) {
                strategies.put(collectionName, strategy);
                log.info("CDC strategy initialized for collection '{}': {}",
                        collectionName, strategy.getClass().getSimpleName());
            }
        }
        log.info("MilvusCdcSourceReader opened. {} strategies, {} collections",
                strategies.size(), collectionDescs.size());
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
        String collectionName = split.getCollectionName();
        log.info("Reading snapshot split: {} (collection={}, offset={}, limit={})",
                split.splitId(), collectionName, split.getOffset(), split.getLimit());

        // Re-describe the collection to get the latest schema.
        // This handles two cases:
        //   1. Initial snapshot — same as cached, no side-effect
        //   2. Recovery snapshot after drop+recreate — fetches the new collectionId/schema
        boolean collectionRecreated = false;
        DescribeCollectionResp latestDesc = null;
        try {
            latestDesc = client.describeCollection(
                    DescribeCollectionReq.builder().collectionName(collectionName).build());
            // Check if the collectionId changed (indicates drop+recreate).
            // Using java.util.Objects.equals for null-safe comparison.
            DescribeCollectionResp cachedDesc = collectionDescs.get(collectionName);
            if (cachedDesc != null && latestDesc != null
                    && !java.util.Objects.equals(cachedDesc.getCollectionID(), latestDesc.getCollectionID())) {
                collectionRecreated = true;
                log.info("Collection '{}' was recreated: old collectionId={}, new collectionId={}",
                        collectionName, cachedDesc.getCollectionID(), latestDesc.getCollectionID());
                // Clear SnapStartPosition tracking so we capture a fresh position
                snapStartCaptured.remove(collectionName);
            }
            collectionDescs.put(collectionName, latestDesc);
        } catch (Exception e) {
            String msg = e.getMessage() == null ? "" : e.getMessage().toLowerCase();
            if (msg.contains("can't find collection") || msg.contains("not found")
                    || msg.contains("doesn't exist") || msg.contains("not exist")) {
                log.warn("Collection '{}' not found during snapshot, skipping split {}",
                        collectionName, split.splitId());
                return;
            }
            log.warn("Failed to re-describe collection '{}', using cached description: {}",
                    collectionName, e.getMessage());
            latestDesc = collectionDescs.get(collectionName);
        }

        // Update table schema if the collection was recreated or not yet cached
        if (latestDesc != null && (collectionRecreated || tableSchema == null)) {
            try {
                org.apache.seatunnel.connectors.seatunnel.milvus.source.utils.MilvusSourceConnectorUtils utils =
                        new org.apache.seatunnel.connectors.seatunnel.milvus.source.utils.MilvusSourceConnectorUtils(config);
                java.util.Map<TablePath, CatalogTable> tables = utils.getTables();
                if (!tables.isEmpty()) {
                    // Look up the matching table for this collection
                    for (CatalogTable ct : tables.values()) {
                        if (collectionName.equals(ct.getTableId().getTableName())) {
                            this.tableSchema = ct.getTableSchema();
                            log.info("Updated table schema for collection '{}' after recreate",
                                    collectionName);
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                log.warn("Failed to refresh table schema for collection '{}': {}",
                        collectionName, e.getMessage());
            }
        }

        // Auto-create (or recreate) target table from Milvus collection schema.
        // If we detected the collection was recreated (collectionId changed), drop
        // the old PG table and recreate from scratch to match the new schema.
        if (latestDesc != null) {
            boolean dropExisting = collectionRecreated;
            List<String> partitionNames = Collections.emptyList();
            if (enablePgPartition) {
                try {
                    partitionNames = client.listPartitions(
                            ListPartitionsReq.builder().collectionName(collectionName).build());
                } catch (Exception e) {
                    log.warn("Failed to list partitions for '{}': {}", collectionName, e.getMessage());
                }
            }
            try {
                AutoCreateTableHelper.ensureTable(config, latestDesc,
                        cdcConfig.getSinkJdbcUrl(), dropExisting,
                        enablePgPartition, partitionNames);
            } catch (Exception e) {
                log.warn("Auto-create/recreate table failed for collection '{}': {}",
                        collectionName, e.getMessage());
            }
        }

        // Use the original table schema for the snapshot converter.
        // __partition_name column is added later by the MilvusToPgVectorTransform.
        TableSchema colSchema = tableSchema;

        // Convert to MilvusSourceSplit so we can reuse MilvusBufferReader
        MilvusSourceSplit sourceSplit = toMilvusSourceSplit(split);
        java.util.function.IntConsumer limiter = rateLimiter != null
                ? rateLimiter::acquire : null;
        MilvusBufferReader bufferReader = new MilvusBufferReader(
                sourceSplit, output, client, colSchema, limiter);
        // Configure collection load retry from CDC config
        if (cdcConfig.getCdcCollectionLoadMaxRetries() != null) {
            bufferReader.setLoadRetryConfig(
                    cdcConfig.getCdcCollectionLoadMaxRetries(),
                    cdcConfig.getCdcCollectionLoadRetryDelayMs() != null
                            ? cdcConfig.getCdcCollectionLoadRetryDelayMs() : 5000L);
        }
        bufferReader.pollData(cdcConfig.getBatchSize());

        // Capture SnapStartPosition AFTER snapshot completes (at-least-once semantics).
        // Track per-collection to cover multi-collection scenarios.
        if (!snapStartCaptured.contains(collectionName)
                && "INITIAL".equalsIgnoreCase(cdcConfig.getStartupMode())) {
            CdcStrategy strategy = strategies.get(collectionName);
            if (strategy != null) {
                ReplicatePosition snapPos = strategy.captureCurrentPosition(collectionName);
                if (snapPos != null) {
                    snapStartCaptured.add(collectionName);
                    context.sendSourceEventToEnumerator(
                            new SnapStartPositionEvent(collectionName, snapPos));
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

        // Look up the per-collection strategy (may have been closed during recovery).
        // Re-create it with the latest collection description if needed.
        String collectionName = split.getCollectionName();
        CdcStrategy strategy = strategies.get(collectionName);
        if (strategy == null) {
            // Try to re-create the strategy with the latest collection description
            DescribeCollectionResp desc = collectionDescs.get(collectionName);
            if (desc != null) {
                log.info("Re-creating CDC strategy for collection '{}' after recovery", collectionName);
                strategy = createCdcStrategy(collectionName, desc);
                if (strategy != null) {
                    strategies.put(collectionName, strategy);
                }
            }
            if (strategy == null) {
                log.warn("No CDC strategy found for collection '{}', skipping split {}",
                        collectionName, split.splitId());
                return;
            }
        }

        ReplicatePosition startPosition = split.getStartPosition();

        try {
            List<SeaTunnelRowWithPosition> events =
                    strategy.pollChanges(split, startPosition);

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

            // Re-queue the incremental split at the TAIL (not head) to ensure
            // fair round-robin across all collections. addFirst would starve
            // other splits because the same split is always re-polled first.
            pendingSplits.addLast(split);

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
                    pendingSplits.addLast(split);
                    return;
                }
                // Backoff window exhausted — escalate to INITIAL recovery
                log.warn("FAILED_PRECONDITION persisted beyond {}ms for split={}, "
                        + "triggering INITIAL recovery",
                        PRECONDITION_BACKOFF_WINDOW_MS, split.splitId());
                failRecords.remove(split.splitId());
                // Close the old strategy so the next incremental re-creates fresh
                closeStrategy(collectionName);
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
            // NOT_FOUND typically indicates the collection was dropped while streaming:
            // the vchannel or collection no longer exists in the WAL.
            if (code == io.grpc.Status.Code.NOT_FOUND
                    || code == io.grpc.Status.Code.INVALID_ARGUMENT) {
                // Close the old strategy so the next incremental re-creates fresh
                closeStrategy(collectionName);
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
        for (Map.Entry<String, CdcStrategy> entry : strategies.entrySet()) {
            try {
                entry.getValue().close();
            } catch (Exception e) {
                log.warn("Error closing CDC strategy for collection '{}'", entry.getKey(), e);
            }
        }
        strategies.clear();
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
     * Remove the CDC strategy for a given collection from the active set.
     * Called during error recovery when the connection is broken or the collection
     * may have been dropped and recreated.
     *
     * <p>Note: we intentionally do NOT call {@code oldStrategy.close()} here because:
     * <ul>
     *   <li>The {@link PollingIncrementalCdcStrategy} shares the reader's {@link #client}
     *       — calling close() would break the shared instance.</li>
     *   <li>The {@link CdcEventStreamStrategyV2} streams are already broken (that's why
     *       the error handler triggered recovery); the GC will clean up.</li>
     * </ul>
     */
    private void closeStrategy(String collectionName) {
        CdcStrategy oldStrategy = strategies.remove(collectionName);
        if (oldStrategy != null) {
            log.info("Removed CDC strategy for collection '{}' (will be re-created on next incremental split)",
                    collectionName);
        }
    }

    /**
     * Convert a MilvusCdcSourceSplit to the format expected by MilvusBufferReader.
     */
    private MilvusSourceSplit toMilvusSourceSplit(MilvusCdcSourceSplit cdcSplit) {
        // Look up the correct TablePath for this split's collection
        TablePath tablePath = sourceTables.keySet().iterator().next();
        for (TablePath tp : sourceTables.keySet()) {
            if (cdcSplit.getCollectionName().equals(tp.getTableName())) {
                tablePath = tp;
                break;
            }
        }
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
     * Create the appropriate CDC strategy for a specific collection.
     * Falls back to polling incremental if the requested strategy is not available.
     *
     * @param collectionName the target collection name
     * @param collectionDesc the collection schema descriptor
     * @return the CDC strategy, or null if polling_incremental fallback (handled separately)
     */
    private CdcStrategy createCdcStrategy(
            String collectionName, DescribeCollectionResp collectionDesc) {
        String strategyType = cdcConfig.getCdcStrategy();

        if ("event_stream".equalsIgnoreCase(strategyType)) {
            if (Boolean.TRUE.equals(cdcConfig.getCdcUseStreamingNode())) {
                CdcEventStreamStrategyV2 v2Strategy = new CdcEventStreamStrategyV2(
                        cdcConfig, collectionDesc, sourceTables);
                if (v2Strategy.isAvailable()) {
                    log.info("Using event_stream V2 CDC strategy via StreamingNode gRPC "
                            + "for collection '{}'", collectionName);
                    return v2Strategy;
                }
                log.warn("event_stream V2 (StreamingNode) not available for collection '{}'. "
                        + "Falling back to polling_incremental.", collectionName);
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

        // Fallback to polling_incremental
        log.info("Using polling incremental CDC strategy for collection '{}'", collectionName);
        return new PollingIncrementalCdcStrategy(cdcConfig,
                new MilvusSourceConverter(tableSchema), tableSchema, client, collectionName);
    }

}
