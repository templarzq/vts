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
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.utils.MilvusConnectorUtils;

import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.partition.request.ListPartitionsReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.response.QueryResp;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Split enumerator for Milvus CDC source. Manages two-phase split generation:
 *
 * <ol>
 *   <li><b>Snapshot phase:</b> Generates bounded splits using offset/limit pagination,
 *       similar to {@code MilvusSourceSplitEnumerator}. Each split covers a range of
 *       records from a collection/partition.</li>
 *   <li><b>Incremental phase:</b> Generates one unbounded split per reader
 *       ({@code endId = Long.MAX_VALUE}) that the reader uses to continuously poll
 *       for CDC events.</li>
 * </ol>
 */
@Slf4j
public class MilvusCdcSourceSplitEnumerator
        implements SourceSplitEnumerator<MilvusCdcSourceSplit, MilvusCdcSourceState> {

    private final Context<MilvusCdcSourceSplit> context;
    private final ReadonlyConfig config;
    private final MilvusCdcSourceConfig cdcConfig;
    private final Map<TablePath, CatalogTable> tables;
    private final Map<Integer, List<MilvusCdcSourceSplit>> pendingSplits;
    private final Object stateLock = new Object();

    private MilvusClientV2 client;
    private boolean snapshotCompleted;
    private Map<String, ReplicatePosition> splitPositions;
    private long globalTimeTick;
    private Set<Integer> readersWithCompletedSnapshot;

    public MilvusCdcSourceSplitEnumerator(
            Context<MilvusCdcSourceSplit> context,
            ReadonlyConfig config,
            MilvusCdcSourceConfig cdcConfig,
            Map<TablePath, CatalogTable> tables,
            MilvusCdcSourceState sourceState) {
        this.context = context;
        this.config = config;
        this.cdcConfig = cdcConfig;
        this.tables = tables;
        this.pendingSplits = new ConcurrentHashMap<>();
        this.readersWithCompletedSnapshot = ConcurrentHashMap.newKeySet();

        if (sourceState != null) {
            this.snapshotCompleted = sourceState.isSnapshotCompleted();
            this.splitPositions = sourceState.getSplitPositions() != null
                    ? new HashMap<>(sourceState.getSplitPositions())
                    : new HashMap<>();
            this.globalTimeTick = sourceState.getGlobalTimeTick();
        } else {
            this.snapshotCompleted = false;
            this.splitPositions = new HashMap<>();
            this.globalTimeTick = 0;
        }
    }

    @Override
    public void open() {
        this.client = new MilvusClientV2(MilvusConnectorUtils.getConnectConfig(config));
    }

    @Override
    public void run() throws Exception {
        String startupMode = cdcConfig.getStartupMode();

        // Phase 1: Snapshot
        if (!snapshotCompleted && "INITIAL".equalsIgnoreCase(startupMode)) {
            log.info("Starting snapshot phase...");
            generateSnapshotSplits();
            assignPendingSplits();
            // Wait for snapshot to complete before moving to incremental.
            // The enumerator will be re-invoked via handleSourceEvent when each
            // reader signals completion.
            return;
        }

        // Phase 2: Incremental
        if (snapshotCompleted || "LATEST".equalsIgnoreCase(startupMode)) {
            log.info("Starting incremental phase...");
            generateIncrementalSplits();
            assignPendingSplits();
            for (int readerId : context.registeredReaders()) {
                context.signalNoMoreSplits(readerId);
            }
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        // Dynamic split requests are not supported — splits are pre-computed
        log.debug("Split request from subtask {} ignored (pre-computed mode)", subtaskId);
    }

    @Override
    public void addSplitsBack(List<MilvusCdcSourceSplit> splits, int subtaskId) {
        synchronized (stateLock) {
            pendingSplits.computeIfAbsent(subtaskId, k -> new ArrayList<>()).addAll(splits);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void handleSourceEvent(int subtaskId, SourceEvent event) {
        if (event instanceof SnapshotCompletedEvent) {
            SnapshotCompletedEvent sce = (SnapshotCompletedEvent) event;
            log.info("Reader {} completed snapshot for split {}", subtaskId, sce.getSplitId());
            readersWithCompletedSnapshot.add(subtaskId);

            // Check if all registered readers have completed their snapshots
            if (readersWithCompletedSnapshot.containsAll(context.registeredReaders())) {
                snapshotCompleted = true;
                log.info("All readers completed snapshot. Transitioning to incremental phase.");
                try {
                    run();
                } catch (Exception e) {
                    log.error("Error transitioning to incremental phase", e);
                }
            }
        } else if (event instanceof IncrementalPositionEvent) {
            IncrementalPositionEvent ipe = (IncrementalPositionEvent) event;
            synchronized (stateLock) {
                splitPositions.put(ipe.getSplitId(), ipe.getPosition());
                if (ipe.getPosition() != null) {
                    globalTimeTick = Math.max(globalTimeTick,
                            ipe.getPosition().getTimeTick());
                }
            }
        }
    }

    @Override
    public MilvusCdcSourceState snapshotState(long checkpointId) throws Exception {
        synchronized (stateLock) {
            List<MilvusCdcSourceSplit> allPending = new ArrayList<>();
            for (List<MilvusCdcSourceSplit> splits : pendingSplits.values()) {
                allPending.addAll(splits);
            }

            return MilvusCdcSourceState.builder()
                    .lastSeenId(getMaxSeenId())
                    .snapshotCompleted(snapshotCompleted)
                    .pendingSplits(allPending)
                    .splitPositions(new HashMap<>(splitPositions))
                    .globalTimeTick(globalTimeTick)
                    .lastPollTime(System.currentTimeMillis())
                    .build();
        }
    }

    @Override
    public void registerReader(int subtaskId) {
        // Assign any pending splits for this reader
        List<MilvusCdcSourceSplit> readerSplits = pendingSplits.remove(subtaskId);
        if (readerSplits != null && !readerSplits.isEmpty()) {
            context.assignSplit(subtaskId, readerSplits);
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplits.values().stream().mapToInt(List::size).sum();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // No-op: checkpoints are acknowledged asynchronously by the engine
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                log.warn("Error closing Milvus client", e);
            }
        }
    }

    // ---- Private helpers ----

    private void generateSnapshotSplits() throws Exception {
        int parallelism = cdcConfig.getParallelism();
        if (parallelism < 1) parallelism = 1;

        for (Map.Entry<TablePath, CatalogTable> entry : tables.entrySet()) {
            CatalogTable table = entry.getValue();
            String collectionName = table.getTableId().getTableName();

            // Filter collections: sync all, specific list, or a single collection
            if (!cdcConfig.shouldSyncCollection(collectionName)) {
                log.info("Skipping snapshot for collection '{}' — not in configured collections",
                        collectionName);
                continue;
            }

            DescribeCollectionResp desc = client.describeCollection(
                    DescribeCollectionReq.builder().collectionName(collectionName).build());

            // Check if the collection has a partition key
            boolean hasPartitionKey = desc.getCollectionSchema().getFieldSchemaList().stream()
                    .anyMatch(f -> Boolean.TRUE.equals(f.getIsPartitionKey()));

            if (!hasPartitionKey) {
                List<String> partitions = client.listPartitions(
                        ListPartitionsReq.builder().collectionName(collectionName).build());
                if (!partitions.isEmpty() && partitions.size() > 1) {
                    // Generate per-partition splits
                    for (String partition : partitions) {
                        MilvusCdcSourceSplit baseSplit = MilvusCdcSourceSplit.builder()
                                .collectionName(collectionName)
                                .partitionName(partition)
                                .snapshot(true)
                                .offset(0)
                                .limit(-1)
                                .build();
                        splitByOffset(baseSplit, parallelism);
                    }
                } else {
                    // Single partition / no partition key: one base split
                    MilvusCdcSourceSplit baseSplit = MilvusCdcSourceSplit.builder()
                            .collectionName(collectionName)
                            .partitionName(null)
                            .snapshot(true)
                            .offset(0)
                            .limit(-1)
                            .build();
                    splitByOffset(baseSplit, parallelism);
                }
            } else {
                // Partition-key-based collection: generate per-partition splits
                List<String> partitionNames = client.listPartitions(
                        ListPartitionsReq.builder().collectionName(collectionName).build());
                for (String pname : partitionNames) {
                    MilvusCdcSourceSplit baseSplit = MilvusCdcSourceSplit.builder()
                            .collectionName(collectionName)
                            .partitionName(pname)
                            .snapshot(true)
                            .offset(0)
                            .limit(-1)
                            .build();
                    splitByOffset(baseSplit, parallelism);
                }
            }
        }
    }

    private void splitByOffset(MilvusCdcSourceSplit baseSplit, int parallelism) throws Exception {
        if (parallelism <= 1) {
            baseSplit.setSplitId("snapshot-" + baseSplit.getCollectionName() + "-0");
            addPendingSplit(Collections.singletonList(baseSplit));
            return;
        }

        // Query row count for the split range
        long totalCount = queryCount(baseSplit);
        long splitSize = Math.max(1, totalCount / parallelism);

        int splitIndex = 0;
        for (int i = 0; i < parallelism; i++) {
            long offset = (long) i * splitSize;
            long limit = (i == parallelism - 1) ? -1 : splitSize;

            MilvusCdcSourceSplit split = MilvusCdcSourceSplit.builder()
                    .splitId("snapshot-" + baseSplit.getCollectionName() + "-" + splitIndex++)
                    .collectionName(baseSplit.getCollectionName())
                    .partitionName(baseSplit.getPartitionName())
                    .snapshot(true)
                    .offset(offset)
                    .limit(limit)
                    .build();
            addPendingSplit(Collections.singletonList(split));
        }
    }

    private long queryCount(MilvusCdcSourceSplit split) {
        try {
            QueryReq queryReq = QueryReq.builder()
                    .collectionName(split.getCollectionName())
                    .filter("")
                    .outputFields(Collections.singletonList("count(*)"))
                    .partitionNames(split.getPartitionName() != null
                            ? Collections.singletonList(split.getPartitionName())
                            : Collections.emptyList())
                    .build();
            QueryResp resp = client.query(queryReq);
            if (resp.getQueryResults() != null && !resp.getQueryResults().isEmpty()) {
                Object countObj = resp.getQueryResults().get(0).getEntity().get("count(*)");
                if (countObj instanceof Number) {
                    return ((Number) countObj).longValue();
                }
            }
        } catch (Exception e) {
            log.warn("Failed to query count for {}: {}", split.getCollectionName(), e.getMessage());
        }
        return 0;
    }

    private void generateIncrementalSplits() {
        int readerCount = context.registeredReaders().size();
        if (readerCount == 0) readerCount = 1;

        long startId = getMaxSeenId();

        // Get all collections that should be synced
        List<String> syncedCollections = new ArrayList<>();
        for (Map.Entry<TablePath, CatalogTable> entry : tables.entrySet()) {
            String colName = entry.getValue().getTableId().getTableName();
            if (cdcConfig.shouldSyncCollection(colName)) {
                syncedCollections.add(colName);
            }
        }

        // If no matching collections, use the effective single collection
        if (syncedCollections.isEmpty()) {
            String effectiveCol = cdcConfig.getEffectiveCollection();
            if (effectiveCol != null) {
                syncedCollections.add(effectiveCol);
            }
        }

        // Generate one incremental split per reader per collection
        int idx = 0;
        for (String collectionName : syncedCollections) {
            for (int readerId : context.registeredReaders()) {
                String splitId = "cdc-inc-" + collectionName + "-" + idx++;
                ReplicatePosition startPos = splitPositions.get(splitId);

                MilvusCdcSourceSplit incSplit = MilvusCdcSourceSplit.builder()
                        .splitId(splitId)
                        .collectionName(collectionName)
                        .snapshot(false)
                        .startId(startId)
                        .endId(Long.MAX_VALUE)
                        .startPosition(startPos)
                        .build();
                addPendingSplit(Collections.singletonList(incSplit));
            }
        }
    }

    private long getMaxSeenId() {
        long max = 0;
        for (ReplicatePosition pos : splitPositions.values()) {
            if (pos != null && pos.getTimeTick() > max) {
                max = pos.getTimeTick();
            }
        }
        return Math.max(max, globalTimeTick);
    }

    private void addPendingSplit(Collection<MilvusCdcSourceSplit> splits) {
        for (MilvusCdcSourceSplit split : splits) {
            int owner = getSplitOwner(split.splitId(), context.registeredReaders().size());
            pendingSplits.computeIfAbsent(owner, k -> new ArrayList<>()).add(split);
        }
    }

    private int getSplitOwner(String splitId, int numReaders) {
        if (numReaders <= 1) return 0;
        return Math.abs(splitId.hashCode()) % numReaders;
    }

    private void assignPendingSplits() {
        for (Map.Entry<Integer, List<MilvusCdcSourceSplit>> entry : pendingSplits.entrySet()) {
            int readerId = entry.getKey();
            List<MilvusCdcSourceSplit> splits = entry.getValue();
            if (!splits.isEmpty() && context.registeredReaders().contains(readerId)) {
                context.assignSplit(readerId, new ArrayList<>(splits));
            }
        }
    }
}
