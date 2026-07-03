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

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.cdc.milvus.proto.ImmutableMessage;
import org.apache.seatunnel.connectors.cdc.milvus.proto.MessageID;
import org.apache.seatunnel.connectors.cdc.milvus.proto.ReplicateCheckpoint;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.exception.MigrationErrorCode;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.exception.MigrationException;

import io.milvus.v2.service.collection.response.DescribeCollectionResp;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * CDC strategy that captures real Milvus WAL events via the server-side
 * {@code DumpMessages} streaming gRPC RPC.
 *
 * <p>Unlike the polling and grpc_replicate strategies (which can only detect
 * new primary keys via query iteration), this strategy consumes the raw WAL
 * insert and delete events and converts them into {@link SeaTunnelRow} with
 * the correct {@link org.apache.seatunnel.api.table.type.RowKind}. This enables:
 *
 * <ul>
 *   <li><b>Delete capture</b> — {@code DeleteRequest} messages are expanded
 *       into rows marked {@code RowKind.DELETE}, carrying the deleted primary
 *       key so downstream sinks can apply the deletion.</li>
 *   <li><b>Same-PK update capture</b> — upserts produce both an INSERT and
 *       DELETE event (or two INSERTs) which downstream sinks can reconcile.</li>
 * </ul>
 *
 * <p>The strategy opens a long-lived {@code DumpMessages} stream and keeps the
 * gRPC iterator alive across multiple {@link #pollChanges} invocations to
 * amortise the RPC setup cost. Position tracking uses the
 * {@link ReplicatePosition#messageId} field to store the WAL message ID
 * string so checkpoints can resume from the exact last consumed position.
 */
/**
 * @deprecated Since Milvus 2.5.5, StreamingNode gRPC (V2 strategy) is the recommended
 *             approach. This DumpMessages-based V1 strategy only works in replication
 *             topology (cluster mode) and is unavailable in standalone deployments.
 *             Use {@link org.apache.seatunnel.connectors.migration.milvus2pgvector.cdc.streaming.CdcEventStreamStrategyV2}
 *             instead.
 */
@Deprecated
@Slf4j
public class CdcEventStreamStrategy implements CdcStrategy {

    private final MilvusCdcGrpcClient grpcClient;
    private final MilvusEventParser parser;
    private final String targetPchannel;
    private final String sourceClusterId;
    private final long maxEventsPerPoll;
    private final long pollIntervalMs;
    private final String startMessageIdOverride;

    /** Live gRPC stream cursor; kept across polls to avoid re-opening the stream. */
    private Iterator<ImmutableMessage> currentStream;

    /** Last emitted event position; used to fill clusterId for subsequent events. */
    private ReplicatePosition lastPosition;

    /**
     * Single-thread executor used to apply a timeout to the blocking
     * {@code FrameIterator.hasNext()} call (which internally does
     * {@code LinkedBlockingQueue.take()}). Without this wrapper a poll would
     * block indefinitely when no new WAL messages arrive, stalling the engine's
     * checkpoint loop.
     */
    private final ExecutorService pollExecutor;

    /**
     * Public constructor — creates its own {@link MilvusCdcGrpcClient}.
     *
     * @param config         CDC source config (must have {@code cdc_pchannel} set)
     * @param collectionDesc collection schema description (from SDK describeCollection)
     */
    public CdcEventStreamStrategy(
            MilvusCdcSourceConfig config, DescribeCollectionResp collectionDesc) {
        this(config, collectionDesc,
                new MilvusCdcGrpcClient(
                        config.getUrl(), config.getToken(), config.getChannelTimeoutMs()));
    }

    /**
     * Package-private constructor that accepts an injected gRPC client.
     * Used by unit tests to substitute a mock client without spinning up a
     * real Milvus server.
     */
    CdcEventStreamStrategy(
            MilvusCdcSourceConfig config,
            DescribeCollectionResp collectionDesc,
            MilvusCdcGrpcClient grpcClient) {
        this.grpcClient = grpcClient;
        this.parser = new MilvusEventParser(collectionDesc, config.getPrimaryKeyField());
        this.targetPchannel = config.getCdcPchannel();
        this.sourceClusterId = config.getCdcSourceClusterId();
        this.maxEventsPerPoll = config.getIncrementalBatchSize();
        this.pollIntervalMs = config.getPollIntervalMs();
        this.startMessageIdOverride = config.getCdcStartMessageId();
        this.pollExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "cdc-event-stream-poll-" + targetPchannel);
            t.setDaemon(true);
            return t;
        });
        log.debug("CdcEventStreamStrategy initialised: pchannel={}, maxEventsPerPoll={}, pollIntervalMs={}",
                targetPchannel, maxEventsPerPoll, pollIntervalMs);
    }

    @Override
    public boolean isAvailable() {
        if (targetPchannel == null || targetPchannel.isEmpty()) {
            log.warn("event_stream CDC isAvailable()=false: cdc_pchannel is not configured");
            return false;
        }

        // DumpMessages API requires a startMessageID obtained from GetReplicateInfo.
        // Without a valid checkpoint, V1 (DumpMessages-based) strategy cannot bootstrap
        // the stream. GetReplicateInfo is only available in replication topology
        // (secondary clusters); in standalone mode it returns empty/fails, and the
        // DumpMessages fallback (default MessageID) does not work because the API
        // needs a concrete start position. Therefore V1 is unavailable in standalone mode.
        try {
            Optional<ReplicateCheckpoint> cp =
                    grpcClient.getReplicateInfo(sourceClusterId, targetPchannel);
            if (cp.isPresent()) {
                log.info("event_stream CDC available: GetReplicateInfo returned checkpoint "
                        + "for pchannel={} (replication topology)", targetPchannel);
                return true;
            } else {
                log.warn("event_stream CDC isAvailable()=false: GetReplicateInfo returned no "
                        + "checkpoint for pchannel={} (standalone mode is not supported by the "
                        + "DumpMessages-based V1 strategy; use StreamingNode V2 strategy instead)",
                        targetPchannel);
                return false;
            }
        } catch (Exception e) {
            log.warn("event_stream CDC isAvailable()=false: GetReplicateInfo RPC failed for "
                    + "pchannel={} (standalone mode not supported by V1 strategy): {}",
                    targetPchannel, e.getMessage());
            return false;
        }
    }

    @Override
    public List<SeaTunnelRowWithPosition> pollChanges(
            MilvusCdcSourceSplit split, ReplicatePosition startPosition) throws Exception {
        // Bootstrap the stream if not already open
        if (currentStream == null) {
            openStream(startPosition);
        }

        List<SeaTunnelRowWithPosition> results = new ArrayList<>();
        while (results.size() < maxEventsPerPoll) {
            if (!hasNextWithTimeout()) {
                break;
            }
            ImmutableMessage msg = currentStream.next();
            List<SeaTunnelRow> rows = parser.parseImmutableMessage(msg);
            if (rows.isEmpty()) {
                continue;
            }
            String msgId = parser.extractMessageIdString(msg);
            long tt = parser.extractTimetick(msg);
            String clusterId = resolveClusterId();
            for (SeaTunnelRow row : rows) {
                ReplicatePosition pos = ReplicatePosition.builder()
                        .clusterId(clusterId)
                        .pchannel(targetPchannel)
                        .messageId(msgId)
                        .timeTick(tt)
                        .timestamp(System.currentTimeMillis())
                        .build();
                results.add(new SeaTunnelRowWithPosition(row, pos));
                lastPosition = pos;
                if (results.size() >= maxEventsPerPoll) {
                    break;
                }
            }
        }

        if (results.isEmpty()) {
            log.debug("No CDC events in this poll (pchannel={})", targetPchannel);
        } else {
            log.debug("Returning {} CDC events (pchannel={}, lastMsgId={})",
                    results.size(), targetPchannel,
                    results.get(results.size() - 1).getPosition().getMessageId());
        }
        return results;
    }

    /**
     * Open (or re-open) the DumpMessages stream. Determines the start
     * {@link MessageID} using, in priority order: explicit override,
     * resumed checkpoint position, or server-side bootstrap via
     * {@code GetReplicateInfo}.
     */
    private void openStream(ReplicatePosition startPosition) throws Exception {
        MessageID startMsgId = resolveStartMessageId(startPosition);
        long startTimetick = 0L;
        if (startPosition != null && startPosition.getTimeTick() > 0) {
            startTimetick = startPosition.getTimeTick();
        }
        log.info("Opening DumpMessages stream: pchannel={}, startMsgId={}, startTimetick={}",
                targetPchannel, startMsgId.getId(), startTimetick);
        currentStream = grpcClient.dumpMessages(targetPchannel, startMsgId, startTimetick, 0L);
    }

    /**
     * Resolve the WAL start position. For standalone Milvus (where GetReplicateInfo
     * may not be available), a default empty MessageID is used to start from the
     * earliest WAL position. For replication topology, GetReplicateInfo is used.
     */
    private MessageID resolveStartMessageId(ReplicatePosition startPosition) {
        if (startMessageIdOverride != null && !startMessageIdOverride.isEmpty()) {
            log.info("Using cdc_start_message_id override: {}", startMessageIdOverride);
            return messageIDFromString(startMessageIdOverride);
        }
        if (startPosition != null
                && startPosition.getMessageId() != null
                && !startPosition.getMessageId().isEmpty()) {
            log.info("Resuming from checkpoint messageId={}", startPosition.getMessageId());
            return messageIDFromString(startPosition.getMessageId());
        }
        
        // First-time bootstrap: query GetReplicateInfo for the current checkpoint
        try {
            Optional<ReplicateCheckpoint> cp =
                    grpcClient.getReplicateInfo(sourceClusterId, targetPchannel);
            if (cp.isPresent()) {
                ReplicateCheckpoint checkpoint = cp.get();
                MessageID mid = MilvusCdcGrpcClient.messageIDFromCheckpoint(checkpoint);
                lastPosition = ReplicatePosition.builder()
                        .clusterId(checkpoint.getClusterId())
                        .pchannel(checkpoint.getPchannel())
                        .messageId(mid.getId())
                        .timeTick(checkpoint.getTimeTick())
                        .timestamp(System.currentTimeMillis())
                        .build();
                log.info("Bootstrap from GetReplicateInfo checkpoint: clusterId={}, pchannel={}, messageId={}, timeTick={}",
                        checkpoint.getClusterId(), checkpoint.getPchannel(), mid.getId(), checkpoint.getTimeTick());
                return mid;
            }
        } catch (Exception e) {
            log.warn("GetReplicateInfo RPC failed (standalone mode detected): {}", e.getMessage());
        }
        
        // Fallback for standalone Milvus: use default empty MessageID to start from earliest position
        MessageID defaultMsgId = createDefaultMessageID();
        lastPosition = ReplicatePosition.builder()
                .clusterId(sourceClusterId != null ? sourceClusterId : "standalone")
                .pchannel(targetPchannel)
                .messageId(defaultMsgId.getId())
                .timeTick(0L)
                .timestamp(System.currentTimeMillis())
                .build();
        log.info("Fallback to default MessageID for standalone mode: messageId={} (will read from earliest WAL position)",
                defaultMsgId.getId());
        return defaultMsgId;
    }
    
    /**
     * Create a default empty MessageID for standalone Milvus mode.
     * This represents starting from the earliest WAL position.
     */
    private MessageID createDefaultMessageID() {
        // Return an empty MessageID - this should cause the WAL reader to start from the beginning
        // Note: We don't set WALName field as it's optional and determined by the server
        return MessageID.newBuilder()
                .setId("")  // Empty ID represents earliest position
                .build();
    }

    /**
     * Call {@code currentStream.hasNext()} with a timeout of
     * {@link #pollIntervalMs}. Returns false if no message arrives within
     * the timeout, or if the stream has ended or errored. When the stream
     * ends or errors, {@link #currentStream} is reset to null so the next
     * {@link #pollChanges} invocation re-bootstraps via
     * {@code GetReplicateInfo}.
     */
    private boolean hasNextWithTimeout() {
        if (currentStream == null) {
            return false;
        }
        Future<Boolean> future = pollExecutor.submit(() -> {
            // Clear any stale interrupt flag from a previous cancel(true) so
            // the blocking take() inside FrameIterator is not short-circuited.
            Thread.interrupted();
            return currentStream.hasNext();
        });
        try {
            boolean hasNext = future.get(pollIntervalMs, TimeUnit.MILLISECONDS);
            if (!hasNext) {
                log.info("DumpMessages stream ended, will re-bootstrap on next poll");
                currentStream = null;
            }
            return hasNext;
        } catch (TimeoutException e) {
            future.cancel(true);
            return false;
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            log.warn("DumpMessages stream error, closing stream: {}", cause.getMessage());
            currentStream = null;
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private String resolveClusterId() {
        if (sourceClusterId != null && !sourceClusterId.isEmpty()) {
            return sourceClusterId;
        }
        if (lastPosition != null && lastPosition.getClusterId() != null) {
            return lastPosition.getClusterId();
        }
        return "";
    }

    private static MessageID messageIDFromString(String id) {
        return MessageID.newBuilder().setId(id).build();
    }

    @Override
    public void close() {
        if (pollExecutor != null) {
            pollExecutor.shutdownNow();
        }
        if (grpcClient != null) {
            try {
                grpcClient.close();
            } catch (Exception e) {
                log.warn("Error closing MilvusCdcGrpcClient", e);
            }
        }
        log.info("CdcEventStreamStrategy closed (pchannel={})", targetPchannel);
    }
}
