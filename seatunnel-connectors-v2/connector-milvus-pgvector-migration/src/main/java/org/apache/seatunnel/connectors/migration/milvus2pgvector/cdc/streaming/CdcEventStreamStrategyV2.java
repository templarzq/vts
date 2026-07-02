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

package org.apache.seatunnel.connectors.migration.milvus2pgvector.cdc.streaming;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.cdc.CdcStrategy;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.cdc.MilvusCdcSourceConfig;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.cdc.MilvusCdcSourceSplit;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.cdc.ReplicatePosition;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.cdc.SeaTunnelRowWithPosition;
import org.apache.seatunnel.connectors.streaming.proto.DeliverPolicy;
import org.apache.seatunnel.connectors.streaming.proto.ImmutableMessage;
import org.apache.seatunnel.connectors.streaming.proto.MessageID;
import org.apache.seatunnel.connectors.streaming.proto.PChannelAccessMode;
import org.apache.seatunnel.connectors.streaming.proto.PChannelInfo;
import org.apache.seatunnel.connectors.streaming.proto.ReplicateCheckpoint;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * CDC strategy V2 that captures real Milvus WAL events via StreamingNode gRPC.
 *
 * <p>Uses {@code StreamingNodeHandlerService.Consume} RPC to directly access WAL
 * messages. Unlike the legacy {@link org.apache.seatunnel.connectors.migration.milvus2pgvector.cdc.CdcEventStreamStrategy}
 * (which uses DumpMessages API and requires replication topology), this strategy
 * works in both standalone and replication topology modes.
 *
 * <p>Core capabilities:
 * <ul>
 *   <li><b>Full snapshot sync</b> — uses {@code DeliverPolicy.all} to read all
 *       historical WAL messages</li>
 *   <li><b>Incremental sync</b> — uses {@code DeliverPolicy.startAfter} to resume
 *       from checkpoint</li>
 *   <li><b>Delete capture</b> — {@code MessageTypeDelete} produces rows with
 *       {@code RowKind.DELETE}</li>
 *   <li><b>Upsert handling</b> — Milvus WAL has no separate Upsert message type;
 *       upserts are decomposed into Delete+Insert pairs at the proxy level.
 *       Insert messages produce {@code RowKind.INSERT} (sink treats as upsert),
 *       Delete messages produce {@code RowKind.DELETE}</li>
 * </ul>
 */
@Slf4j
public class CdcEventStreamStrategyV2 implements CdcStrategy {

    private final StreamingNodeHandlerClient streamingNodeClient;
    private final StreamingMessageParser parser;
    private final String pchannelName;
    private final String vchannelName;
    private final long collectionId;
    private final long maxEventsPerPoll;
    private final String sourceClusterId;

    /** Live Consume stream cursor; kept across polls to avoid re-opening. */
    private ConsumeStream consumeStream;

    /** Last emitted event position; used for checkpointing. */
    private ReplicatePosition lastPosition;

    /** Cached term (static) — avoids re-probing across strategy instances. */
    private static volatile long cachedTerm = -1;

    /**
     * Public constructor — creates its own {@link StreamingNodeHandlerClient}.
     *
     * @param config         CDC source config (must have {@code cdc_pchannel} set)
     * @param collectionDesc collection schema description
     */
    public CdcEventStreamStrategyV2(
            MilvusCdcSourceConfig config, DescribeCollectionResp collectionDesc) {
        this.pchannelName = config.getCdcPchannel();
        this.collectionId = collectionDesc.getCollectionID();
        // vchannel format: {pchannel}_{collectionID}v{shardIdx}
        // For single-shard collections, shardIdx = 0
        this.vchannelName = pchannelName + "_" + collectionId + "v0";
        this.maxEventsPerPoll = config.getIncrementalBatchSize();
        this.sourceClusterId = config.getCdcSourceClusterId();

        String streamingNodeAddress = config.getStreamingNodeAddress() != null
                ? config.getStreamingNodeAddress()
                : config.getUrl();

        this.streamingNodeClient = new StreamingNodeHandlerClient(
                streamingNodeAddress,
                config.getToken(),
                config.getChannelTimeoutMs());

        this.parser = new StreamingMessageParser(collectionDesc, config.getPrimaryKeyField());

        log.info("CdcEventStreamStrategyV2 initialized: pchannel={}, vchannel={}, collectionId={}, streamingNode={}",
                pchannelName, vchannelName, collectionId, streamingNodeAddress);
    }

    /**
     * Check if StreamingNode gRPC is available and the configured pchannel is valid.
     *
     * <p>Two-step probe:
     * <ol>
     *   <li>Call {@code GetReplicateCheckpoint} to verify the StreamingNode service is
     *       reachable. UNIMPLEMENTED (wrong port) or UNAVAILABLE (connection refused)
     *       → return false immediately.</li>
     *   <li>If the service is reachable, try opening a Consume stream with term=1.
     *       In standalone Milvus, term is always 1 (no channel rebalancing).
     *       A valid pchannel will succeed; an invalid pchannel name will fail.</li>
     * </ol>
     *
     * @return true if StreamingNode service is reachable AND the pchannel is valid
     */
    @Override
    public boolean isAvailable() {
        if (pchannelName == null || pchannelName.isEmpty()) {
            log.warn("event_stream V2 isAvailable()=false: cdc_pchannel is not configured");
            return false;
        }

        // Step 1: Verify StreamingNode service is reachable via GetReplicateCheckpoint.
        // FAILED_PRECONDITION is expected in standalone mode (replication not enabled),
        // but the service is still reachable. UNIMPLEMENTED/UNAVAILABLE means the service
        // is not reachable at all.
        try {
            PChannelInfo pchannel = PChannelInfo.newBuilder()
                    .setName(pchannelName)
                    .setTerm(1)
                    .setAccessMode(PChannelAccessMode.PCHANNEL_ACCESS_READONLY)
                    .build();

            streamingNodeClient.getReplicateCheckpoint(pchannel);
            log.info("StreamingNode service reachable for pchannel={} (checkpoint RPC succeeded)",
                    pchannelName);
        } catch (io.grpc.StatusRuntimeException e) {
            io.grpc.Status.Code code = e.getStatus().getCode();
            if (code == io.grpc.Status.Code.FAILED_PRECONDITION) {
                log.info("StreamingNode service reachable (FAILED_PRECONDITION on checkpoint, "
                        + "proceeding to pchannel validation via Consume RPC)");
            } else {
                log.warn("StreamingNode gRPC not reachable: {} (code={})", e.getMessage(), code);
                return false;
            }
        } catch (Exception e) {
            log.warn("StreamingNode gRPC not available: {}", e.getMessage());
            return false;
        }

        // Step 2: Validate the pchannel by trying to open a Consume stream with term=1.
        // A valid pchannel will succeed; an invalid pchannel name will fail.
        boolean pchannelValid = probeConsumeStream(pchannelName);
        if (pchannelValid) {
            log.info("StreamingNode gRPC available for pchannel={} (Consume probe succeeded)",
                    pchannelName);
        } else {
            log.warn("StreamingNode gRPC service reachable but pchannel={} is invalid "
                    + "(Consume probe failed for term=1)", pchannelName);
        }
        return pchannelValid;
    }

    /**
     * Probe the pchannel by opening a Consume stream.
     * Tries cached term first, then term=1. If that fails with UNMATCHED_CHANNEL_TERM,
     * extracts the server's current term directly from the error and retries.
     * No range probing — the term is obtained directly from the server response.
     * Returns true if the stream opens successfully, false otherwise.
     */
    private boolean probeConsumeStream(String pchannelCandidate) {
        String vchannelCandidate = pchannelCandidate + "_" + collectionId + "v0";
        DeliverPolicy policy = DeliverPolicy.newBuilder()
                .setAll(Empty.newBuilder().build())
                .build();

        // Step 1: Try cached term (from previous successful connection).
        long ct = cachedTerm;
        if (ct > 0) {
            ConsumeStream stream = tryCreateStream(pchannelCandidate, vchannelCandidate, policy, ct);
            if (stream != null) {
                stream.close();
                log.debug("Consume probe succeeded for pchannel={}, term={} (cached)",
                        pchannelCandidate, ct);
                return true;
            }
            cachedTerm = -1;
        }

        // Step 2: Try term=1 (typical for fresh deployments).
        long term = 1;
        ConsumeStream stream = tryCreateStream(pchannelCandidate, vchannelCandidate, policy, term);
        if (stream != null) {
            stream.close();
            return true;
        }

        // Step 3: Extract the server's current term from UNMATCHED_CHANNEL_TERM error and retry.
        long currentTerm = lastStreamCurrentTerm;
        if (currentTerm > 0 && currentTerm != term) {
            stream = tryCreateStream(pchannelCandidate, vchannelCandidate, policy, currentTerm);
            if (stream != null) {
                stream.close();
                log.info("Consume probe succeeded for pchannel={}, term={} (from UNMATCHED_CHANNEL_TERM error)",
                        pchannelCandidate, currentTerm);
                return true;
            }
        }
        return false;
    }

    /** Last ConsumeStream's current term from UNMATCHED_CHANNEL_TERM error. */
    private long lastStreamCurrentTerm = -1;

    /**
     * Try to create a Consume stream with the given term.
     * If the stream fails with UNMATCHED_CHANNEL_TERM, stores the server's current
     * term in {@link #lastStreamCurrentTerm} for the next retry.
     *
     * @return the open ConsumeStream, or null if it failed
     */
    private ConsumeStream tryCreateStream(String pchannel, String vchannel,
                                          DeliverPolicy policy, long term) {
        try {
            PChannelInfo pchannelInfo = PChannelInfo.newBuilder()
                    .setName(pchannel)
                    .setTerm(term)
                    .setAccessMode(PChannelAccessMode.PCHANNEL_ACCESS_READONLY)
                    .build();

            ConsumeStream stream = streamingNodeClient.createConsumeStream(
                    pchannelInfo, vchannel, policy);

            // Wait for async response — reduced from 100ms to 30ms.
            // The server responds quickly with error on wrong term.
            Thread.sleep(30);

            if (stream.isOpen()) {
                // Cache successful term for future instances.
                if (term != cachedTerm && term > 0) {
                    cachedTerm = term;
                }
                return stream;
            }

            // Extract the server's current term from UNMATCHED_CHANNEL_TERM error
            long currentTerm = stream.getCurrentTermFromError();
            if (currentTerm > 0) {
                lastStreamCurrentTerm = currentTerm;
                cachedTerm = currentTerm;
                log.info("UNMATCHED_CHANNEL_TERM: tried term={}, server current term={}",
                        term, currentTerm);
            }

            try {
                stream.close();
            } catch (Exception ignored) {
            }
        } catch (Exception e) {
            log.debug("Failed to create Consume stream for pchannel={}, term={}: {}",
                    pchannel, term, e.getMessage());
        }
        return null;
    }

    /**
     * Poll for incremental changes since the given position.
     *
     * @param split         the incremental split being read
     * @param startPosition the position to start reading from (null for full snapshot)
     * @return list of rows with their CDC positions
     * @throws Exception on connection or deserialization errors
     */
    @Override
    public List<SeaTunnelRowWithPosition> pollChanges(
            MilvusCdcSourceSplit split, ReplicatePosition startPosition) throws Exception {

        // Open stream if not already open or if startPosition changed
        if (consumeStream == null || !consumeStream.isOpen()) {
            openStream(startPosition);
        }

        List<SeaTunnelRowWithPosition> results = new ArrayList<>();
        int eventCount = 0;

        try {
            while (consumeStream.hasNext() && eventCount < maxEventsPerPoll) {
                ImmutableMessage message = consumeStream.next();
                eventCount++;

                // Parse message into SeaTunnelRow(s)
                List<SeaTunnelRow> parsedRows = parser.parseMessage(message);

                // Build position for this message
                MessageID messageId = parser.extractMessageID(message);
                long timetick = parser.extractTimetick(message);
                ReplicatePosition position = ReplicatePosition.builder()
                        .clusterId(sourceClusterId)
                        .pchannel(pchannelName)
                        .messageId(messageId.getId().toStringUtf8())
                        .timeTick(timetick)
                        .timestamp(System.currentTimeMillis())
                        .build();
                lastPosition = position;

                // Wrap each row with its position
                for (SeaTunnelRow row : parsedRows) {
                    results.add(new SeaTunnelRowWithPosition(row, position));
                }

                log.debug("Poll event {}: type={}, messageId={}, rows={}",
                        eventCount, parser.parseMessageType(message), messageId.getId().toStringUtf8(), parsedRows.size());
            }
        } catch (Exception e) {
            log.warn("Error polling changes: {}", e.getMessage());
            // Return partial results
        }

        log.info("Poll completed: {} events, {} rows", eventCount, results.size());
        return results;
    }

    /**
     * Open the Consume stream with the appropriate DeliverPolicy.
     * Auto-detects the correct pchannel by trying all pchannels (0-15) if the
     * configured pchannel doesn't work.
     *
     * @param startPosition Optional starting position (null for full snapshot)
     */
    private void openStream(ReplicatePosition startPosition) {
        DeliverPolicy policy;
        if (startPosition != null && startPosition.getMessageId() != null
                && !startPosition.getMessageId().isEmpty()) {
            // Incremental sync: start after checkpoint
            MessageID startMessageId = MessageID.newBuilder()
                    .setId(ByteString.copyFromUtf8(startPosition.getMessageId()))
                    .build();
            policy = DeliverPolicy.newBuilder()
                    .setStartAfter(startMessageId)
                    .build();
            log.info("Opening Consume stream for incremental sync: startAfter messageId={}",
                    startPosition.getMessageId());
        } else {
            // Full snapshot: deliver all messages
            policy = DeliverPolicy.newBuilder()
                    .setAll(Empty.newBuilder().build())
                    .build();
            log.info("Opening Consume stream for full snapshot: DeliverPolicy.all");
        }

        // Try the configured pchannel first, then auto-detect if it fails
        if (tryOpenStream(pchannelName, policy)) {
            return;
        }

        // Auto-detect: try all pchannels (0-15)
        log.info("Configured pchannel {} failed; auto-detecting correct pchannel for collectionId={}",
                pchannelName, collectionId);
        for (int i = 0; i < 16; i++) {
            String candidatePchannel = "by-dev-rootcoord-dml_" + i;
            if (candidatePchannel.equals(pchannelName)) {
                continue;  // Already tried
            }
            if (tryOpenStream(candidatePchannel, policy)) {
                log.info("Auto-detected correct pchannel: {} for collectionId={}",
                        candidatePchannel, collectionId);
                return;
            }
        }

        log.error("Failed to open Consume stream on any pchannel for collectionId={}", collectionId);
        throw new RuntimeException("Failed to open CDC stream: no valid pchannel found");
    }

    /**
     * Try to open a Consume stream on the given pchannel.
     * Tries cached term first, then term=1. If that fails with UNMATCHED_CHANNEL_TERM,
     * extracts the server's current term directly from the error and retries.
     * No range probing — the term is obtained directly from the server response.
     * Returns true if the stream was opened successfully.
     *
     * @param pchannelCandidate pchannel name to try
     * @param policy DeliverPolicy (all or startAfter)
     * @return true if stream opened and stored in {@link #consumeStream}
     */
    private boolean tryOpenStream(String pchannelCandidate, DeliverPolicy policy) {
        String vchannelCandidate = pchannelCandidate + "_" + collectionId + "v0";

        // Step 1: Try cached term (from previous successful connection).
        long ct = cachedTerm;
        if (ct > 0) {
            ConsumeStream stream = tryCreateStream(pchannelCandidate, vchannelCandidate, policy, ct);
            if (stream != null) {
                consumeStream = stream;
                log.debug("Consume stream opened for pchannel={}, term={} (cached)",
                        pchannelCandidate, ct);
                return true;
            }
            cachedTerm = -1;
        }

        // Step 2: Try term=1 (typical for fresh deployments).
        long term = 1;
        ConsumeStream stream = tryCreateStream(pchannelCandidate, vchannelCandidate, policy, term);
        if (stream != null) {
            consumeStream = stream;
            log.info("Consume stream opened for pchannel={}, term={}", pchannelCandidate, term);
            return true;
        }

        // Step 3: Extract the server's current term from UNMATCHED_CHANNEL_TERM error and retry.
        long currentTerm = lastStreamCurrentTerm;
        if (currentTerm > 0 && currentTerm != term) {
            stream = tryCreateStream(pchannelCandidate, vchannelCandidate, policy, currentTerm);
            if (stream != null) {
                consumeStream = stream;
                log.info("Consume stream opened for pchannel={}, term={} (from UNMATCHED_CHANNEL_TERM error)",
                        pchannelCandidate, currentTerm);
                return true;
            }
        }
        return false;
    }

    /**
     * Get the last consumed position (for checkpointing).
     *
     * @return last ReplicatePosition, or null if no messages consumed
     */
    public ReplicatePosition getLastPosition() {
        return lastPosition;
    }

    /**
     * Close the CDC stream and release resources.
     */
    @Override
    public void close() {
        try {
            if (consumeStream != null) {
                consumeStream.close();
                log.info("Consume stream closed for pchannel={}", pchannelName);
            }
        } catch (Exception e) {
            log.warn("Error closing ConsumeStream: {}", e.getMessage());
        }

        try {
            if (streamingNodeClient != null) {
                streamingNodeClient.close();
                log.info("StreamingNodeHandlerClient closed");
            }
        } catch (Exception e) {
            log.warn("Error closing StreamingNodeHandlerClient: {}", e.getMessage());
        }
    }
}