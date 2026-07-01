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
 *   <li><b>Upsert handling</b> — {@code MessageTypeUpsert} produces rows with
 *       {@code RowKind.INSERT} (sink handles upsert)</li>
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
     * Check if StreamingNode gRPC is available.
     * Uses GetReplicateCheckpoint RPC as a connectivity test.
     * If the RPC returns FAILED_PRECONDITION (replication not enabled), the service
     * is still reachable and Consume RPC may work — return true.
     * Only return false for UNIMPLEMENTED (wrong port) or UNAVAILABLE (connection refused).
     *
     * @return true if StreamingNode service is reachable
     */
    @Override
    public boolean isAvailable() {
        if (pchannelName == null || pchannelName.isEmpty()) {
            log.warn("event_stream V2 isAvailable()=false: cdc_pchannel is not configured");
            return false;
        }

        try {
            PChannelInfo pchannel = PChannelInfo.newBuilder()
                    .setName(pchannelName)
                    .setTerm(1)
                    .setAccessMode(PChannelAccessMode.PCHANNEL_ACCESS_READONLY)
                    .build();

            Optional<ReplicateCheckpoint> checkpoint = streamingNodeClient.getReplicateCheckpoint(pchannel);
            log.info("StreamingNode gRPC available for pchannel={} (checkpoint available: {})",
                    pchannelName, checkpoint.isPresent());
            return true;  // RPC succeeded
        } catch (io.grpc.StatusRuntimeException e) {
            io.grpc.Status.Code code = e.getStatus().getCode();
            log.warn("StreamingNode gRPC status: {} (code={})", e.getMessage(), code);

            // FAILED_PRECONDITION = service exists but replication not enabled
            // Consume RPC may still work in standalone mode
            if (code == io.grpc.Status.Code.FAILED_PRECONDITION) {
                log.info("StreamingNode service reachable (FAILED_PRECONDITION on checkpoint, "
                        + "Consume RPC should still work in standalone mode)");
                return true;
            }

            // UNIMPLEMENTED = wrong port, UNAVAILABLE = connection refused
            return false;
        } catch (Exception e) {
            log.warn("StreamingNode gRPC not available: {}", e.getMessage());
            return false;
        }
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
     * Returns true if the stream was opened successfully (no immediate error).
     *
     * <p>Term is auto-detected by trying values from 1 to 30. In Milvus, term increments
     * each time the channel is rebalanced, and the WAL is opened with the current term.
     * Using a mismatched term causes GetAvailableWAL to return UNMATCHED_CHANNEL_TERM,
     * which maps to gRPC FAILED_PRECONDITION.
     */
    private boolean tryOpenStream(String pchannelCandidate, DeliverPolicy policy) {
        String vchannelCandidate = pchannelCandidate + "_" + collectionId + "v0";

        for (long term = 1; term <= 30; term++) {
            try {
                PChannelInfo pchannel = PChannelInfo.newBuilder()
                        .setName(pchannelCandidate)
                        .setTerm(term)
                        .setAccessMode(PChannelAccessMode.PCHANNEL_ACCESS_READONLY)
                        .build();

                ConsumeStream stream = streamingNodeClient.createConsumeStream(
                        pchannel, vchannelCandidate, policy);

                Thread.sleep(100);

                if (stream.isOpen()) {
                    consumeStream = stream;
                    log.info("Consume stream opened successfully for pchannel={}, vchannel={}, term={}",
                            pchannelCandidate, vchannelCandidate, term);
                    return true;
                } else {
                    try {
                        stream.close();
                    } catch (Exception ignored) {
                    }
                }
            } catch (Exception e) {
                log.info("Failed to open stream on pchannel={} with term={}: {}",
                        pchannelCandidate, term, e.getMessage());
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