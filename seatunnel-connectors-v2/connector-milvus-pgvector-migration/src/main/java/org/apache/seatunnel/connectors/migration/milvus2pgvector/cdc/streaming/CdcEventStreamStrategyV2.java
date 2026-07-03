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
    /** Configured pchannel — may be updated by auto-discovery in {@link #isAvailable()}. */
    private String pchannelName;
    private final String vchannelName;
    private final long collectionId;
    private final long maxEventsPerPoll;
    private final String sourceClusterId;
    private final PChannelResolver pchannelResolver;

    /** Live Consume stream cursor; kept across polls to avoid re-opening. */
    private ConsumeStream consumeStream;

    /** Last emitted event position; used for checkpointing. */
    private ReplicatePosition lastPosition;

    /** Cached term (static) — avoids re-probing across strategy instances. */
    private static volatile long cachedTerm = -1;

    /**
     * Public constructor — creates its own {@link StreamingNodeHandlerClient}.
     *
     * <p>The {@code cdc_pchannel} config is optional — when omitted (null), pchannel
     * auto-discovery from etcd and 0-15 scanning is performed in {@link #isAvailable()}
     * and {@link #openStream}.
     *
     * @param config         CDC source config
     * @param collectionDesc collection schema description
     */
    public CdcEventStreamStrategyV2(
            MilvusCdcSourceConfig config, DescribeCollectionResp collectionDesc) {
        this.pchannelName = config.getCdcPchannel();
        this.collectionId = collectionDesc.getCollectionID();
        // vchannel format: {pchannel}_{collectionID}v{shardIdx}
        // For single-shard collections, shardIdx = 0
        // Skip pre-computing vchannel when pchannel is not yet resolved
        this.vchannelName = pchannelName != null && !pchannelName.isEmpty()
                ? pchannelName + "_" + collectionId + "v0"
                : null;
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

        // Initialize pchannel resolver for etcd-based auto-discovery
        this.pchannelResolver = new PChannelResolver(
                config.getCdcEtcdEndpoint(),
                extractRootPath(config));

        log.info("CdcEventStreamStrategyV2 initialized: pchannel={}, vchannel={}, collectionId={}, streamingNode={}",
                pchannelName, vchannelName, collectionId, streamingNodeAddress);
    }

    /**
     * Check if StreamingNode gRPC is available and the pchannel is valid.
     *
     * <p>Discovery order:
     * <ol>
     *   <li>If {@code cdc_pchannel} is configured, try it first; otherwise skip to step 2.</li>
     *   <li>Query etcd for the channel-cp key matching this collection.</li>
     *   <li>Scan pchannels 0-15 to auto-detect the correct one.</li>
     * </ol>
     *
     * <p>Two-step probe per candidate pchannel:
     * <ol>
     *   <li>Call {@code GetReplicateCheckpoint} to verify the StreamingNode service is
     *       reachable. UNIMPLEMENTED (wrong port) or UNAVAILABLE (connection refused)
     *       → return false immediately.</li>
     *   <li>If the service is reachable, try opening a Consume stream.</li>
     * </ol>
     *
     * @return true if StreamingNode service is reachable AND a valid pchannel is found
     */
    @Override
    public boolean isAvailable() {

        // Step 1: Verify StreamingNode service is reachable via GetReplicateCheckpoint.
        // FAILED_PRECONDITION is expected in standalone mode (replication not enabled),
        // but the service is still reachable. UNIMPLEMENTED/UNAVAILABLE means the service
        // is not reachable at all.
        // When pchannelName is not configured, use a standard fallback for the health probe.
        String healthPchan = (pchannelName != null && !pchannelName.isEmpty())
                ? pchannelName
                : "by-dev-rootcoord-dml_0";
        boolean hasConfiguredPchan = pchannelName != null && !pchannelName.isEmpty();
        try {
            PChannelInfo pchannel = PChannelInfo.newBuilder()
                    .setName(healthPchan)
                    .setTerm(1)
                    .build();

            streamingNodeClient.getReplicateCheckpoint(pchannel);
            log.info("StreamingNode service reachable for pchannel={} (checkpoint RPC succeeded)",
                    healthPchan);
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

        // Step 2: Validate the pchannel.
        // Priority: etcd-resolved → configured → auto-discovery (0-15).
        // etcd is the authoritative source for collection→pchannel mapping.
        // A physically valid pchannel may not contain this collection's data,
        // so we MUST prefer the etcd-resolved pchannel over the configured one.
        String resolvedPchan = resolvePchannelFromEtcd();

        // Try etcd-resolved pchannel first (authoritative mapping)
        if (resolvedPchan != null) {
            if (probeConsumeStream(resolvedPchan)) {
                log.info("StreamingNode gRPC available for etcd-resolved pchannel={}", resolvedPchan);
                this.pchannelName = resolvedPchan;
                return true;
            }
        }

        // Try configured pchannel as fallback
        if (hasConfiguredPchan && !pchannelName.equals(resolvedPchan)) {
            if (probeConsumeStream(pchannelName)) {
                log.info("StreamingNode gRPC available for configured pchannel={}", pchannelName);
                return true;
            }
        }

        // Auto-discover: try pchannels 0-15
        log.info("etcd and configured pchannel failed; auto-discovering for collectionId={}",
                collectionId);
        String prefix = extractRootPathFromPchannel();
        for (int i = 0; i < 16; i++) {
            String candidate = prefix + "-rootcoord-dml_" + i;
            if (candidate.equals(resolvedPchan)) continue;
            if (hasConfiguredPchan && candidate.equals(pchannelName)) continue;
            if (probeConsumeStream(candidate)) {
                log.info("Auto-discovered correct pchannel: {} for collectionId={}",
                        candidate, collectionId);
                this.pchannelName = candidate;
                return true;
            }
        }

        log.warn("StreamingNode gRPC service reachable but no valid pchannel found "
                + "for collectionId={} (tried configured={}, etcd={}, and 0-15)",
                collectionId, pchannelName, resolvedPchan);
        return false;
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
            } else {
                // Fallback: try to extract term from FAILED_PRECONDITION stream error
                currentTerm = extractCurrentTermFromStreamError(stream);
                if (currentTerm > 0) {
                    lastStreamCurrentTerm = currentTerm;
                    cachedTerm = currentTerm;
                    log.info("FAILED_PRECONDITION: tried term={}, server current term={}",
                            term, currentTerm);
                }
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
     * Try to extract the server's current term from a FAILED_PRECONDITION stream error.
     * When the server rejects the stream with FAILED_PRECONDITION (e.g., term mismatch),
     * the current term may be embedded in the gRPC status description.
     *
     * @param stream the failed ConsumeStream
     * @return the server's current term, or -1 if it cannot be determined
     */
    private long extractCurrentTermFromStreamError(ConsumeStream stream) {
        if (stream.getGrpcStatusCode() != io.grpc.Status.Code.FAILED_PRECONDITION) {
            return -1;
        }
        Throwable streamError = stream.getStreamError();
        if (!(streamError instanceof io.grpc.StatusRuntimeException)) {
            return -1;
        }
        String desc = ((io.grpc.StatusRuntimeException) streamError).getStatus().getDescription();
        if (desc == null || desc.isEmpty()) {
            return -1;
        }
        java.util.regex.Matcher m = java.util.regex.Pattern
                .compile("current term is (\\d+)", java.util.regex.Pattern.CASE_INSENSITIVE)
                .matcher(desc);
        if (m.find()) {
            try {
                return Long.parseLong(m.group(1));
            } catch (NumberFormatException ignored) {
            }
        }
        return -1;
    }

    /**
     * Extract the root path from the configured pchannel (e.g. "by-dev" from
     * "by-dev-rootcoord-dml_0") or from the Milvus URL as fallback.
     */
    private static String extractRootPath(MilvusCdcSourceConfig config) {
        String pchannel = config.getCdcPchannel();
        if (pchannel != null && pchannel.contains("-rootcoord-dml_")) {
            return pchannel.substring(0, pchannel.indexOf("-rootcoord-dml_"));
        }
        // Default root path for Milvus
        return "by-dev";
    }

    /**
     * Extract the root path from the configured pchannel name, falling back to "by-dev".
     */
    private String extractRootPathFromPchannel() {
        if (pchannelName != null && pchannelName.contains("-rootcoord-dml_")) {
            return pchannelName.substring(0, pchannelName.indexOf("-rootcoord-dml_"));
        }
        return "by-dev";
    }

    /** Recovery retry counter — reset on successful poll, incremented on FAILED_PRECONDITION. */
    private int recoveryRetryCount = 0;
    private static final int MAX_RECOVERY_RETRIES = 3;

    /**
     * Counter for consecutive empty polls. Used to throttle the
     * "Poll completed: 0 events, 0 rows" INFO log so it only fires once
     * every {@link #EMPTY_POLL_INFO_INTERVAL} empty polls (≈1 minute at the
     * default 1s poll interval), instead of every poll cycle. Reset to 0
     * whenever a non-empty poll is observed.
     */
    private int emptyPollCount = 0;
    private static final int EMPTY_POLL_INFO_INTERVAL = 60;

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

        // Open stream if not already open or if startPosition changed.
        // If the previous stream failed with FAILED_PRECONDITION, reset the
        // term cache and re-acquire pchannel info before re-opening.
        if (consumeStream == null || !consumeStream.isOpen()) {
            if (consumeStream != null && !consumeStream.isOpen()
                    && consumeStream.getGrpcStatusCode() == io.grpc.Status.Code.FAILED_PRECONDITION) {
                recoveryRetryCount++;
                if (recoveryRetryCount > MAX_RECOVERY_RETRIES) {
                    log.error("FAILED_PRECONDITION recovery exceeded max retries ({}) for pchannel={}",
                            MAX_RECOVERY_RETRIES, pchannelName);
                    throw new RuntimeException(
                            "FAILED_PRECONDITION recovery exceeded max retries for pchannel="
                                    + pchannelName);
                }
                log.warn("Consume stream closed with FAILED_PRECONDITION for pchannel={}, "
                        + "resetting term cache and re-establishing stream (retry {}/{})",
                        pchannelName, recoveryRetryCount, MAX_RECOVERY_RETRIES);
                // Reset cached term to force fresh term probing from server
                cachedTerm = -1;
                lastStreamCurrentTerm = -1;
                try {
                    consumeStream.close();
                } catch (Exception ignored) {
                }
                consumeStream = null;
            }
            // Resume from last consumed position if recovering, otherwise use caller's position
            if (consumeStream == null) {
                openStream(lastPosition != null ? lastPosition : startPosition);
            } else {
                openStream(startPosition);
            }
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
            // Return partial results — FAILED_PRECONDITION recovery will trigger
            // on the next poll call when the closed stream is detected.
        }

        // Reset recovery counter on successful poll
        if (consumeStream != null && consumeStream.isOpen()) {
            recoveryRetryCount = 0;
        }

        // Throttle the "Poll completed: 0 events, 0 rows" log to avoid log spam
        // during idle incremental periods. Print INFO on the first empty poll
        // (signals transition to idle), then every EMPTY_POLL_INFO_INTERVAL-th
        // empty poll (≈1 minute at default 1s poll interval); other empty
        // polls emit DEBUG only. Non-empty polls always log INFO.
        if (eventCount == 0 && results.isEmpty()) {
            emptyPollCount++;
            if (emptyPollCount == 1 || emptyPollCount % EMPTY_POLL_INFO_INTERVAL == 0) {
                log.info("Poll completed: {} events, {} rows (idle, consecutive empty polls={})",
                        eventCount, results.size(), emptyPollCount);
            } else {
                log.debug("Poll completed: {} events, {} rows (consecutive empty polls={})",
                        eventCount, results.size(), emptyPollCount);
            }
        } else {
            emptyPollCount = 0;
            log.info("Poll completed: {} events, {} rows", eventCount, results.size());
        }
        return results;
    }

    /**
     * Resolve the correct pchannel for this collection from etcd.
     * @return the etcd-resolved pchannel, or null if not found
     */
    private String resolvePchannelFromEtcd() {
        try {
            String resolved = pchannelResolver.resolvePChannel(collectionId);
            if (resolved != null) {
                log.info("Resolved pchannel from etcd: {} for collectionId={}", resolved, collectionId);
                return resolved;
            }
        } catch (Exception e) {
            log.debug("Failed to resolve pchannel from etcd: {}", e.getMessage());
        }
        return null;
    }

    /**
     * Open the Consume stream with the appropriate DeliverPolicy.
     * Auto-detects the correct pchannel: configured → etcd → scan (0-15).
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

        boolean hasConfiguredPchan = pchannelName != null && !pchannelName.isEmpty();

        // Priority: etcd-resolved → cached/configured → auto-discovery (0-15).
        // etcd is the authoritative source for collection→pchannel mapping.

        // Step 1: Try etcd-resolved pchannel first (authoritative mapping)
        String etcdPchan = resolvePchannelFromEtcd();
        if (etcdPchan != null) {
            log.info("Trying etcd-resolved pchannel: {} for collectionId={}", etcdPchan, collectionId);
            if (tryOpenStream(etcdPchan, policy)) {
                log.info("Successfully opened stream on etcd-resolved pchannel: {}", etcdPchan);
                return;
            }
        }

        // Step 2: Try the configured or cached pchannel as fallback
        if (hasConfiguredPchan && !pchannelName.equals(etcdPchan)) {
            if (tryOpenStream(pchannelName, policy)) {
                return;
            }
        }

        // Step 3: Auto-detect: try all pchannels (0-15)
        String prefix = extractRootPathFromPchannel();
        log.info("etcd and configured pchannel failed; auto-detecting for collectionId={} (prefix={})",
                collectionId, prefix);
        for (int i = 0; i < 16; i++) {
            String candidatePchannel = prefix + "-rootcoord-dml_" + i;
            if (candidatePchannel.equals(etcdPchan)) continue;
            if (hasConfiguredPchan && candidatePchannel.equals(pchannelName)) continue;
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