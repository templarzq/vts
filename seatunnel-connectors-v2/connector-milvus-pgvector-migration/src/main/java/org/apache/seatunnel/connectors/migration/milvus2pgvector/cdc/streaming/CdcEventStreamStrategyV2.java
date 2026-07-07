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
    private final long collectionId;
    private final int shardsNum;
    private final long maxEventsPerPoll;
    private final String sourceClusterId;
    private final PChannelResolver pchannelResolver;

    /** Live Consume stream cursors — one per shard; kept across polls to avoid re-opening. */
    private final java.util.List<ConsumeStream> consumeStreams = new java.util.ArrayList<>();

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
            MilvusCdcSourceConfig config, DescribeCollectionResp collectionDesc,
            String tableId) {
        this.pchannelName = config.getCdcPchannel();
        this.collectionId = collectionDesc.getCollectionID();
        // Default to 1 shard if not specified (Milvus default)
        this.shardsNum = collectionDesc.getShardsNum() != null
                ? collectionDesc.getShardsNum() : 1;
        this.maxEventsPerPoll = config.getIncrementalBatchSize();
        this.sourceClusterId = config.getCdcSourceClusterId();

        String streamingNodeAddress = config.getStreamingNodeAddress() != null
                ? config.getStreamingNodeAddress()
                : config.getUrl();

        // StreamingNode TLS: only pass cert paths when explicitly enabled.
        // In standalone Milvus, the StreamingNode gRPC port (22222) is plaintext
        // even when the main Milvus port (19530) has TLS enabled.
        final boolean useTls = Boolean.TRUE.equals(config.getStreamingNodeUseTls());
        final String snCaPath = useTls ? config.getCaPemPath() : null;
        final String snCertPath = useTls ? config.getClientPemPath() : null;
        final String snKeyPath = useTls ? config.getClientKeyPath() : null;
        final String snServerName = useTls ? config.getServerName() : null;

        this.streamingNodeClient = new StreamingNodeHandlerClient(
                streamingNodeAddress,
                config.getToken(),
                config.getChannelTimeoutMs(),
                snCaPath,
                snCertPath,
                snKeyPath,
                snServerName);

        this.parser = new StreamingMessageParser(collectionDesc, config.getPrimaryKeyField());
        this.parser.setTableId(tableId);

        // Initialize pchannel resolver for etcd-based auto-discovery
        this.pchannelResolver = new PChannelResolver(
                config.getCdcEtcdEndpoint(),
                extractRootPath(config),
                config.getCdcEtcdCaPath(),
                config.getCdcEtcdClientCertPath(),
                config.getCdcEtcdClientKeyPath(),
                config.getCdcEtcdUsername(),
                config.getCdcEtcdPassword());

        log.info("CdcEventStreamStrategyV2 initialized: pchannel={}, collectionId={}, shardsNum={}, tableId={}, streamingNode={}",
                pchannelName, collectionId, shardsNum, tableId, streamingNodeAddress);
    }

    /** Build vchannel names from pchannel + collectionId + shard index. */
    private java.util.List<String> buildVchannelNames(String pchannel) {
        java.util.List<String> names = new java.util.ArrayList<>(shardsNum);
        for (int i = 0; i < shardsNum; i++) {
            names.add(pchannel + "_" + collectionId + "v" + i);
        }
        return names;
    }

    /**
     * Check if StreamingNode gRPC is available and the pchannel is valid.
     *
     * <p>Version compatibility matrix (auto-detected via capability probing, no
     * version-string parsing):
     * <ul>
     *   <li><b>Milvus &lt; 2.5.5</b> — StreamingNodeHandlerService does not exist.
     *       Both GetReplicateCheckpoint and Consume return UNIMPLEMENTED → return false.</li>
     *   <li><b>Milvus 2.5.5+</b> — StreamingNodeHandlerService.Consume exists, but
     *       GetReplicateCheckpoint RPC is not yet introduced. GetReplicateCheckpoint
     *       returns UNIMPLEMENTED, but Consume probing succeeds → return true.</li>
     *   <li><b>Milvus 2.6.0+</b> — Full support: GetReplicateCheckpoint succeeds
     *       (or FAILED_PRECONDITION in standalone mode) → return true.</li>
     * </ul>
     *
     * <p>Discovery order for a valid pchannel:
     * <ol>
     *   <li>If {@code cdc_pchannel} is configured, try it first; otherwise skip to step 2.</li>
     *   <li>Query etcd for the channel-cp key matching this collection.</li>
     *   <li>Scan pchannels 0-15 to auto-detect the correct one.</li>
     * </ol>
     *
     * <p>Two-step probe per candidate pchannel:
     * <ol>
     *   <li>Call {@code GetReplicateCheckpoint} (if available) to verify the
     *       StreamingNode service is reachable. FAILED_PRECONDITION (2.6 standalone)
     *       and UNIMPLEMENTED (2.5.5 — RPC absent but service exists) both proceed
     *       to Consume probing. UNAVAILABLE (service not started) → return false.</li>
     *   <li>Try opening a Consume stream — this is the authoritative capability test.</li>
     * </ol>
     *
     * @return true if StreamingNode service is reachable AND a valid pchannel is found
     */
    @Override
    public boolean isAvailable() {

        // Step 1: Probe StreamingNode service reachability via GetReplicateCheckpoint.
        // This RPC only exists in Milvus >= 2.6. Its response code distinguishes:
        //   - OK / FAILED_PRECONDITION → Milvus 2.6+, service reachable (proceed)
        //   - UNIMPLEMENTED            → Milvus 2.5.5 (Consume exists) or 2.4 (no service).
        //                                 Must probe Consume to distinguish.
        //   - UNAVAILABLE              → Service not started / wrong port (give up).
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
            log.info("StreamingNode service reachable for pchannel={} (GetReplicateCheckpoint succeeded, Milvus >= 2.6)",
                    healthPchan);
        } catch (io.grpc.StatusRuntimeException e) {
            io.grpc.Status.Code code = e.getStatus().getCode();
            if (code == io.grpc.Status.Code.FAILED_PRECONDITION) {
                // Milvus 2.6.x standalone: replication not enabled, but service is reachable.
                log.info("StreamingNode service reachable (FAILED_PRECONDITION on checkpoint, "
                        + "proceeding to pchannel validation via Consume RPC)");
            } else if (code == io.grpc.Status.Code.UNIMPLEMENTED) {
                // GetReplicateCheckpoint RPC absent. Two possibilities:
                //   - Milvus 2.5.5+: StreamingNodeHandlerService exists (Consume available)
                //     but GetReplicateCheckpoint not yet introduced.
                //   - Milvus < 2.5.5: StreamingNodeHandlerService does not exist at all.
                // The Consume-based probing below distinguishes the two cases: 2.5.5 will
                // pass, 2.4 will fail and isAvailable() returns false (caller falls back to
                // polling_incremental).
                log.info("GetReplicateCheckpoint UNIMPLEMENTED (Milvus < 2.6 or StreamingNode absent); "
                        + "proceeding to Consume RPC probing for capability detection");
            } else {
                // UNAVAILABLE etc.: StreamingNode service not reachable at all.
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
        String resolvedPchan;
        try {
            resolvedPchan = resolvePchannelFromEtcd();
        } catch (Exception e) {
            log.warn("etcd pchannel resolution failed, strategy unavailable: {}", e.getMessage());
            return false;
        }

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
                } else {
                    // Term extraction failed — log full error details for debugging
                    Throwable streamErr = stream.getStreamError();
                    String errMsg = streamErr != null ? streamErr.getMessage() : "null";
                    String errDesc = (streamErr instanceof io.grpc.StatusRuntimeException)
                            ? ((io.grpc.StatusRuntimeException) streamErr).getStatus().getDescription()
                            : "null";
                    log.warn("Failed to extract current term from stream error. "
                            + "triedTerm={}, gRPC status={}, description={}, streamError={}",
                            term, stream.getGrpcStatusCode(), errDesc, errMsg);
                }
            }

            try {
                stream.close();
            } catch (Exception ignored) {
            }
        } catch (Exception e) {
            log.warn("Failed to create Consume stream for pchannel={}, term={}: {}",
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

        // Open streams if not already open
        if (consumeStreams.isEmpty() || !consumeStreams.get(0).isOpen()) {
            // Check for FAILED_PRECONDITION recovery
            if (!consumeStreams.isEmpty() && !consumeStreams.get(0).isOpen()
                    && consumeStreams.get(0).getGrpcStatusCode() == io.grpc.Status.Code.FAILED_PRECONDITION) {
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
                cachedTerm = -1;
                lastStreamCurrentTerm = -1;
                closeStreams();
            }
            // Open streams for all vchannels
            openStreams(lastPosition != null ? lastPosition : startPosition);
        }

        List<SeaTunnelRowWithPosition> results = new ArrayList<>();
        int eventCount = 0;

        // Poll from all shard streams, round-robin
        long eventsPerStream = Math.max(1, maxEventsPerPoll / consumeStreams.size());
        for (ConsumeStream stream : consumeStreams) {
            if (!stream.isOpen()) continue;
            long streamEvents = 0;
            try {
                while (stream.hasNext() && streamEvents < eventsPerStream
                        && (eventCount + streamEvents) < maxEventsPerPoll) {
                    ImmutableMessage message = stream.next();
                    eventCount++;
                    streamEvents++;

                    List<SeaTunnelRow> parsedRows = parser.parseMessage(message);
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

                    for (SeaTunnelRow row : parsedRows) {
                        results.add(new SeaTunnelRowWithPosition(row, position));
                    }
                }
            } catch (io.grpc.StatusRuntimeException e) {
                log.warn("gRPC error polling from stream: {}", e.getMessage());
            } catch (RuntimeException e) {
                log.error("Fatal error polling changes: {}", e.getMessage());
                throw e;
            } catch (Exception e) {
                log.warn("Error polling from stream: {}", e.getMessage());
            }
        }

        // Reset recovery counter on successful poll
        if (!consumeStreams.isEmpty() && consumeStreams.get(0).isOpen()) {
            recoveryRetryCount = 0;
        }

        if (results.isEmpty()) {
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
            log.info("Poll completed: {} events, {} rows ({} streams, {} shards)",
                    eventCount, results.size(), consumeStreams.size(), shardsNum);
        }
        return results;
    }

    /**
     * Resolve the correct pchannel for this collection from etcd.
     * @return the etcd-resolved pchannel, or null if not found in etcd
     * @throws RuntimeException if etcd is unreachable or connection fails
     */
    private String resolvePchannelFromEtcd() {
        try {
            String resolved = pchannelResolver.resolvePChannel(collectionId);
            if (resolved != null) {
                log.info("Resolved pchannel from etcd: {} for collectionId={}", resolved, collectionId);
                return resolved;
            }
        } catch (RuntimeException e) {
            log.error("Failed to resolve pchannel from etcd: {}. "
                    + "Check etcd endpoint configuration and connectivity.", e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("Failed to resolve pchannel from etcd: {}. "
                    + "Check etcd endpoint configuration and connectivity.", e.getMessage());
            throw new RuntimeException("Failed to resolve pchannel from etcd", e);
        }
        return null;
    }

    /**
     * Open Consume streams for all vchannels (one per shard) with the appropriate DeliverPolicy.
     * Auto-detects the correct pchannel: etcd → configured → scan (0-15).
     */
    private void openStreams(ReplicatePosition startPosition) {
        DeliverPolicy policy;
        if (startPosition != null && startPosition.getMessageId() != null
                && !startPosition.getMessageId().isEmpty()) {
            MessageID startMessageId = MessageID.newBuilder()
                    .setId(ByteString.copyFromUtf8(startPosition.getMessageId()))
                    .build();
            policy = DeliverPolicy.newBuilder()
                    .setStartAfter(startMessageId)
                    .build();
            log.info("Opening Consume streams for incremental sync: startAfter messageId={}, shards={}",
                    startPosition.getMessageId(), shardsNum);
        } else {
            policy = DeliverPolicy.newBuilder()
                    .setAll(Empty.newBuilder().build())
                    .build();
            log.info("Opening Consume streams for full snapshot: DeliverPolicy.all, shards={}", shardsNum);
        }

        boolean hasConfiguredPchan = pchannelName != null && !pchannelName.isEmpty();

        // Resolve pchannel
        String etcdPchan = resolvePchannelFromEtcd();
        String resolvedPchannel = null;

        if (etcdPchan != null) {
            resolvedPchannel = etcdPchan;
        } else if (hasConfiguredPchan) {
            resolvedPchannel = pchannelName;
        } else {
            // Auto-detect pchannel
            String prefix = extractRootPathFromPchannel();
            for (int i = 0; i < 16; i++) {
                String candidate = prefix + "-rootcoord-dml_" + i;
                if (probeConsumeStream(candidate)) {
                    resolvedPchannel = candidate;
                    break;
                }
            }
        }

        if (resolvedPchannel == null) {
            log.error("Failed to resolve pchannel for collectionId={}", collectionId);
            throw new RuntimeException("Failed to open CDC stream: no valid pchannel found");
        }

        this.pchannelName = resolvedPchannel;
        java.util.List<String> vchannels = buildVchannelNames(resolvedPchannel);
        log.info("Opening {} Consume streams for {} vchannels on pchannel={}",
                vchannels.size(), vchannels, resolvedPchannel);

        closeStreams();
        for (String vchannel : vchannels) {
            ConsumeStream stream = tryCreateStreamForChannel(resolvedPchannel, vchannel, policy);
            if (stream != null) {
                consumeStreams.add(stream);
            } else {
                log.error("Failed to open stream for vchannel={}", vchannel);
                throw new RuntimeException("Failed to open CDC stream for vchannel=" + vchannel);
            }
        }
    }

    /** Try open a single stream using term probing. */
    private ConsumeStream tryCreateStreamForChannel(String pchannel, String vchannel,
                                                     DeliverPolicy policy) {
        // Try cached term first
        long ct = cachedTerm;
        if (ct > 0) {
            ConsumeStream stream = tryCreateStream(pchannel, vchannel, policy, ct);
            if (stream != null) return stream;
            cachedTerm = -1;
        }
        // Try term=1
        ConsumeStream stream = tryCreateStream(pchannel, vchannel, policy, 1);
        if (stream != null) return stream;
        // Try server-reported current term
        long currentTerm = lastStreamCurrentTerm;
        if (currentTerm > 0 && currentTerm != 1) {
            stream = tryCreateStream(pchannel, vchannel, policy, currentTerm);
            if (stream != null) return stream;
        }
        log.warn("Failed to open Consume stream for pchannel={}, vchannel={}",
                pchannel, vchannel);
        return null;
    }

    private void closeStreams() {
        for (ConsumeStream s : consumeStreams) {
            try { s.close(); } catch (Exception ignored) { }
        }
        consumeStreams.clear();
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
     * Capture the current WAL position by briefly opening a Consume stream and
     * reading one message. Used to establish the SnapStartPosition before snapshot
     * begins, so the incremental phase can start from this position rather than
     * replaying the entire WAL.
     *
     * @param collectionName collection to capture position for
     * @return current WAL position, or null if unavailable
     */
    @Override
    public ReplicatePosition captureCurrentPosition(String collectionName) {
        if (pchannelName == null || pchannelName.isEmpty()) {
            // Auto-discover pchannel first if not yet resolved
            String etcdPchan = resolvePchannelFromEtcd();
            if (etcdPchan != null) {
                pchannelName = etcdPchan;
            } else {
                log.warn("Cannot capture SnapStartPosition: no pchannel resolved for collectionId={}",
                        collectionId);
                return null;
            }
        }

        // Use v0 (first shard) for position capture — all shards share the same WAL timeline
        String vchan = pchannelName + "_" + collectionId + "v0";
        DeliverPolicy policy = DeliverPolicy.newBuilder()
                .setAll(Empty.newBuilder().build())
                .build();

        ConsumeStream tempStream = null;
        try {
            tempStream = tryCreateStream(pchannelName, vchan, policy,
                    cachedTerm > 0 ? cachedTerm : 1);
            if (tempStream == null) {
                long ct = lastStreamCurrentTerm;
                if (ct > 0) {
                    tempStream = tryCreateStream(pchannelName, vchan, policy, ct);
                }
            }
            if (tempStream == null || !tempStream.isOpen()) {
                log.warn("Failed to open temporary Consume stream for SnapStartPosition capture");
                return null;
            }

            // Wait for the first data message with timeout (handles high-latency environments).
            // Barrier/checkpoint messages are skipped to get the actual data position.
            long deadline = System.currentTimeMillis() + 5000;
            while (System.currentTimeMillis() < deadline) {
                if (tempStream.hasNext()) {
                    ImmutableMessage msg = tempStream.next();
                    // Skip barrier/checkpoint messages (no data rows)
                    List<SeaTunnelRow> parsedRows = parser.parseMessage(msg);
                    if (parsedRows.isEmpty()) {
                        log.debug("Skipping barrier/checkpoint message during SnapStartPosition capture");
                        continue;
                    }
                    MessageID msgId = parser.extractMessageID(msg);
                    long timetick = parser.extractTimetick(msg);
                    ReplicatePosition pos = ReplicatePosition.builder()
                            .clusterId(sourceClusterId)
                            .pchannel(pchannelName)
                            .messageId(msgId.getId().toStringUtf8())
                            .timeTick(timetick)
                            .timestamp(System.currentTimeMillis())
                            .build();
                    log.info("Captured SnapStartPosition: pchannel={}, messageId={}, timeTick={}",
                            pchannelName, pos.getMessageId(), timetick);
                    return pos;
                }
                Thread.sleep(100);
            }
            log.warn("Timed out waiting for first data message from temporary Consume stream "
                    + "({}ms). WAL may be empty or latency too high.", 5000);
            return null;
        } catch (Exception e) {
            log.warn("Failed to capture SnapStartPosition: {}", e.getMessage());
            return null;
        } finally {
            if (tempStream != null) {
                try {
                    tempStream.close();
                } catch (Exception ignored) {
                }
            }
        }
    }

    /**
     * Close the CDC stream and release resources.
     */
    @Override
    public void close() {
        closeStreams();
        log.info("All {} Consume streams closed for pchannel={}", shardsNum, pchannelName);

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