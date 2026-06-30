/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import org.apache.seatunnel.connectors.cdc.milvus.proto.DumpMessagesRequest;
import org.apache.seatunnel.connectors.cdc.milvus.proto.DumpMessagesResponse;
import org.apache.seatunnel.connectors.cdc.milvus.proto.GetReplicateInfoRequest;
import org.apache.seatunnel.connectors.cdc.milvus.proto.GetReplicateInfoResponse;
import org.apache.seatunnel.connectors.cdc.milvus.proto.ImmutableMessage;
import org.apache.seatunnel.connectors.cdc.milvus.proto.MessageID;
import org.apache.seatunnel.connectors.cdc.milvus.proto.MilvusServiceGrpc;
import org.apache.seatunnel.connectors.cdc.milvus.proto.ReplicateCheckpoint;
import org.apache.seatunnel.connectors.cdc.milvus.proto.Status;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Low-level gRPC client for the Milvus CDC {@code DumpMessages} and
 * {@code GetReplicateInfo} RPCs.
 *
 * <p>This client uses the migration module's self-generated
 * {@link MilvusServiceGrpc} stub (NOT the SDK's {@code io.milvus.grpc}
 * variant), and manages its own {@link ManagedChannel}. The channel is
 * constructed with {@code grpc-netty-shaded} so it shares the same transport
 * implementation as the SDK at runtime after shading.
 *
 * <p>The client is intentionally minimal: it only exposes the two RPCs needed
 * by {@link CdcEventStreamStrategy}. Higher-level concerns (collection
 * filtering, row conversion, position tracking) live in the strategy and
 * parser classes.
 */
@Slf4j
public class MilvusCdcGrpcClient implements AutoCloseable {

    private static final String AUTHORIZATION_HEADER = "authorization";
    private static final String BEARER_PREFIX = "Bearer ";

    private final ManagedChannel channel;
    private final MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub;
    private final MilvusServiceGrpc.MilvusServiceStub asyncStub;
    private final long channelTimeoutMs;

    public MilvusCdcGrpcClient(
            String url,
            String token,
            long channelTimeoutMs) {
        this.channelTimeoutMs = channelTimeoutMs;
        this.channel = buildChannel(url, token, channelTimeoutMs);
        this.blockingStub = MilvusServiceGrpc.newBlockingStub(channel);
        this.asyncStub = MilvusServiceGrpc.newStub(channel);
    }

    /**
     * Query Milvus for the current replicate checkpoint of a PChannel. Used to
     * bootstrap a CDC stream when no resumable position is available locally.
     *
     * @param sourceClusterId the source cluster ID (may be empty)
     * @param targetPchannel  the physical channel name (e.g. {@code by-dev-rootcoord-dml_0})
     * @return the checkpoint (or salvage checkpoint as fallback), or empty if
     *         neither is available or the RPC fails
     */
    public Optional<ReplicateCheckpoint> getReplicateInfo(
            String sourceClusterId, String targetPchannel) {
        GetReplicateInfoRequest.Builder reqBuilder = GetReplicateInfoRequest.newBuilder()
                .setTargetPchannel(targetPchannel);
        if (sourceClusterId != null && !sourceClusterId.isEmpty()) {
            reqBuilder.setSourceClusterId(sourceClusterId);
        }
        try {
            GetReplicateInfoResponse resp = blockingStub.withDeadlineAfter(
                    channelTimeoutMs, TimeUnit.MILLISECONDS)
                    .getReplicateInfo(reqBuilder.build());
            if (resp.hasCheckpoint() && isUsableCheckpoint(resp.getCheckpoint())) {
                return Optional.of(resp.getCheckpoint());
            }
            if (resp.hasSalvageCheckpoint() && isUsableCheckpoint(resp.getSalvageCheckpoint())) {
                log.info("Falling back to salvage checkpoint for pchannel={}", targetPchannel);
                return Optional.of(resp.getSalvageCheckpoint());
            }
            log.info("GetReplicateInfo returned no usable checkpoint for pchannel={}", targetPchannel);
            return Optional.empty();
        } catch (Exception e) {
            log.warn("GetReplicateInfo RPC failed for pchannel={}: {}", targetPchannel, e.getMessage());
            return Optional.empty();
        }
    }

    private static boolean isUsableCheckpoint(ReplicateCheckpoint cp) {
        return cp != null && !cp.getMessageId().isEmpty() && cp.getPchannel() != null && !cp.getPchannel().isEmpty();
    }

    /**
     * Open a {@code DumpMessages} server-streaming RPC and return an iterator
     * over the contained {@link ImmutableMessage} frames. Status frames are
     * logged and terminate the iteration. The caller must close the returned
     * iterator (or this client) to release the underlying gRPC stream.
     *
     * @param pchannel        the physical channel name
     * @param startMessageId  the exclusive-start message ID (may be built from
     *                        a {@link ReplicateCheckpoint} returned by
     *                        {@link #getReplicateInfo})
     * @param startTimetick   optional lower timetick bound (0 = no lower bound)
     * @param endTimetick     optional upper timetick bound (0 = no upper bound)
     * @return iterator of WAL immutable messages
     */
    public Iterator<ImmutableMessage> dumpMessages(
            String pchannel,
            MessageID startMessageId,
            long startTimetick,
            long endTimetick) {
        DumpMessagesRequest.Builder reqBuilder = DumpMessagesRequest.newBuilder()
                .setPchannel(pchannel)
                .setStartMessageId(startMessageId);
        if (startTimetick > 0) {
            reqBuilder.setStartTimetick(startTimetick);
        }
        if (endTimetick > 0) {
            reqBuilder.setEndTimetick(endTimetick);
        }
        DumpMessagesRequest request = reqBuilder.build();

        // Use a StreamObserver-to-Iterator adapter so we can return a plain
        // Iterator<ImmutableMessage> to callers without exposing gRPC types.
        LinkedBlockingQueue<Frame> queue = new LinkedBlockingQueue<>();
        StreamObserver<DumpMessagesResponse> observer = new StreamObserver<DumpMessagesResponse>() {
            @Override
            public void onNext(DumpMessagesResponse resp) {
                if (resp.hasMessage()) {
                    queue.add(Frame.of(resp.getMessage()));
                } else if (resp.hasStatus()) {
                    Status status = resp.getStatus();
                    log.info("DumpMessages status frame: code={} reason={} (terminating stream)",
                            status.getCode(), status.getReason());
                    queue.add(Frame.endOfStream());
                }
            }

            @Override
            public void onError(Throwable t) {
                log.warn("DumpMessages stream error: {}", t.getMessage());
                queue.add(Frame.error(t));
            }

            @Override
            public void onCompleted() {
                queue.add(Frame.endOfStream());
            }
        };

        asyncStub.dumpMessages(request, observer);

        return new FrameIterator(queue);
    }

    /**
     * Build a {@link MessageID} proto from a {@link ReplicateCheckpoint}'s
     * raw {@code message_id} bytes. The {@code wALName} field is set to
     * {@code Unknown} because the checkpoint bytes are WAL-specific and the
     * exact implementation is determined by the server.
     */
    public static MessageID messageIDFromCheckpoint(ReplicateCheckpoint cp) {
        return MessageID.newBuilder()
                .setId(new String(cp.getMessageId().toByteArray()))
                .build();
    }

    @Override
    public void close() {
        if (channel != null && !channel.isShutdown()) {
            channel.shutdown();
            try {
                if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                    channel.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                channel.shutdownNow();
            }
        }
    }

    // ---- internals ----

    private static ManagedChannel buildChannel(String url, String token, long timeoutMs) {
        HostPort hp = parseUrl(url);
        // Use ManagedChannelBuilder (not NettyChannelBuilder directly) so the
        // transport is auto-discovered via SPI. This works with both the
        // regular grpc-netty and the shaded grpc-netty-shaded artifacts.
        ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forAddress(hp.host, hp.port);

        if (hp.tls) {
            builder.useTransportSecurity();
        } else {
            builder.usePlaintext();
        }

        // Attach authorization header if a token is provided. Milvus accepts
        // either "Bearer <token>" or a raw token in the authorization header.
        if (token != null && !token.isEmpty()) {
            Metadata metadata = new Metadata();
            String value = token.startsWith(BEARER_PREFIX) || token.contains(" ")
                    ? token : BEARER_PREFIX + token;
            metadata.put(Key.of(AUTHORIZATION_HEADER, Metadata.ASCII_STRING_MARSHALLER), value);
            builder.intercept(MetadataUtils.newAttachHeadersInterceptor(metadata));
        }
        return builder.build();
    }

    private static HostPort parseUrl(String url) {
        if (url == null || url.isEmpty()) {
            throw new IllegalArgumentException("Milvus URL must not be empty");
        }
        String trimmed = url.trim();
        boolean tls = false;
        String host;
        int port = 19530;
        try {
            URI uri = new URI(trimmed);
            String scheme = uri.getScheme();
            if (scheme != null) {
                String lower = scheme.toLowerCase();
                if (lower.equals("https") || lower.equals("grpcs") || lower.equals("tls")) {
                    tls = true;
                }
            }
            String hostPart = uri.getHost();
            if (hostPart == null || hostPart.isEmpty()) {
                // Strip scheme and try again
                String stripped = trimmed.replaceAll("^[a-zA-Z]+://", "");
                int colon = stripped.lastIndexOf(':');
                if (colon > 0 && stripped.endsWith("]")) {
                    hostPart = stripped; // IPv6
                } else if (colon > 0) {
                    hostPart = stripped.substring(0, colon);
                    try {
                        port = Integer.parseInt(stripped.substring(colon + 1));
                    } catch (NumberFormatException ignored) {
                        // ignore
                    }
                } else {
                    hostPart = stripped;
                }
            } else if (uri.getPort() > 0) {
                port = uri.getPort();
            }
            host = hostPart;
            if (host.startsWith("[") && host.endsWith("]")) {
                host = host.substring(1, host.length() - 1);
            }
        } catch (URISyntaxException e) {
            // Fallback: split on last colon
            int colon = trimmed.lastIndexOf(':');
            if (colon > 0) {
                host = trimmed.substring(0, colon);
                try {
                    port = Integer.parseInt(trimmed.substring(colon + 1));
                } catch (NumberFormatException ignored) {
                    // ignore
                }
            } else {
                host = trimmed;
            }
        }
        return new HostPort(host, port, tls);
    }

    private static final class HostPort {
        final String host;
        final int port;
        final boolean tls;

        HostPort(String host, int port, boolean tls) {
            this.host = host;
            this.port = port;
            this.tls = tls;
        }
    }

    /** A single frame in the gRPC stream: a message, end-of-stream, or error. */
    private static final class Frame {
        final ImmutableMessage message;
        final Throwable error;
        final boolean endOfStream;

        private Frame(ImmutableMessage message, Throwable error, boolean endOfStream) {
            this.message = message;
            this.error = error;
            this.endOfStream = endOfStream;
        }

        static Frame of(ImmutableMessage msg) {
            return new Frame(msg, null, false);
        }

        static Frame error(Throwable t) {
            return new Frame(null, t, false);
        }

        static Frame endOfStream() {
            return new Frame(null, null, true);
        }
    }

    /** Iterator over the gRPC stream frames that unwraps messages and surfaces errors. */
    private static final class FrameIterator implements Iterator<ImmutableMessage> {
        private final LinkedBlockingQueue<Frame> queue;
        private Frame nextFrame;
        private boolean terminated;

        FrameIterator(LinkedBlockingQueue<Frame> queue) {
            this.queue = queue;
        }

        @Override
        public boolean hasNext() {
            if (nextFrame != null) {
                return true;
            }
            if (terminated) {
                return false;
            }
            try {
                nextFrame = queue.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for DumpMessages frame", e);
            }
            if (nextFrame.error != null) {
                terminated = true;
                if (nextFrame.error instanceof RuntimeException) {
                    throw (RuntimeException) nextFrame.error;
                }
                throw new RuntimeException(nextFrame.error);
            }
            if (nextFrame.endOfStream) {
                terminated = true;
                return false;
            }
            return true;
        }

        @Override
        public ImmutableMessage next() {
            if (!hasNext()) {
                throw new NoSuchElementException("DumpMessages stream exhausted");
            }
            ImmutableMessage msg = nextFrame.message;
            nextFrame = null;
            return msg;
        }
    }
}
