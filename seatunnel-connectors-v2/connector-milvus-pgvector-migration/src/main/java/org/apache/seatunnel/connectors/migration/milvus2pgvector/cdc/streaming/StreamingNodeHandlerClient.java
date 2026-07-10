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

import org.apache.seatunnel.connectors.streaming.proto.ConsumeRequest;
import org.apache.seatunnel.connectors.streaming.proto.ConsumeResponse;
import org.apache.seatunnel.connectors.streaming.proto.CreateConsumerRequest;
import org.apache.seatunnel.connectors.streaming.proto.CreateVChannelConsumerRequest;
import org.apache.seatunnel.connectors.streaming.proto.DeliverPolicy;
import org.apache.seatunnel.connectors.streaming.proto.GetReplicateCheckpointRequest;
import org.apache.seatunnel.connectors.streaming.proto.GetReplicateCheckpointResponse;
import org.apache.seatunnel.connectors.streaming.proto.ImmutableMessage;
import org.apache.seatunnel.connectors.streaming.proto.PChannelAccessMode;
import org.apache.seatunnel.connectors.streaming.proto.PChannelInfo;
import org.apache.seatunnel.connectors.streaming.proto.ReplicateCheckpoint;
import org.apache.seatunnel.connectors.streaming.proto.StreamingNodeHandlerServiceGrpc;

import com.google.protobuf.Empty;
import com.google.protobuf.ByteString;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.TlsChannelCredentials;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Low-level gRPC client for the Milvus StreamingNode {@code StreamingNodeHandlerService}.
 * This client provides access to WAL messages via the {@code Consume} streaming RPC,
 * which is available in both standalone and replication topology modes (unlike
 * DumpMessages which only works in replication topology).
 *
 * <p>Supports TLS and mTLS via {@code ca_pem_path}, {@code client_pem_path},
 * and {@code client_key_path} configuration options (since 2.3.11).
 */
@Slf4j
public class StreamingNodeHandlerClient implements AutoCloseable {

    private static final String AUTHORIZATION_HEADER = "authorization";
    private static final String CREATE_CONSUMER_HEADER = "create-consumer";

    private final ManagedChannel channel;
    private final StreamingNodeHandlerServiceGrpc.StreamingNodeHandlerServiceStub asyncStub;
    private final StreamingNodeHandlerServiceGrpc.StreamingNodeHandlerServiceBlockingStub blockingStub;
    private final long channelTimeoutMs;

    /**
     * Constructor with full TLS/mTLS support.
     *
     * @param url              StreamingNode URL (e.g., "http://localhost:19531")
     * @param token            Authentication token (Bearer token)
     * @param channelTimeoutMs Timeout for unary RPCs in milliseconds
     * @param caPemPath        CA certificate PEM file path (null/empty = system trust store)
     * @param clientPemPath    Client certificate PEM file path for mTLS (null/empty = no mTLS)
     * @param clientKeyPath    Client private key PEM file path for mTLS (null/empty = no mTLS)
     * @param serverName       Server name override for TLS SNI / hostname verification
     */
    public StreamingNodeHandlerClient(String url, String token, long channelTimeoutMs,
                                       String caPemPath, String clientPemPath,
                                       String clientKeyPath, String serverName) {
        this.channelTimeoutMs = channelTimeoutMs;
        this.channel = buildChannel(url, token, channelTimeoutMs,
                caPemPath, clientPemPath, clientKeyPath, serverName);
        this.asyncStub = StreamingNodeHandlerServiceGrpc.newStub(channel);
        this.blockingStub = StreamingNodeHandlerServiceGrpc.newBlockingStub(channel);
        log.info("StreamingNodeHandlerClient initialized for URL: {} (tls={})",
                url, SslUtil.hasTlsConfig(caPemPath, clientPemPath, clientKeyPath));
    }

    /**
     * Build a gRPC ManagedChannel with Bearer token authentication and optional TLS/mTLS.
     * Handles various URL formats: "http://host:port", "https://host:port", "host:port", "host".
     *
     * <p>When TLS certificates are configured, the channel uses {@link TlsChannelCredentials}
     * for secure transport; otherwise plaintext is used.
     */
    private ManagedChannel buildChannel(String url, String token, long timeoutMs,
                                         String caPemPath, String clientPemPath,
                                         String clientKeyPath, String serverName) {
        String host;
        int port;

        if (url == null || url.isEmpty()) {
            throw new IllegalArgumentException("StreamingNode URL is null or empty");
        }

        try {
            String parsedUrl = url;
            if (!parsedUrl.contains("://")) {
                parsedUrl = "grpc://" + parsedUrl;
            }
            URI uri = new URI(parsedUrl);
            host = uri.getHost();
            port = uri.getPort() > 0 ? uri.getPort() : 22222;
        } catch (URISyntaxException e) {
            String[] parts = url.split(":");
            host = parts[0];
            port = parts.length > 1 ? Integer.parseInt(parts[1]) : 22222;
        }

        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException("Cannot parse host from URL: " + url);
        }

        log.info("Building gRPC channel to StreamingNode at {}:{}", host, port);

        ManagedChannelBuilder<?> builder;

        boolean hasTls = SslUtil.hasTlsConfig(caPemPath, clientPemPath, clientKeyPath);
        if (hasTls) {
            TlsChannelCredentials.Builder tlsBuilder = TlsChannelCredentials.newBuilder();
            try {
                if (SslUtil.hasCaConfig(caPemPath)) {
                    tlsBuilder.trustManager(new File(caPemPath));
                }
                if (SslUtil.hasMtlsConfig(clientPemPath, clientKeyPath)) {
                    tlsBuilder.keyManager(new File(clientPemPath), new File(clientKeyPath));
                }
            } catch (java.io.IOException e) {
                throw new IllegalArgumentException(
                        "Failed to load TLS certificate files for StreamingNode", e);
            }
            builder = Grpc.newChannelBuilder(host + ":" + port, tlsBuilder.build());
            log.info("TLS enabled for StreamingNode channel (ca={}, mTLS={})",
                    SslUtil.hasCaConfig(caPemPath),
                    SslUtil.hasMtlsConfig(clientPemPath, clientKeyPath));
        } else {
            builder = ManagedChannelBuilder.forAddress(host, port).usePlaintext();
        }

        if (serverName != null && !serverName.isEmpty()) {
            builder.overrideAuthority(serverName);
        }

        builder.keepAliveTime(timeoutMs, TimeUnit.MILLISECONDS);

        if (token != null && !token.isEmpty()) {
            Metadata metadata = new Metadata();
            Metadata.Key<String> key =
                    Metadata.Key.of(AUTHORIZATION_HEADER, Metadata.ASCII_STRING_MARSHALLER);
            // Token format per Milvus auth interceptor: base64(username:password)
            // No "Bearer " prefix — the interceptor does not strip it.
            String encodedToken = Base64.getEncoder()
                    .encodeToString(token.getBytes(StandardCharsets.UTF_8));
            metadata.put(key, encodedToken);
            builder.intercept(MetadataUtils.newAttachHeadersInterceptor(metadata));
        }

        return builder.build();
    }

    /**
     * Get the current replicate checkpoint for a PChannel.
     * This checkpoint can be used as the starting position for a Consume stream.
     *
     * @param pchannel PChannel information (name, term, access_mode)
     * @return Optional checkpoint (empty if no checkpoint exists)
     * @throws io.grpc.StatusRuntimeException if gRPC call fails (UNIMPLEMENTED, UNAVAILABLE, etc.)
     */
    public Optional<ReplicateCheckpoint> getReplicateCheckpoint(PChannelInfo pchannel) {
        GetReplicateCheckpointRequest request = GetReplicateCheckpointRequest.newBuilder()
                .setPchannel(pchannel)
                .build();

        GetReplicateCheckpointResponse response = blockingStub
                .withDeadlineAfter(channelTimeoutMs, TimeUnit.MILLISECONDS)
                .getReplicateCheckpoint(request);

        if (response.hasCheckpoint() && isValidCheckpoint(response.getCheckpoint())) {
            log.info("GetReplicateCheckpoint succeeded for pchannel={}, checkpoint available",
                    pchannel.getName());
            return Optional.of(response.getCheckpoint());
        } else {
            log.info("GetReplicateCheckpoint returned empty checkpoint for pchannel={}",
                    pchannel.getName());
            return Optional.empty();
        }
    }

    /**
     * Check if a checkpoint is valid (non-null, non-empty).
     */
    private boolean isValidCheckpoint(ReplicateCheckpoint checkpoint) {
        return checkpoint != null
                && !checkpoint.getPchannel().isEmpty()
                && !checkpoint.getMessageId().isEmpty();
    }

    /**
     * Create a Consume stream for reading WAL messages.
     *
     * <p>The Milvus StreamingNode Consume RPC requires:
     * <ol>
     *   <li><b>gRPC metadata</b>: A {@code create-consumer} header containing a
     *       base64-encoded {@link CreateConsumerRequest} protobuf message (with PChannelInfo).</li>
     *   <li><b>First stream message</b>: A {@link ConsumeRequest} with
     *       {@code CreateVChannelConsumerRequest} set (with vchannel + DeliverPolicy).</li>
     * </ol>
     *
     * @param pchannel  PChannel information (name, term, access_mode)
     * @param vchannel  Virtual channel name (format: {pchannel}_{collectionID}v{idx})
     * @param policy    DeliverPolicy: all (full snapshot) or startAfter (checkpoint recovery)
     * @return ConsumeStream instance (iterator over ImmutableMessage)
     */
    public ConsumeStream createConsumeStream(PChannelInfo pchannel, String vchannel, DeliverPolicy policy) {
        log.info("Creating ConsumeStream for pchannel={}, vchannel={}, policy={}",
                pchannel.getName(), vchannel, policy.getPolicyCase());

        // 1. Build CreateConsumerRequest for gRPC metadata header.
        // Milvus StreamingNode uses a CUSTOM base64 encoding scheme for the
        // "create-consumer" metadata header (NOT the gRPC-standard "-bin" suffix).
        // The server manually base64-decodes the value using:
        //   md.Get("create-consumer") → base64.StdEncoding.DecodeString → proto.Unmarshal
        CreateConsumerRequest createConsumerReq = CreateConsumerRequest.newBuilder()
                .setPchannel(pchannel)
                .build();
        String base64Value = java.util.Base64.getEncoder()
                .encodeToString(createConsumerReq.toByteArray());

        // 2. Create gRPC metadata with manually base64-encoded protobuf value.
        // Use ASCII_STRING_MARSHALLER (not BINARY_BYTE_MARSHALLER) because
        // the server expects a plain-text base64 string, not binary bytes.
        Metadata metadata = new Metadata();
        Metadata.Key<String> createConsumerKey =
                Metadata.Key.of(CREATE_CONSUMER_HEADER, Metadata.ASCII_STRING_MARSHALLER);
        metadata.put(createConsumerKey, base64Value);

        // 3. Create async stub with metadata interceptor
        StreamingNodeHandlerServiceGrpc.StreamingNodeHandlerServiceStub stubWithMetadata =
                asyncStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));

        // 4. Create VChannel consumer request (first stream message).
        ConsumeRequest createRequest = ConsumeRequest.newBuilder()
                .setCreateVchannelConsumer(CreateVChannelConsumerRequest.newBuilder()
                        .setVchannel(vchannel)
                        .setDeliverPolicy(policy)
                        .build())
                .build();

        return new ConsumeStream(stubWithMetadata, createRequest, pchannel.getName());
    }

    /**
     * Close the gRPC channel and release resources.
     */
    @Override
    public void close() {
        if (channel != null && !channel.isShutdown()) {
            try {
                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
                log.info("StreamingNodeHandlerClient closed");
            } catch (InterruptedException e) {
                channel.shutdownNow();
                log.warn("StreamingNodeHandlerClient shutdown interrupted");
            }
        }
    }
}