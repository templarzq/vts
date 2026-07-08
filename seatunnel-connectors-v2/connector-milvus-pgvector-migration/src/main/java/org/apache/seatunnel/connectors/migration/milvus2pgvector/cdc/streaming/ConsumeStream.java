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
import org.apache.seatunnel.connectors.streaming.proto.CreateVChannelConsumerResponse;
import org.apache.seatunnel.connectors.streaming.proto.ImmutableMessage;
import org.apache.seatunnel.connectors.streaming.proto.StreamingCode;
import org.apache.seatunnel.connectors.streaming.proto.StreamingError;
import org.apache.seatunnel.connectors.streaming.proto.StreamingNodeHandlerServiceGrpc;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ConsumeStream wraps a bidirectional gRPC stream for reading WAL messages.
 * The client sends control requests (CreateVChannelConsumer, CloseConsumer),
 * and the server streams back ConsumeResponse messages containing ImmutableMessage.
 *
 * This class implements Iterator<ImmutableMessage> for easy consumption:
 *   - hasNext() checks if there are more messages
 *   - next() returns the next ImmutableMessage from the stream
 *   - close() sends CloseConsumerRequest and terminates the stream
 */
@Slf4j
public class ConsumeStream implements Iterator<ImmutableMessage>, AutoCloseable {

    private final StreamingNodeHandlerServiceGrpc.StreamingNodeHandlerServiceStub asyncStub;
    private final String pchannelName;

    // Request observer for sending control requests to server
    private StreamObserver<ConsumeRequest> requestObserver;

    // Response queue for receiving messages from server
    private final LinkedBlockingQueue<ImmutableMessage> messageQueue;

    // Flag indicating if stream is still open
    private volatile boolean isOpen = true;

    // Counter of messages dropped due to queue overflow (for monitoring)
    private volatile long droppedMessages = 0;

    // Error from server (if any)
    private volatile Throwable streamError = null;

    // gRPC status code from onError (if the error is a StatusRuntimeException)
    private volatile io.grpc.Status.Code grpcStatusCode = null;

    // Error from CreateVChannelConsumerResponse (if any)
    private volatile StreamingCode createVchannelErrorCode = null;
    private volatile String createVchannelErrorCause = null;

    // Term extracted from stream-level FAILED_PRECONDITION error (gRPC trailers/description)
    private volatile long streamLevelCurrentTerm = -1;

    // Pattern to extract the server's current term from UNMATCHED_CHANNEL_TERM error cause.
    // Milvus error format: "channel <name> at term <clientTerm> is expected, but current term is <serverTerm>"
    // The client must retry with <serverTerm> (the number after "current term is").
    private static final Pattern CURRENT_TERM_PATTERN =
            Pattern.compile("current term is (\\d+)", Pattern.CASE_INSENSITIVE);

    // Pattern to extract term from gRPC status description or trailers
    private static final Pattern TERM_PATTERN_DESC =
            Pattern.compile("term[=: ]*(\\d+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern TERM_PATTERN_CHANNEL =
            Pattern.compile("channel.*?at term (\\d+).*?expected.*?current term is (\\d+)",
                    Pattern.CASE_INSENSITIVE);

    /**
     * Constructor: creates a bidirectional stream and sends initial CreateVChannelConsumerRequest.
     *
     * @param asyncStub      gRPC async stub for StreamingNodeHandlerService
     * @param createRequest  Initial ConsumeRequest (must be CreateVChannelConsumerRequest)
     * @param pchannelName   PChannel name for logging
     */
    public ConsumeStream(
            StreamingNodeHandlerServiceGrpc.StreamingNodeHandlerServiceStub asyncStub,
            ConsumeRequest createRequest,
            String pchannelName) {
        this.asyncStub = asyncStub;
        this.pchannelName = pchannelName;
        // Bounded queue with put()-based backpressure.
        // The gRPC server pushes WAL messages continuously via streaming —
        // it does NOT wait for pollChanges() to consume. When the consumer
        // (pollChanges → parse → JDBC sink) is slower than the producer
        // (gRPC WAL stream), the queue fills up. put() blocks the gRPC
        // callback thread, which triggers gRPC flow control to signal the
        // Milvus server to pause — the correct end-to-end backpressure chain.
        // Capacity must be large enough to absorb bursts between polls
        // but small enough to keep memory bounded.
        this.messageQueue = new LinkedBlockingQueue<>(2000);

        // Create response observer for handling server responses
        StreamObserver<ConsumeResponse> responseObserver = new StreamObserver<ConsumeResponse>() {
            @Override
            public void onNext(ConsumeResponse response) {
                if (response.hasConsume()) {
                    // Received a WAL message via ConsumeMessageReponse
                    ImmutableMessage message = response.getConsume().getMessage();
                    // Backpressure via put(): when the consumer (JDBC sink) is slower
                    // than the producer (gRPC WAL stream), the bounded queue fills up
                    // and put() blocks this gRPC callback thread. This triggers gRPC
                    // flow control, which signals the Milvus server to slow down —
                    // the correct end-to-end backpressure chain. No messages are dropped.
                    try {
                        int queueSize = messageQueue.size();
                        // Early warning at 60% — consumer is falling behind
                        if (queueSize >= 1200 && queueSize < 1800) {
                            log.warn("Message queue filling up for pchannel={}: "
                                    + "size={}/2000 (60%) — consumer may be lagging",
                                    pchannelName, queueSize);
                        }
                        // Critical at 90% — backpressure about to engage
                        if (queueSize >= 1800) {
                            log.warn("Message queue nearly full for pchannel={}: "
                                    + "size={}/2000 (90%) — backpressure engaging, "
                                    + "gRPC stream will slow down",
                                    pchannelName, queueSize);
                        }
                        messageQueue.put(message);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        droppedMessages++;
                        log.warn("Interrupted while enqueuing message for pchannel={}, "
                                + "dropping message (total dropped: {})",
                                pchannelName, droppedMessages);
                    }
                } else if (response.hasCreate()) {
                    // Consumer created successfully
                    log.info("Consumer created successfully for pchannel={}, serverId={}",
                            pchannelName, response.getCreate().getConsumerServerId());
                } else if (response.hasCreateVchannel()) {
                    CreateVChannelConsumerResponse vcResp = response.getCreateVchannel();
                    if (vcResp.hasError()) {
                        StreamingError err = vcResp.getError();
                        createVchannelErrorCode = err.getCode();
                        createVchannelErrorCause = err.getCause();
                        log.warn("CreateVChannelConsumer failed for pchannel={}: code={}, cause={}",
                                pchannelName, createVchannelErrorCode, createVchannelErrorCause);
                        // VChannel consumer creation failed — stream is not usable.
                        isOpen = false;
                    } else {
                        log.info("VChannel consumer created for pchannel={}, consumerId={}",
                                pchannelName, vcResp.getConsumerId());
                    }
                } else if (response.hasCreateVchannels()) {
                    // Handle CreateVChannelConsumersResponse (plural form, field 4)
                    java.util.List<CreateVChannelConsumerResponse> vcResps =
                            response.getCreateVchannels().getCreateVchannelsList();
                    for (int i = 0; i < vcResps.size(); i++) {
                        CreateVChannelConsumerResponse vcResp = vcResps.get(i);
                        if (vcResp.hasError()) {
                            StreamingError err = vcResp.getError();
                            createVchannelErrorCode = err.getCode();
                            createVchannelErrorCause = err.getCause();
                            log.warn("CreateVChannelConsumer[{}] failed for pchannel={}: code={}, cause={}",
                                    i, pchannelName, createVchannelErrorCode, createVchannelErrorCause);
                            isOpen = false;
                        } else {
                            log.info("VChannel consumer[{}] created for pchannel={}, consumerId={}",
                                    i, pchannelName, vcResp.getConsumerId());
                        }
                    }
                } else if (response.hasCloseVchannel()) {
                    log.info("VChannel consumer closed for pchannel={}, consumerId={}",
                            pchannelName, response.getCloseVchannel().getConsumerId());
                } else if (response.hasClose()) {
                    log.info("Consumer closed for pchannel={}", pchannelName);
                }
            }

            @Override
            public void onError(Throwable t) {
                if (t instanceof StatusRuntimeException) {
                    Status status = ((StatusRuntimeException) t).getStatus();
                    grpcStatusCode = status.getCode();
                    // Try to extract StreamingError from grpc-status-details-bin trailer
                    String streamingDetail = "";
                    try {
                        io.grpc.Metadata trailers = ((StatusRuntimeException) t).getTrailers();
                        if (trailers != null) {
                            byte[] statusDetailsBin = trailers.get(
                                    io.grpc.Metadata.Key.of("grpc-status-details-bin",
                                            io.grpc.Metadata.BINARY_BYTE_MARSHALLER));
                            if (statusDetailsBin != null) {
                                // Parse as google.rpc.Status to extract StreamingError detail
                                com.google.rpc.Status rpcStatus = com.google.rpc.Status.parseFrom(statusDetailsBin);
                                for (com.google.protobuf.Any detail : rpcStatus.getDetailsList()) {
                                    if (detail.is(StreamingError.class)) {
                                        StreamingError se = detail.unpack(StreamingError.class);
                                        streamingDetail = ", streamCode=" + se.getCode()
                                                + ", streamCause=" + se.getCause();
                                    }
                                }
                            }
                        }
                    } catch (Exception ignored) {
                        streamingDetail = ", (detail parsing failed)";
                    }
                    log.error("Consume stream error for pchannel={}: code={}, description={}, cause={}{}",
                            pchannelName, status.getCode(), status.getDescription(),
                            status.getCause() != null ? status.getCause().getMessage() : "none",
                            streamingDetail);

                    // Extract current term from FAILED_PRECONDITION so the caller
                    // can retry with the correct term instead of giving up.
                    if (status.getCode() == Status.Code.FAILED_PRECONDITION) {
                        long term = extractTermFromStatus(status, (StatusRuntimeException) t);
                        if (term > 0) {
                            streamLevelCurrentTerm = term;
                            log.info("Extracted current term={} from FAILED_PRECONDITION for pchannel={}",
                                    term, pchannelName);
                        }
                    }
                } else {
                    log.error("Consume stream error for pchannel={}: {}", pchannelName, t.getMessage());
                }
                streamError = t;
                isOpen = false;
            }

            /**
             * Extract the server's current term from a FAILED_PRECONDITION gRPC status.
             * Tries multiple sources: status description, grpc-status-details-bin trailer,
             * gRPC trailers metadata, and regex patterns known from Milvus StreamingNode.
             */
            private long extractTermFromStatus(Status status, StatusRuntimeException sre) {
                // 1. Try status description
                String desc = status.getDescription();
                if (desc != null && !desc.isEmpty()) {
                    Matcher m = CURRENT_TERM_PATTERN.matcher(desc);
                    if (m.find()) return Long.parseLong(m.group(1));
                    m = TERM_PATTERN_DESC.matcher(desc);
                    if (m.find()) return Long.parseLong(m.group(1));
                    m = TERM_PATTERN_CHANNEL.matcher(desc);
                    if (m.find()) return Long.parseLong(m.group(2));
                }

                // 2. Try grpc-status-details-bin trailer — Milvus embeds StreamingError
                //    with UNMATCHED_CHANNEL_TERM cause: "channel X at term Y is expected,
                //    but current term is Z"
                try {
                    io.grpc.Metadata trailers = sre.getTrailers();
                    if (trailers != null) {
                        byte[] statusDetailsBin = trailers.get(
                                io.grpc.Metadata.Key.of("grpc-status-details-bin",
                                        io.grpc.Metadata.BINARY_BYTE_MARSHALLER));
                        if (statusDetailsBin != null) {
                            com.google.rpc.Status rpcStatus = com.google.rpc.Status.parseFrom(statusDetailsBin);
                            for (com.google.protobuf.Any detail : rpcStatus.getDetailsList()) {
                                if (detail.is(StreamingError.class)) {
                                    StreamingError se = detail.unpack(StreamingError.class);
                                    if (se.getCode() == StreamingCode.STREAMING_CODE_UNMATCHED_CHANNEL_TERM
                                            && se.getCause() != null) {
                                        Matcher m = CURRENT_TERM_PATTERN.matcher(se.getCause());
                                        if (m.find()) return Long.parseLong(m.group(1));
                                    }
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    log.debug("Failed to extract term from grpc-status-details-bin: {}", e.getMessage());
                }

                // 3. Try gRPC trailers metadata
                try {
                    io.grpc.Metadata trailers = sre.getTrailers();
                    if (trailers != null) {
                        for (String key : trailers.keys()) {
                            if (key != null && key.toLowerCase().contains("term")) {
                                String val = trailers.get(
                                        io.grpc.Metadata.Key.of(key,
                                                io.grpc.Metadata.ASCII_STRING_MARSHALLER));
                                if (val != null) {
                                    try { return Long.parseLong(val.trim()); } catch (NumberFormatException ignored) {}
                                    Matcher m = CURRENT_TERM_PATTERN.matcher(val);
                                    if (m.find()) return Long.parseLong(m.group(1));
                                }
                            }
                        }
                        // Also check common Milvus trailer keys
                        String[] milvusKeys = {"channel-term", "current-term", "x-milvus-term", "term"};
                        for (String key : milvusKeys) {
                            String val = trailers.get(
                                    io.grpc.Metadata.Key.of(key,
                                            io.grpc.Metadata.ASCII_STRING_MARSHALLER));
                            if (val != null) {
                                try { return Long.parseLong(val.trim()); } catch (NumberFormatException ignored) {}
                            }
                        }
                    }
                } catch (Exception e) {
                    log.debug("Failed to extract term from gRPC trailers: {}", e.getMessage());
                }

                // 4. Try the cause exception's message
                Throwable cause = status.getCause();
                if (cause != null && cause.getMessage() != null) {
                    Matcher m = CURRENT_TERM_PATTERN.matcher(cause.getMessage());
                    if (m.find()) return Long.parseLong(m.group(1));
                }

                return -1;
            }

            @Override
            public void onCompleted() {
                log.info("Consume stream completed for pchannel={}", pchannelName);
                isOpen = false;
            }
        };

        // Create bidirectional stream
        this.requestObserver = asyncStub.consume(responseObserver);

        // Send initial CreateVChannelConsumerRequest
        try {
            requestObserver.onNext(createRequest);
            log.info("Sent CreateVChannelConsumerRequest for pchannel={}", pchannelName);
        } catch (Exception e) {
            log.error("Failed to send CreateVChannelConsumerRequest: {}", e.getMessage());
            streamError = e;
            isOpen = false;
        }
    }

    /**
     * Check if there are more messages in the queue.
     * This method blocks briefly to wait for messages if the queue is empty but stream is still open.
     *
     * @return true if there are more messages, false if stream is closed and queue is empty
     */
    @Override
    public boolean hasNext() {
        if (!messageQueue.isEmpty()) {
            return true;
        }

        if (!isOpen) {
            // Stream closed, check if there was an error
            if (streamError != null) {
                throw new RuntimeException("Stream error: " + streamError.getMessage(), streamError);
            }
            return false;  // Stream completed normally
        }

        // Stream still open, wait briefly for messages
        try {
            ImmutableMessage msg = messageQueue.poll(50, TimeUnit.MILLISECONDS);
            if (msg != null) {
                // Put it back for next() to retrieve
                messageQueue.put(msg);
                return true;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }

        // Still open but no message arrived in 100ms
        // Return false to avoid blocking next() — caller should poll again.
        return false;
    }

    /**
     * Get the next ImmutableMessage from the stream.
     * This method blocks until a message is available or stream closes.
     *
     * @return next ImmutableMessage
     * @throws NoSuchElementException if stream is closed and queue is empty
     */
    @Override
    public ImmutableMessage next() {
        if (!hasNext()) {
            throw new NoSuchElementException("Stream closed and queue empty for pchannel=" + pchannelName);
        }

        try {
            ImmutableMessage message = messageQueue.poll(2, TimeUnit.SECONDS);  // Block up to 2s
            if (message != null) {
                return message;
            } else {
                throw new NoSuchElementException("No message available within timeout for pchannel=" + pchannelName);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for message", e);
        }
    }

    /**
     * Close the stream by sending CloseConsumerRequest.
     */
    @Override
    public void close() {
        if (requestObserver != null && isOpen) {
            try {
                // Send close request
                ConsumeRequest closeRequest = ConsumeRequest.newBuilder()
                        .setClose(org.apache.seatunnel.connectors.streaming.proto.CloseConsumerRequest.newBuilder().build())
                        .build();
                requestObserver.onNext(closeRequest);

                // Close request stream
                requestObserver.onCompleted();

                log.info("Sent CloseConsumerRequest for pchannel={}", pchannelName);
            } catch (Exception e) {
                log.warn("Error closing ConsumeStream for pchannel={}: {}", pchannelName, e.getMessage());
            } finally {
                isOpen = false;
                requestObserver = null;
            }
        }
    }

    /**
     * Check if stream is still open (active).
     */
    public boolean isOpen() {
        return isOpen;
    }

    /**
     * Get stream error (if any).
     */
    public Throwable getStreamError() {
        return streamError;
    }

    /**
     * Get the gRPC status code from the stream error (if any).
     * Only set when the error is a {@link io.grpc.StatusRuntimeException}.
     *
     * @return gRPC status code, or null if no error or error is not gRPC-related
     */
    public io.grpc.Status.Code getGrpcStatusCode() {
        return grpcStatusCode;
    }

    /**
     * Get the error code from CreateVChannelConsumerResponse (if any).
     */
    public StreamingCode getCreateVchannelErrorCode() {
        return createVchannelErrorCode;
    }

    /**
     * Get the error cause string from CreateVChannelConsumerResponse (if any).
     */
    public String getCreateVchannelErrorCause() {
        return createVchannelErrorCause;
    }

    /**
     * Get the count of messages dropped due to queue overflow or interruption.
     * Non-zero values indicate backpressure was insufficient or the consumer
     * thread was interrupted during enqueue.
     */
    public long getDroppedMessages() {
        return droppedMessages;
    }

    /**
     * Get the current queue size (for monitoring/backpressure visibility).
     */
    public int getQueueSize() {
        return messageQueue.size();
    }

    /**
     * If the vchannel consumer creation failed with UNMATCHED_CHANNEL_TERM,
     * extract the server's current term from the error cause string.
     *
     * <p>Milvus error format:
     * <pre>"channel &lt;name&gt; at term &lt;clientTerm&gt; is expected, but current term is &lt;serverTerm&gt;"</pre>
     * The client must retry with {@code <serverTerm>}.
     *
     * @return the server's current term, or -1 if it cannot be determined
     */
    public long getCurrentTermFromError() {
        // 1. Stream-level FAILED_PRECONDITION term (highest priority)
        if (streamLevelCurrentTerm > 0) {
            return streamLevelCurrentTerm;
        }

        // 2. CreateVChannelConsumerResponse UNMATCHED_CHANNEL_TERM
        if (createVchannelErrorCode == StreamingCode.STREAMING_CODE_UNMATCHED_CHANNEL_TERM) {
            if (createVchannelErrorCause != null && !createVchannelErrorCause.isEmpty()) {
                Matcher m = CURRENT_TERM_PATTERN.matcher(createVchannelErrorCause);
                if (m.find()) {
                    try {
                        return Long.parseLong(m.group(1));
                    } catch (NumberFormatException e) {
                        log.warn("Failed to parse current term from cause: {}", createVchannelErrorCause);
                    }
                }
            }
        }

        // 3. Also check FAILED_PRECONDITION from createVchannel response (stream may
        //    close before delivering the createVchannel response, but the error code
        //    may still be set on the stream-level)
        if (grpcStatusCode == io.grpc.Status.Code.FAILED_PRECONDITION) {
            if (createVchannelErrorCause != null && !createVchannelErrorCause.isEmpty()) {
                Matcher m = CURRENT_TERM_PATTERN.matcher(createVchannelErrorCause);
                if (m.find()) {
                    try {
                        return Long.parseLong(m.group(1));
                    } catch (NumberFormatException e) {
                        // ignore
                    }
                }
            }
        }

        return -1;
    }
}