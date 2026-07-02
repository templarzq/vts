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

    // Error from server (if any)
    private volatile Throwable streamError = null;

    // gRPC status code from onError (if the error is a StatusRuntimeException)
    private volatile io.grpc.Status.Code grpcStatusCode = null;

    // Error from CreateVChannelConsumerResponse (if any)
    private volatile StreamingCode createVchannelErrorCode = null;
    private volatile String createVchannelErrorCause = null;

    // Pattern to extract the server's current term from UNMATCHED_CHANNEL_TERM error cause.
    // Milvus error format: "channel <name> at term <clientTerm> is expected, but current term is <serverTerm>"
    // The client must retry with <serverTerm> (the number after "current term is").
    private static final Pattern CURRENT_TERM_PATTERN =
            Pattern.compile("current term is (\\d+)", Pattern.CASE_INSENSITIVE);

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
        this.messageQueue = new LinkedBlockingQueue<>(1000);  // Buffer size

        // Create response observer for handling server responses
        StreamObserver<ConsumeResponse> responseObserver = new StreamObserver<ConsumeResponse>() {
            @Override
            public void onNext(ConsumeResponse response) {
                if (response.hasConsume()) {
                    // Received a WAL message via ConsumeMessageReponse
                    ImmutableMessage message = response.getConsume().getMessage();
                    if (!messageQueue.offer(message)) {
                        log.warn("Message queue full for pchannel={}, dropping message", pchannelName);
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
                    log.error("Consume stream error for pchannel={}: code={}, description={}, cause={}",
                            pchannelName, status.getCode(), status.getDescription(),
                            status.getCause() != null ? status.getCause().getMessage() : "none");
                } else {
                    log.error("Consume stream error for pchannel={}: {}", pchannelName, t.getMessage());
                }
                streamError = t;
                isOpen = false;
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
        if (createVchannelErrorCode != StreamingCode.STREAMING_CODE_UNMATCHED_CHANNEL_TERM) {
            return -1;
        }
        if (createVchannelErrorCause == null || createVchannelErrorCause.isEmpty()) {
            return -1;
        }
        Matcher m = CURRENT_TERM_PATTERN.matcher(createVchannelErrorCause);
        if (m.find()) {
            try {
                return Long.parseLong(m.group(1));
            } catch (NumberFormatException e) {
                log.warn("Failed to parse current term from cause: {}", createVchannelErrorCause);
            }
        }
        log.warn("UNMATCHED_CHANNEL_TERM error cause does not contain 'current term is <N>': {}",
                createVchannelErrorCause);
        return -1;
    }
}