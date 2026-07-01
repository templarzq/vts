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

import org.apache.seatunnel.connectors.streaming.proto.DeliverPolicy;
import org.apache.seatunnel.connectors.streaming.proto.PChannelInfo;

import com.google.protobuf.Empty;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link DeliverPolicy} construction and PChannelInfo building.
 * Verifies that the proto messages used by CdcEventStreamStrategyV2 are correctly built.
 */
@DisplayName("StreamingNode Proto Building Tests")
class StreamingNodeProtoBuilderTest {

    @Test
    @DisplayName("Build DeliverPolicy.all for full snapshot")
    void testBuildDeliverPolicyAll() {
        DeliverPolicy policy = DeliverPolicy.newBuilder()
                .setAll(Empty.newBuilder().build())
                .build();

        assertEquals(DeliverPolicy.PolicyCase.ALL, policy.getPolicyCase());
        assertNotNull(policy.getAll());
    }

    @Test
    @DisplayName("Build DeliverPolicy.startAfter for checkpoint recovery")
    void testBuildDeliverPolicyStartAfter() {
        org.apache.seatunnel.connectors.streaming.proto.MessageID messageId =
                org.apache.seatunnel.connectors.streaming.proto.MessageID.newBuilder()
                        .setId(com.google.protobuf.ByteString.copyFromUtf8("checkpoint-msg-001"))
                        .build();

        DeliverPolicy policy = DeliverPolicy.newBuilder()
                .setStartAfter(messageId)
                .build();

        assertEquals(DeliverPolicy.PolicyCase.START_AFTER, policy.getPolicyCase());
        assertEquals("checkpoint-msg-001", policy.getStartAfter().getId().toStringUtf8());
    }

    @Test
    @DisplayName("Build PChannelInfo with name and term")
    void testBuildPChannelInfo() {
        PChannelInfo pchannel = PChannelInfo.newBuilder()
                .setName("by-dev-rootcoord-dml_0")
                .setTerm(1)
                .build();

        assertEquals("by-dev-rootcoord-dml_0", pchannel.getName());
        assertEquals(1, pchannel.getTerm());
    }

    @Test
    @DisplayName("Build PChannelInfo with access mode")
    void testBuildPChannelInfoWithAccessMode() {
        PChannelInfo pchannel = PChannelInfo.newBuilder()
                .setName("test-channel")
                .setTerm(1)
                .setAccessModeValue(0)  // READWRITE
                .build();

        assertEquals("test-channel", pchannel.getName());
        assertEquals(0, pchannel.getAccessModeValue());
    }

    @Test
    @DisplayName("Build ImmutableMessage with properties")
    void testBuildImmutableMessage() {
        java.util.Map<String, String> properties = new java.util.HashMap<>();
        properties.put("messages.type", "Delete");
        properties.put("messages.collection", "123");

        org.apache.seatunnel.connectors.streaming.proto.ImmutableMessage message =
                org.apache.seatunnel.connectors.streaming.proto.ImmutableMessage.newBuilder()
                        .setId(org.apache.seatunnel.connectors.streaming.proto.MessageID.newBuilder()
                                .setId(com.google.protobuf.ByteString.copyFromUtf8("msg-001"))
                                .build())
                        .setPayload(com.google.protobuf.ByteString.copyFromUtf8("payload"))
                        .putAllProperties(properties)
                        .build();

        assertEquals("msg-001", message.getId().getId().toStringUtf8());
        assertEquals("Delete", message.getPropertiesMap().get("messages.type"));
        assertEquals("123", message.getPropertiesMap().get("messages.collection"));
    }

    @Test
    @DisplayName("Build ConsumeRequest with CreateVChannelConsumer")
    void testBuildConsumeRequest() {
        DeliverPolicy policy = DeliverPolicy.newBuilder()
                .setAll(Empty.newBuilder().build())
                .build();

        org.apache.seatunnel.connectors.streaming.proto.ConsumeRequest request =
                org.apache.seatunnel.connectors.streaming.proto.ConsumeRequest.newBuilder()
                        .setCreateVchannelConsumer(
                                org.apache.seatunnel.connectors.streaming.proto.CreateVChannelConsumerRequest.newBuilder()
                                        .setVchannel("test-vchannel")
                                        .setDeliverPolicy(policy)
                                        .build())
                        .build();

        assertTrue(request.hasCreateVchannelConsumer());
        assertEquals("test-vchannel", request.getCreateVchannelConsumer().getVchannel());
        assertEquals(DeliverPolicy.PolicyCase.ALL,
                request.getCreateVchannelConsumer().getDeliverPolicy().getPolicyCase());
    }
}