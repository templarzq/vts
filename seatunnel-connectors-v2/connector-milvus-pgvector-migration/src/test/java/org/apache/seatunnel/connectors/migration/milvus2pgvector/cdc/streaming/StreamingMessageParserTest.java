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

import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.streaming.proto.ImmutableMessage;
import org.apache.seatunnel.connectors.streaming.proto.MessageID;

import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link StreamingMessageParser}.
 * Verifies message type detection and row conversion for Insert/Delete/Upsert messages.
 */
@DisplayName("StreamingMessageParser - WAL Message Parsing Tests")
class StreamingMessageParserTest {

    private StreamingMessageParser parser;

    @BeforeEach
    void setUp() {
        // Use a minimal mock collection description
        DescribeCollectionResp collectionDesc = DescribeCollectionResp.builder().build();
        parser = new StreamingMessageParser(collectionDesc, "id");
    }

    /**
     * Helper: create an ImmutableMessage with given type and properties.
     */
    private ImmutableMessage createMessage(String messageType, String messageId) {
        Map<String, String> properties = new HashMap<>();
        properties.put("messages.type", messageType);
        properties.put("messages.collection", "123");
        properties.put("messages.vchannel", "test-channel");
        properties.put("messages.timetick", "1000");

        MessageID id = MessageID.newBuilder()
                .setId(com.google.protobuf.ByteString.copyFromUtf8(messageId))
                .build();

        return ImmutableMessage.newBuilder()
                .setId(id)
                .setPayload(com.google.protobuf.ByteString.copyFromUtf8("test-payload"))
                .putAllProperties(properties)
                .build();
    }

    @Test
    @DisplayName("Parse Insert message type")
    void testParseInsertMessageType() {
        ImmutableMessage message = createMessage("Insert", "msg-001");
        String type = parser.parseMessageType(message);
        assertEquals("Insert", type);
    }

    @Test
    @DisplayName("Parse Delete message type")
    void testParseDeleteMessageType() {
        ImmutableMessage message = createMessage("Delete", "msg-002");
        String type = parser.parseMessageType(message);
        assertEquals("Delete", type);
    }

    @Test
    @DisplayName("Parse Upsert message type")
    void testParseUpsertMessageType() {
        ImmutableMessage message = createMessage("Upsert", "msg-003");
        String type = parser.parseMessageType(message);
        assertEquals("Upsert", type);
    }

    @Test
    @DisplayName("Parse Insert message produces INSERT rows")
    void testParseInsertMessageProducesInsertRows() {
        ImmutableMessage message = createMessage("Insert", "msg-001");
        List<SeaTunnelRow> rows = parser.parseMessage(message);

        assertNotNull(rows);
        assertEquals(1, rows.size());
        assertEquals(RowKind.INSERT, rows.get(0).getRowKind());
    }

    @Test
    @DisplayName("Parse Delete message produces DELETE rows")
    void testParseDeleteMessageProducesDeleteRows() {
        ImmutableMessage message = createMessage("Delete", "msg-002");
        List<SeaTunnelRow> rows = parser.parseMessage(message);

        assertNotNull(rows);
        assertEquals(1, rows.size());
        assertEquals(RowKind.DELETE, rows.get(0).getRowKind());
    }

    @Test
    @DisplayName("Parse Upsert message produces INSERT rows (upsert as insert)")
    void testParseUpsertMessageProducesInsertRows() {
        ImmutableMessage message = createMessage("Upsert", "msg-003");
        List<SeaTunnelRow> rows = parser.parseMessage(message);

        assertNotNull(rows);
        assertEquals(1, rows.size());
        assertEquals(RowKind.INSERT, rows.get(0).getRowKind());
    }

    @Test
    @DisplayName("Parse unknown message type returns empty list")
    void testParseUnknownMessageTypeReturnsEmpty() {
        ImmutableMessage message = createMessage("Unknown", "msg-004");
        List<SeaTunnelRow> rows = parser.parseMessage(message);
        assertNotNull(rows);
        assertTrue(rows.isEmpty());
    }

    @Test
    @DisplayName("Extract MessageID from message")
    void testExtractMessageID() {
        ImmutableMessage message = createMessage("Insert", "msg-005");
        MessageID messageId = parser.extractMessageID(message);
        assertNotNull(messageId);
        assertEquals("msg-005", messageId.getId().toStringUtf8());
    }

    @Test
    @DisplayName("Extract timetick from message")
    void testExtractTimetick() {
        ImmutableMessage message = createMessage("Insert", "msg-006");
        long timetick = parser.extractTimetick(message);
        assertEquals(1000L, timetick);
    }

    @Test
    @DisplayName("Extract timetick with invalid format returns 0")
    void testExtractTimetickInvalid() {
        Map<String, String> properties = new HashMap<>();
        properties.put("messages.type", "Insert");
        properties.put("messages.timetick", "invalid");

        ImmutableMessage message = ImmutableMessage.newBuilder()
                .setId(MessageID.newBuilder().setId(com.google.protobuf.ByteString.copyFromUtf8("msg-007")).build())
                .putAllProperties(properties)
                .build();

        long timetick = parser.extractTimetick(message);
        assertEquals(0L, timetick);
    }

    @Test
    @DisplayName("isDataMessage returns true for Insert/Delete/Upsert")
    void testIsDataMessageTrue() {
        assertTrue(parser.isDataMessage(createMessage("Insert", "msg-1")));
        assertTrue(parser.isDataMessage(createMessage("Delete", "msg-2")));
        assertTrue(parser.isDataMessage(createMessage("Upsert", "msg-3")));
    }

    @Test
    @DisplayName("isDataMessage returns false for unknown types")
    void testIsDataMessageFalse() {
        assertFalse(parser.isDataMessage(createMessage("Unknown", "msg-4")));
        assertFalse(parser.isDataMessage(createMessage("CreateCollection", "msg-5")));
    }
}