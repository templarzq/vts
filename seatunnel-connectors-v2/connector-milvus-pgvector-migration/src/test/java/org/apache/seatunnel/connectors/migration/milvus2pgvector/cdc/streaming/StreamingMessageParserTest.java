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

import com.google.protobuf.ByteString;
import io.milvus.grpc.FieldData;
import io.milvus.grpc.LongArray;
import io.milvus.grpc.ScalarField;
import io.milvus.grpc.IDs;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import milvus.proto.msg.Msg;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link StreamingMessageParser}.
 * Verifies message type detection and row conversion for Insert/Delete messages.
 *
 * <p>Milvus WAL message types (from {@code milvus.proto.msg.MessageType}):
 * <ul>
 *   <li>TimeTick = 1</li>
 *   <li>Insert = 2</li>
 *   <li>Delete = 3</li>
 *   <li>Flush = 4</li>
 * </ul>
 *
 * <p>There is NO separate Upsert message type. Upserts are decomposed into
 * Delete + Insert pairs at the proxy level.
 */
@DisplayName("StreamingMessageParser - WAL Message Parsing Tests")
class StreamingMessageParserTest {

    private StreamingMessageParser parser;

    /** Message type integer values (matching Milvus WAL properties). */
    private static final String MSG_TYPE_INSERT = "2";
    private static final String MSG_TYPE_DELETE = "3";
    private static final String MSG_TYPE_TIMETICK = "1";
    private static final String MSG_TYPE_FLUSH = "4";

    @BeforeEach
    void setUp() {
        // Build a minimal collection schema with one Int64 field "id"
        CreateCollectionReq.FieldSchema idField = CreateCollectionReq.FieldSchema.builder()
                .name("id")
                .dataType(io.milvus.v2.common.DataType.Int64)
                .isPrimaryKey(true)
                .build();

        List<CreateCollectionReq.FieldSchema> fields = new ArrayList<>();
        fields.add(idField);

        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .fieldSchemaList(fields)
                .build();

        DescribeCollectionResp collectionDesc = DescribeCollectionResp.builder()
                .collectionSchema(schema)
                .build();

        parser = new StreamingMessageParser(collectionDesc, "id");
    }

    /**
     * Helper: create an ImmutableMessage with the given message type (as integer string).
     * Uses the correct WAL property keys: "_t" for message type, "_tt" for timetick.
     *
     * @param messageType Integer string for message type (e.g., "2" for Insert)
     * @param messageId   Message ID string
     * @param payload      Protobuf payload bytes
     */
    private ImmutableMessage createMessage(String messageType, String messageId, ByteString payload) {
        Map<String, String> properties = new HashMap<>();
        properties.put("_t", messageType);
        properties.put("_tt", "rs");  // base36 encoding of 1000

        MessageID id = MessageID.newBuilder()
                .setId(ByteString.copyFromUtf8(messageId))
                .build();

        return ImmutableMessage.newBuilder()
                .setId(id)
                .setPayload(payload)
                .putAllProperties(properties)
                .build();
    }

    /** Helper: create a message with a dummy payload (for type-only tests). */
    private ImmutableMessage createMessage(String messageType, String messageId) {
        return createMessage(messageType, messageId, ByteString.copyFromUtf8("dummy"));
    }

    /** Helper: build a valid InsertRequest payload with one Int64 field. */
    private ByteString buildInsertPayload(long[] ids) {
        LongArray longArray = LongArray.newBuilder()
                .addAllData(java.util.Arrays.asList(java.util.Arrays.stream(ids).boxed().toArray(Long[]::new)))
                .build();

        FieldData fieldData = FieldData.newBuilder()
                .setFieldName("id")
                .setType(io.milvus.grpc.DataType.Int64)
                .setScalars(ScalarField.newBuilder().setLongData(longArray).build())
                .build();

        Msg.InsertRequest insertRequest = Msg.InsertRequest.newBuilder()
                .setCollectionName("test_collection")
                .setNumRows(ids.length)
                .addFieldsData(fieldData)
                .build();

        // Use toByteArray() + copyFrom to bridge Milvus shaded protobuf (milvus.com.google.protobuf)
        // and the standard protobuf (com.google.protobuf) used by ImmutableMessage.
        return ByteString.copyFrom(insertRequest.toByteArray());
    }

    /** Helper: build a valid DeleteRequest payload with Int64 primary keys. */
    private ByteString buildDeletePayload(long[] pks) {
        LongArray longArray = LongArray.newBuilder()
                .addAllData(java.util.Arrays.asList(java.util.Arrays.stream(pks).boxed().toArray(Long[]::new)))
                .build();

        IDs ids = IDs.newBuilder()
                .setIntId(longArray)
                .build();

        Msg.DeleteRequest deleteRequest = Msg.DeleteRequest.newBuilder()
                .setCollectionName("test_collection")
                .setPrimaryKeys(ids)
                .build();

        // Use toByteArray() + copyFrom to bridge Milvus shaded protobuf and standard protobuf.
        return ByteString.copyFrom(deleteRequest.toByteArray());
    }

    // ===== Message Type Detection =====

    @Test
    @DisplayName("Parse Insert message type")
    void testParseInsertMessageType() {
        ImmutableMessage message = createMessage(MSG_TYPE_INSERT, "msg-001");
        String type = parser.parseMessageType(message);
        assertEquals("Insert", type);
    }

    @Test
    @DisplayName("Parse Delete message type")
    void testParseDeleteMessageType() {
        ImmutableMessage message = createMessage(MSG_TYPE_DELETE, "msg-002");
        String type = parser.parseMessageType(message);
        assertEquals("Delete", type);
    }

    @Test
    @DisplayName("Parse TimeTick message type")
    void testParseTimeTickMessageType() {
        ImmutableMessage message = createMessage(MSG_TYPE_TIMETICK, "msg-003");
        String type = parser.parseMessageType(message);
        assertEquals("TimeTick", type);
    }

    @Test
    @DisplayName("Parse Flush message type")
    void testParseFlushMessageType() {
        ImmutableMessage message = createMessage(MSG_TYPE_FLUSH, "msg-004");
        String type = parser.parseMessageType(message);
        assertEquals("Flush", type);
    }

    // ===== Row Conversion =====

    @Test
    @DisplayName("Parse Insert message produces INSERT rows")
    void testParseInsertMessageProducesInsertRows() {
        ByteString payload = buildInsertPayload(new long[]{42L});
        ImmutableMessage message = createMessage(MSG_TYPE_INSERT, "msg-001", payload);
        List<SeaTunnelRow> rows = parser.parseMessage(message);

        assertNotNull(rows);
        assertEquals(1, rows.size());
        assertEquals(RowKind.INSERT, rows.get(0).getRowKind());
        assertEquals(42L, rows.get(0).getField(0));
    }

    @Test
    @DisplayName("Parse Delete message produces DELETE rows")
    void testParseDeleteMessageProducesDeleteRows() {
        ByteString payload = buildDeletePayload(new long[]{99L});
        ImmutableMessage message = createMessage(MSG_TYPE_DELETE, "msg-002", payload);
        List<SeaTunnelRow> rows = parser.parseMessage(message);

        assertNotNull(rows);
        assertEquals(1, rows.size());
        assertEquals(RowKind.DELETE, rows.get(0).getRowKind());
        assertEquals(99L, rows.get(0).getField(0));
    }

    @Test
    @DisplayName("Parse unknown message type returns empty list")
    void testParseUnknownMessageTypeReturnsEmpty() {
        ImmutableMessage message = createMessage("999", "msg-004");
        List<SeaTunnelRow> rows = parser.parseMessage(message);
        assertNotNull(rows);
        assertTrue(rows.isEmpty());
    }

    // ===== MessageID and Timetick =====

    @Test
    @DisplayName("Extract MessageID from message")
    void testExtractMessageID() {
        ImmutableMessage message = createMessage(MSG_TYPE_INSERT, "msg-005");
        MessageID messageId = parser.extractMessageID(message);
        assertNotNull(messageId);
        assertEquals("msg-005", messageId.getId().toStringUtf8());
    }

    @Test
    @DisplayName("Extract timetick from message (base36 decoded)")
    void testExtractTimetick() {
        ImmutableMessage message = createMessage(MSG_TYPE_INSERT, "msg-006");
        long timetick = parser.extractTimetick(message);
        assertEquals(1000L, timetick);
    }

    @Test
    @DisplayName("Extract timetick with invalid format returns 0")
    void testExtractTimetickInvalid() {
        Map<String, String> properties = new HashMap<>();
        properties.put("_t", MSG_TYPE_INSERT);
        // "12.5" is genuinely unparseable in base36 ('.' is not a valid base36 digit).
        // Note: a plain alphabetic string like "invalid" IS valid base36, so it would not
        // trigger the fallback path.
        properties.put("_tt", "12.5");

        ImmutableMessage message = ImmutableMessage.newBuilder()
                .setId(MessageID.newBuilder().setId(ByteString.copyFromUtf8("msg-007")).build())
                .setPayload(ByteString.copyFromUtf8("dummy"))
                .putAllProperties(properties)
                .build();

        long timetick = parser.extractTimetick(message);
        assertEquals(0L, timetick);
    }

    // ===== isDataMessage =====

    @Test
    @DisplayName("isDataMessage returns true for Insert/Delete")
    void testIsDataMessageTrue() {
        assertTrue(parser.isDataMessage(createMessage(MSG_TYPE_INSERT, "msg-1")));
        assertTrue(parser.isDataMessage(createMessage(MSG_TYPE_DELETE, "msg-2")));
    }

    @Test
    @DisplayName("isDataMessage returns false for non-DML types")
    void testIsDataMessageFalse() {
        assertFalse(parser.isDataMessage(createMessage(MSG_TYPE_TIMETICK, "msg-3")));
        assertFalse(parser.isDataMessage(createMessage(MSG_TYPE_FLUSH, "msg-4")));
        assertFalse(parser.isDataMessage(createMessage("999", "msg-5")));
    }
}
