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

package org.apache.seatunnel.connectors.migration.milvus2pgvector.cdc;

import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.cdc.milvus.proto.ImmutableMessage;
import org.apache.seatunnel.connectors.cdc.milvus.proto.MessageID;
import org.apache.seatunnel.connectors.cdc.milvus.proto.ReplicateCheckpoint;

import io.milvus.grpc.DeleteRequest;
import io.milvus.grpc.FieldData;
import io.milvus.grpc.IDs;
import io.milvus.grpc.InsertRequest;
import io.milvus.grpc.LongArray;
import io.milvus.grpc.ScalarField;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;

import com.google.protobuf.ByteString;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link CdcEventStreamStrategy}.
 *
 * <p>These tests mock {@link MilvusCdcGrpcClient} and do NOT depend on a real Milvus server. They
 * validate availability detection, WAL event parsing (insert/delete), position tracking, batch size
 * limits, and stream-error re-bootstrap behavior.
 */
@ExtendWith(MockitoExtension.class)
class CdcEventStreamStrategyTest {

    @Mock
    private MilvusCdcGrpcClient mockClient;

    private CdcEventStreamStrategy strategy;

    @AfterEach
    void tearDown() {
        if (strategy != null) {
            strategy.close();
        }
    }

    // ---------- helpers ----------

    private MilvusCdcSourceConfig baseConfig() {
        return MilvusCdcSourceConfig.builder()
                .url("http://localhost:19530")
                .token("")
                .collection("test_collection")
                .cdcStrategy("event_stream")
                .cdcPchannel("test-pchannel")
                .incrementalBatchSize(10L)
                .pollIntervalMs(50L)
                .channelTimeoutMs(5000L)
                .primaryKeyField("id")
                .build();
    }

    private MilvusCdcSourceConfig configWithoutPchannel() {
        return MilvusCdcSourceConfig.builder()
                .url("http://localhost:19530")
                .token("")
                .collection("test_collection")
                .cdcStrategy("event_stream")
                .cdcPchannel(null)
                .incrementalBatchSize(10L)
                .pollIntervalMs(50L)
                .channelTimeoutMs(5000L)
                .primaryKeyField("id")
                .build();
    }

    private DescribeCollectionResp collectionDesc() {
        return DescribeCollectionResp.builder()
                .collectionName("test_collection")
                .collectionSchema(CreateCollectionReq.CollectionSchema.builder()
                        .fieldSchemaList(Arrays.asList(
                                CreateCollectionReq.FieldSchema.builder()
                                        .name("id")
                                        .dataType(DataType.Int64)
                                        .isPrimaryKey(true)
                                        .autoID(false)
                                        .build(),
                                CreateCollectionReq.FieldSchema.builder()
                                        .name("vector")
                                        .dataType(DataType.FloatVector)
                                        .dimension(8)
                                        .build()))
                        .build())
                .collectionID(1L)
                .build();
    }

    private ReplicateCheckpoint checkpoint() {
        return ReplicateCheckpoint.newBuilder()
                .setClusterId("cluster-1")
                .setPchannel("test-pchannel")
                .setMessageId(ByteString.copyFromUtf8("msg-0"))
                .setTimeTick(100L)
                .build();
    }

    private ImmutableMessage buildInsertMessage(String msgId, String timetick, long pk) {
        InsertRequest insertReq = InsertRequest.newBuilder()
                .setCollectionName("test_collection")
                .setNumRows(1)
                .addFieldsData(FieldData.newBuilder()
                        .setFieldName("id")
                        .setType(io.milvus.grpc.DataType.Int64)
                        .setScalars(ScalarField.newBuilder()
                                .setLongData(LongArray.newBuilder().addData(pk).build())
                                .build())
                        .build())
                .build();

        return ImmutableMessage.newBuilder()
                .setId(MessageID.newBuilder().setId(msgId).build())
                .setPayload(ByteString.copyFrom(insertReq.toByteArray()))
                .putProperties("messages.type", "Insert")
                .putProperties("messages.collection", "1")
                .putProperties("messages.timetick", timetick)
                .build();
    }

    private ImmutableMessage buildDeleteMessage(String msgId, String timetick, long pk) {
        IDs ids = IDs.newBuilder()
                .setIntId(LongArray.newBuilder().addData(pk).build())
                .build();
        DeleteRequest baseDelete = DeleteRequest.newBuilder()
                .setCollectionName("test_collection")
                .setExpr("id in [" + pk + "]")
                .build();
        byte[] deleteBytes = injectField(baseDelete.toByteArray(), 12, ids.toByteArray());

        return ImmutableMessage.newBuilder()
                .setId(MessageID.newBuilder().setId(msgId).build())
                .setPayload(ByteString.copyFrom(deleteBytes))
                .putProperties("messages.type", "Delete")
                .putProperties("messages.collection", "1")
                .putProperties("messages.timetick", timetick)
                .build();
    }

    private static byte[] injectField(byte[] base, int fieldNumber, byte[] value) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(base, 0, base.length);
        int tag = (fieldNumber << 3) | 2; // wire type 2 = length-delimited
        writeVarint(bos, tag);
        writeVarint(bos, value.length);
        bos.write(value, 0, value.length);
        return bos.toByteArray();
    }

    private static void writeVarint(ByteArrayOutputStream bos, int value) {
        while (value > 0x7F) {
            bos.write((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        bos.write(value & 0x7F);
    }

    // ---------- 1. isAvailable: pchannel not configured ----------

    @Test
    void testIsAvailable_PchannelNotConfigured() {
        strategy = new CdcEventStreamStrategy(configWithoutPchannel(), collectionDesc(), mockClient);
        assertFalse(strategy.isAvailable());
    }

    // ---------- 2. isAvailable: GetReplicateInfo empty ----------

    @Test
    void testIsAvailable_GetReplicateInfoEmpty() {
        when(mockClient.getReplicateInfo(any(), any())).thenReturn(Optional.empty());
        strategy = new CdcEventStreamStrategy(baseConfig(), collectionDesc(), mockClient);
        assertFalse(strategy.isAvailable());
    }

    // ---------- 3. isAvailable: success ----------

    @Test
    void testIsAvailable_Success() {
        when(mockClient.getReplicateInfo(any(), any())).thenReturn(Optional.of(checkpoint()));
        strategy = new CdcEventStreamStrategy(baseConfig(), collectionDesc(), mockClient);
        assertTrue(strategy.isAvailable());
    }

    // ---------- 4. pollChanges: empty stream ----------

    @Test
    void testPollChanges_EmptyStream() throws Exception {
        when(mockClient.getReplicateInfo(any(), any())).thenReturn(Optional.of(checkpoint()));
        when(mockClient.dumpMessages(anyString(), any(MessageID.class), anyLong(), anyLong()))
                .thenReturn(Collections.emptyIterator());
        strategy = new CdcEventStreamStrategy(baseConfig(), collectionDesc(), mockClient);

        List<SeaTunnelRowWithPosition> results = strategy.pollChanges(null, null);
        assertTrue(results.isEmpty());
    }

    // ---------- 5. pollChanges: insert events ----------

    @Test
    void testPollChanges_InsertEvents() throws Exception {
        when(mockClient.getReplicateInfo(any(), any())).thenReturn(Optional.of(checkpoint()));
        when(mockClient.dumpMessages(anyString(), any(MessageID.class), anyLong(), anyLong()))
                .thenReturn(Collections.singletonList(
                        buildInsertMessage("msg-1", "200", 42L)).iterator());
        strategy = new CdcEventStreamStrategy(baseConfig(), collectionDesc(), mockClient);

        List<SeaTunnelRowWithPosition> results = strategy.pollChanges(null, null);
        assertEquals(1, results.size());

        SeaTunnelRow row = results.get(0).getRow();
        assertEquals(RowKind.INSERT, row.getRowKind());
        assertEquals(42L, row.getField(0));

        ReplicatePosition pos = results.get(0).getPosition();
        assertEquals("msg-1", pos.getMessageId());
        assertEquals(200L, pos.getTimeTick());
    }

    // ---------- 6. pollChanges: delete events ----------

    @Test
    void testPollChanges_DeleteEvents() throws Exception {
        when(mockClient.getReplicateInfo(any(), any())).thenReturn(Optional.of(checkpoint()));
        when(mockClient.dumpMessages(anyString(), any(MessageID.class), anyLong(), anyLong()))
                .thenReturn(Collections.singletonList(
                        buildDeleteMessage("msg-2", "300", 99L)).iterator());
        strategy = new CdcEventStreamStrategy(baseConfig(), collectionDesc(), mockClient);

        List<SeaTunnelRowWithPosition> results = strategy.pollChanges(null, null);
        assertEquals(1, results.size());

        SeaTunnelRow row = results.get(0).getRow();
        assertEquals(RowKind.DELETE, row.getRowKind());
        assertEquals(99L, row.getField(0));
    }

    // ---------- 7. pollChanges: max events per poll ----------

    @Test
    void testPollChanges_MaxEventsPerPoll() throws Exception {
        MilvusCdcSourceConfig config = MilvusCdcSourceConfig.builder()
                .url("http://localhost:19530")
                .token("")
                .collection("test_collection")
                .cdcStrategy("event_stream")
                .cdcPchannel("test-pchannel")
                .incrementalBatchSize(2L)
                .pollIntervalMs(50L)
                .channelTimeoutMs(5000L)
                .primaryKeyField("id")
                .build();

        when(mockClient.getReplicateInfo(any(), any())).thenReturn(Optional.of(checkpoint()));
        when(mockClient.dumpMessages(anyString(), any(MessageID.class), anyLong(), anyLong()))
                .thenReturn(Arrays.asList(
                        buildInsertMessage("msg-1", "200", 1L),
                        buildInsertMessage("msg-2", "201", 2L),
                        buildInsertMessage("msg-3", "202", 3L)).iterator());
        strategy = new CdcEventStreamStrategy(config, collectionDesc(), mockClient);

        List<SeaTunnelRowWithPosition> results = strategy.pollChanges(null, null);
        assertEquals(2, results.size());
    }

    // ---------- 8. pollChanges: position tracking ----------

    @Test
    void testPollChanges_PositionTracking() throws Exception {
        when(mockClient.getReplicateInfo(any(), any())).thenReturn(Optional.of(checkpoint()));
        when(mockClient.dumpMessages(anyString(), any(MessageID.class), anyLong(), anyLong()))
                .thenReturn(Arrays.asList(
                        buildInsertMessage("msg-1", "200", 1L),
                        buildInsertMessage("msg-2", "201", 2L)).iterator());
        strategy = new CdcEventStreamStrategy(baseConfig(), collectionDesc(), mockClient);

        List<SeaTunnelRowWithPosition> results = strategy.pollChanges(null, null);
        assertEquals(2, results.size());
        assertEquals("msg-2", results.get(results.size() - 1).getPosition().getMessageId());
    }

    // ---------- 9. pollChanges: stream error then re-bootstrap ----------

    @Test
    void testPollChanges_StreamError_ThenRebootstrap() throws Exception {
        Iterator<ImmutableMessage> failingIterator = new Iterator<ImmutableMessage>() {
            @Override
            public boolean hasNext() {
                throw new RuntimeException("stream error");
            }

            @Override
            public ImmutableMessage next() {
                throw new NoSuchElementException();
            }
        };

        when(mockClient.getReplicateInfo(any(), any())).thenReturn(Optional.of(checkpoint()));
        when(mockClient.dumpMessages(anyString(), any(MessageID.class), anyLong(), anyLong()))
                .thenReturn(failingIterator,
                        Collections.singletonList(buildInsertMessage("msg-1", "200", 42L)).iterator());
        strategy = new CdcEventStreamStrategy(baseConfig(), collectionDesc(), mockClient);

        // First poll: stream error surfaces as empty result.
        List<SeaTunnelRowWithPosition> firstResults = strategy.pollChanges(null, null);
        assertTrue(firstResults.isEmpty());

        // Second poll: stream re-bootstraps and emits the insert.
        List<SeaTunnelRowWithPosition> secondResults = strategy.pollChanges(null, null);
        assertEquals(1, secondResults.size());
        assertEquals(RowKind.INSERT, secondResults.get(0).getRow().getRowKind());
        assertEquals(42L, secondResults.get(0).getRow().getField(0));
    }
}
