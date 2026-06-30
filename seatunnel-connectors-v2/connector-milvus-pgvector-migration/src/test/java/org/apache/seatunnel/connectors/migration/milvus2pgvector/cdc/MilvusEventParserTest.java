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

import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.cdc.milvus.proto.ImmutableMessage;
import org.apache.seatunnel.connectors.cdc.milvus.proto.MessageID;

import io.milvus.grpc.BoolArray;
import io.milvus.grpc.DeleteRequest;
import io.milvus.grpc.DoubleArray;
import io.milvus.grpc.FieldData;
import io.milvus.grpc.FloatArray;
import io.milvus.grpc.IDs;
import io.milvus.grpc.InsertRequest;
import io.milvus.grpc.IntArray;
import io.milvus.grpc.JSONArray;
import io.milvus.grpc.LongArray;
import io.milvus.grpc.ScalarField;
import io.milvus.grpc.StringArray;
import io.milvus.grpc.VectorField;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;

import com.google.protobuf.ByteString;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link MilvusEventParser}.
 *
 * <p>These tests construct protobuf DML messages (InsertRequest / DeleteRequest) directly and
 * wrap them in {@link ImmutableMessage} frames. They do NOT depend on a real Milvus server.
 *
 * <p>The SDK's {@code io.milvus.grpc.*} proto classes are shaded by connector-milvus's
 * maven-shade-plugin ({@code com.google} -> {@code milvus.com.google}). We bridge the two
 * protobuf universes via {@code byte[]} (toByteArray / parseFrom(byte[])). SDK calls that
 * require a shaded {@link milvus.com.google.protobuf.ByteString} use the fully qualified
 * class name to avoid confusing it with the non-shaded {@link com.google.protobuf.ByteString}
 * used by our own {@link ImmutableMessage} proto.
 */
public class MilvusEventParserTest {

    private static final String COLLECTION_NAME = "test_collection";

    // ---------- Schema helpers ----------

    /**
     * Build a {@link DescribeCollectionResp} with an Int64 primary key, a FloatVector field
     * (dim=8) and a VarChar field. This is the default schema used by most tests.
     */
    private DescribeCollectionResp buildTestSchema(long collectionId) {
        List<CreateCollectionReq.FieldSchema> fields = new ArrayList<>();
        fields.add(CreateCollectionReq.FieldSchema.builder()
                .name("id").dataType(DataType.Int64).isPrimaryKey(true).autoID(false).build());
        fields.add(CreateCollectionReq.FieldSchema.builder()
                .name("vector").dataType(DataType.FloatVector).dimension(8).build());
        fields.add(CreateCollectionReq.FieldSchema.builder()
                .name("category").dataType(DataType.VarChar).maxLength(64).build());
        return DescribeCollectionResp.builder()
                .collectionName(COLLECTION_NAME)
                .collectionSchema(CreateCollectionReq.CollectionSchema.builder()
                        .fieldSchemaList(fields).build())
                .collectionID(collectionId)
                .build();
    }

    /** Build a schema with a VarChar primary key and a FloatVector field (dim=8). */
    private DescribeCollectionResp buildVarCharPkSchema(long collectionId) {
        List<CreateCollectionReq.FieldSchema> fields = new ArrayList<>();
        fields.add(CreateCollectionReq.FieldSchema.builder()
                .name("id").dataType(DataType.VarChar).isPrimaryKey(true)
                .maxLength(64).autoID(false).build());
        fields.add(CreateCollectionReq.FieldSchema.builder()
                .name("vector").dataType(DataType.FloatVector).dimension(8).build());
        return DescribeCollectionResp.builder()
                .collectionName(COLLECTION_NAME)
                .collectionSchema(CreateCollectionReq.CollectionSchema.builder()
                        .fieldSchemaList(fields).build())
                .collectionID(collectionId)
                .build();
    }

    /** Build a schema with an Int64 primary key and a BinaryVector field (dim=128). */
    private DescribeCollectionResp buildBinaryVectorSchema(long collectionId) {
        List<CreateCollectionReq.FieldSchema> fields = new ArrayList<>();
        fields.add(CreateCollectionReq.FieldSchema.builder()
                .name("id").dataType(DataType.Int64).isPrimaryKey(true).autoID(false).build());
        fields.add(CreateCollectionReq.FieldSchema.builder()
                .name("binary_vec").dataType(DataType.BinaryVector).dimension(128).build());
        return DescribeCollectionResp.builder()
                .collectionName(COLLECTION_NAME)
                .collectionSchema(CreateCollectionReq.CollectionSchema.builder()
                        .fieldSchemaList(fields).build())
                .collectionID(collectionId)
                .build();
    }

    /** Build a schema with an Int64 PK and several scalar field types. */
    private DescribeCollectionResp buildMultiScalarSchema(long collectionId) {
        List<CreateCollectionReq.FieldSchema> fields = new ArrayList<>();
        fields.add(CreateCollectionReq.FieldSchema.builder()
                .name("id").dataType(DataType.Int64).isPrimaryKey(true).autoID(false).build());
        fields.add(CreateCollectionReq.FieldSchema.builder()
                .name("bool_field").dataType(DataType.Bool).build());
        fields.add(CreateCollectionReq.FieldSchema.builder()
                .name("int32_field").dataType(DataType.Int32).build());
        fields.add(CreateCollectionReq.FieldSchema.builder()
                .name("int64_field").dataType(DataType.Int64).build());
        fields.add(CreateCollectionReq.FieldSchema.builder()
                .name("float_field").dataType(DataType.Float).build());
        fields.add(CreateCollectionReq.FieldSchema.builder()
                .name("double_field").dataType(DataType.Double).build());
        fields.add(CreateCollectionReq.FieldSchema.builder()
                .name("varchar_field").dataType(DataType.VarChar).maxLength(64).build());
        fields.add(CreateCollectionReq.FieldSchema.builder()
                .name("json_field").dataType(DataType.JSON).build());
        return DescribeCollectionResp.builder()
                .collectionName(COLLECTION_NAME)
                .collectionSchema(CreateCollectionReq.CollectionSchema.builder()
                        .fieldSchemaList(fields).build())
                .collectionID(collectionId)
                .build();
    }

    // ---------- WAL message helper ----------

    /**
     * Build an {@link ImmutableMessage} frame wrapping the given DML payload bytes. The
     * {@code messages.type}, {@code messages.collection} and {@code messages.timetick}
     * properties are populated to mirror real Milvus CDC frames.
     */
    private ImmutableMessage buildWALMessage(String type, byte[] payload, String collectionId, String timetick) {
        return ImmutableMessage.newBuilder()
                .setId(MessageID.newBuilder().setId("msg-" + System.nanoTime()).build())
                .setPayload(ByteString.copyFrom(payload))
                .putProperties("messages.type", type)
                .putProperties("messages.collection", collectionId)
                .putProperties("messages.timetick", timetick)
                .build();
    }

    /** Build an {@link ImmutableMessage} with an explicit message id (for extract tests). */
    private ImmutableMessage buildWALMessageWithId(
            String msgId, String type, byte[] payload, String collectionId, String timetick) {
        ImmutableMessage.Builder builder = ImmutableMessage.newBuilder()
                .setId(MessageID.newBuilder().setId(msgId).build())
                .putProperties("messages.type", type)
                .putProperties("messages.collection", collectionId)
                .putProperties("messages.timetick", timetick);
        if (payload != null) {
            builder.setPayload(ByteString.copyFrom(payload));
        }
        return builder.build();
    }

    // ---------- DeleteRequest primary_keys injection ----------

    /**
     * Build a DeleteRequest with the {@code primary_keys} field (proto field 12, IDs message)
     * injected via raw protobuf wire format. SDK 2.6.8's DeleteRequest builder does not
     * expose this field, so we append it as an unknown length-delimited field.
     *
     * @param collectionName collection name for the DeleteRequest
     * @param expr           optional expr string (may be empty)
     * @param primaryKeys    serialized IDs message (the primary_keys payload)
     * @return serialized DeleteRequest bytes containing the injected primary_keys field
     */
    private static byte[] buildDeleteRequestWithPrimaryKeys(String collectionName, String expr, IDs primaryKeys) {
        DeleteRequest base = DeleteRequest.newBuilder()
                .setCollectionName(collectionName)
                .setExpr(expr)
                .build();
        byte[] baseBytes = base.toByteArray();
        byte[] idsBytes = primaryKeys.toByteArray();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(baseBytes, 0, baseBytes.length);
        // Field 12, wire type 2 (length-delimited): tag = (12 << 3) | 2 = 98 = 0x62
        bos.write(0x62);
        writeVarint(bos, idsBytes.length);
        bos.write(idsBytes, 0, idsBytes.length);
        return bos.toByteArray();
    }

    private static void writeVarint(ByteArrayOutputStream bos, int value) {
        while (value > 0x7F) {
            bos.write((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        bos.write(value & 0x7F);
    }

    // ---------- FloatVector verification helper ----------

    /**
     * Read the first float from a {@link ByteBuffer} produced by the parser for a
     * FloatVector column. Parser-issued buffers are little-endian.
     */
    private static float readFloat(ByteBuffer buf, int index) {
        ByteBuffer dup = buf.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        return dup.getFloat(index * 4);
    }

    // ==================================================================
    // 1. testParseInsertMessage_Int64Pk_FloatVector
    // ==================================================================

    @Test
    public void testParseInsertMessage_Int64Pk_FloatVector() {
        DescribeCollectionResp schema = buildTestSchema(1L);
        MilvusEventParser parser = new MilvusEventParser(schema, "id");

        // FloatVector data: 3 rows x dim=8 = 24 floats
        FloatArray.Builder floatVecBuilder = FloatArray.newBuilder();
        for (int r = 0; r < 3; r++) {
            for (int d = 0; d < 8; d++) {
                floatVecBuilder.addData((float) (r * 8 + d + 1));
            }
        }

        InsertRequest insertReq = InsertRequest.newBuilder()
                .setCollectionName(COLLECTION_NAME)
                .setNumRows(3)
                .addFieldsData(FieldData.newBuilder()
                        .setFieldName("id")
                        .setType(io.milvus.grpc.DataType.Int64)
                        .setScalars(ScalarField.newBuilder()
                                .setLongData(LongArray.newBuilder()
                                        .addData(1L).addData(2L).addData(3L).build())
                                .build())
                        .build())
                .addFieldsData(FieldData.newBuilder()
                        .setFieldName("vector")
                        .setType(io.milvus.grpc.DataType.FloatVector)
                        .setVectors(VectorField.newBuilder()
                                .setDim(8)
                                .setFloatVector(floatVecBuilder.build())
                                .build())
                        .build())
                .addFieldsData(FieldData.newBuilder()
                        .setFieldName("category")
                        .setType(io.milvus.grpc.DataType.VarChar)
                        .setScalars(ScalarField.newBuilder()
                                .setStringData(StringArray.newBuilder()
                                        .addData("a").addData("b").addData("c").build())
                                .build())
                        .build())
                .build();

        ImmutableMessage msg = buildWALMessage("Insert", insertReq.toByteArray(), "1", "100");

        List<SeaTunnelRow> rows = parser.parseImmutableMessage(msg);
        assertEquals(3, rows.size(), "should produce 3 rows");
        assertEquals(RowKind.INSERT, rows.get(0).getRowKind());
        assertEquals(RowKind.INSERT, rows.get(2).getRowKind());

        // PK values at field index 0
        assertEquals(1L, rows.get(0).getField(0));
        assertEquals(2L, rows.get(1).getField(0));
        assertEquals(3L, rows.get(2).getField(0));

        // Category values at field index 2
        assertEquals("a", rows.get(0).getField(2));
        assertEquals("b", rows.get(1).getField(2));
        assertEquals("c", rows.get(2).getField(2));

        // Vector bytes at field index 1 — verify first float of each row.
        Object vec0 = rows.get(0).getField(1);
        assertNotNull(vec0, "vector field should not be null");
        assertTrue(vec0 instanceof ByteBuffer, "vector should be a ByteBuffer");
        ByteBuffer bb0 = (ByteBuffer) vec0;
        // dim=8 floats * 4 bytes = 32 bytes
        assertEquals(32, bb0.remaining(), "float vector byte buffer should be 32 bytes");
        assertEquals(1.0f, readFloat(bb0, 0), 1e-6f, "row 0 first float");

        ByteBuffer bb1 = (ByteBuffer) rows.get(1).getField(1);
        assertEquals(9.0f, readFloat(bb1, 0), 1e-6f, "row 1 first float");

        ByteBuffer bb2 = (ByteBuffer) rows.get(2).getField(1);
        assertEquals(17.0f, readFloat(bb2, 0), 1e-6f, "row 2 first float");

        // TableId should equal the collection name (no partition).
        assertEquals(COLLECTION_NAME, rows.get(0).getTableId());
    }

    // ==================================================================
    // 2. testParseInsertMessage_VarCharPk
    // ==================================================================

    @Test
    public void testParseInsertMessage_VarCharPk() {
        DescribeCollectionResp schema = buildVarCharPkSchema(1L);
        MilvusEventParser parser = new MilvusEventParser(schema, "id");

        FloatArray.Builder floatVecBuilder = FloatArray.newBuilder();
        for (int r = 0; r < 3; r++) {
            for (int d = 0; d < 8; d++) {
                floatVecBuilder.addData((float) (r * 8 + d));
            }
        }

        InsertRequest insertReq = InsertRequest.newBuilder()
                .setCollectionName(COLLECTION_NAME)
                .setNumRows(3)
                .addFieldsData(FieldData.newBuilder()
                        .setFieldName("id")
                        .setType(io.milvus.grpc.DataType.VarChar)
                        .setScalars(ScalarField.newBuilder()
                                .setStringData(StringArray.newBuilder()
                                        .addData("a").addData("b").addData("c").build())
                                .build())
                        .build())
                .addFieldsData(FieldData.newBuilder()
                        .setFieldName("vector")
                        .setType(io.milvus.grpc.DataType.FloatVector)
                        .setVectors(VectorField.newBuilder()
                                .setDim(8)
                                .setFloatVector(floatVecBuilder.build())
                                .build())
                        .build())
                .build();

        ImmutableMessage msg = buildWALMessage("Insert", insertReq.toByteArray(), "1", "100");

        List<SeaTunnelRow> rows = parser.parseImmutableMessage(msg);
        assertEquals(3, rows.size(), "should produce 3 rows");
        assertEquals(RowKind.INSERT, rows.get(0).getRowKind());

        // String PK values at field index 0
        assertEquals("a", rows.get(0).getField(0));
        assertEquals("b", rows.get(1).getField(0));
        assertEquals("c", rows.get(2).getField(0));

        // Vector bytes at field index 1
        ByteBuffer bb = (ByteBuffer) rows.get(1).getField(1);
        assertNotNull(bb);
        assertEquals(32, bb.remaining(), "float vector byte buffer should be 32 bytes");
    }

    // ==================================================================
    // 3. testParseInsertMessage_BinaryVector
    // ==================================================================

    @Test
    public void testParseInsertMessage_BinaryVector() {
        DescribeCollectionResp schema = buildBinaryVectorSchema(1L);
        MilvusEventParser parser = new MilvusEventParser(schema, "id");

        // BinaryVector: dim=128, 2 rows -> 32 bytes total (16 bytes per row).
        byte[] binaryData = new byte[32];
        for (int i = 0; i < 32; i++) {
            binaryData[i] = (byte) i;
        }

        InsertRequest insertReq = InsertRequest.newBuilder()
                .setCollectionName(COLLECTION_NAME)
                .setNumRows(2)
                .addFieldsData(FieldData.newBuilder()
                        .setFieldName("id")
                        .setType(io.milvus.grpc.DataType.Int64)
                        .setScalars(ScalarField.newBuilder()
                                .setLongData(LongArray.newBuilder()
                                        .addData(1L).addData(2L).build())
                                .build())
                        .build())
                .addFieldsData(FieldData.newBuilder()
                        .setFieldName("binary_vec")
                        .setType(io.milvus.grpc.DataType.BinaryVector)
                        .setVectors(VectorField.newBuilder()
                                .setDim(128)
                                .setBinaryVector(
                                        milvus.com.google.protobuf.ByteString.copyFrom(binaryData))
                                .build())
                        .build())
                .build();

        ImmutableMessage msg = buildWALMessage("Insert", insertReq.toByteArray(), "1", "100");

        List<SeaTunnelRow> rows = parser.parseImmutableMessage(msg);
        assertEquals(2, rows.size(), "should produce 2 rows");
        assertEquals(RowKind.INSERT, rows.get(0).getRowKind());

        // PK values
        assertEquals(1L, rows.get(0).getField(0));
        assertEquals(2L, rows.get(1).getField(0));

        // BinaryVector ByteBuffer slicing — 16 bytes per row.
        ByteBuffer bb0 = (ByteBuffer) rows.get(0).getField(1);
        ByteBuffer bb1 = (ByteBuffer) rows.get(1).getField(1);
        assertNotNull(bb0);
        assertNotNull(bb1);
        assertEquals(16, bb0.remaining(), "row 0 binary vector should be 16 bytes");
        assertEquals(16, bb1.remaining(), "row 1 binary vector should be 16 bytes");

        // Verify the actual bytes — row 0 = bytes [0..15], row 1 = bytes [16..31].
        byte[] row0Bytes = new byte[16];
        bb0.get(row0Bytes);
        for (int i = 0; i < 16; i++) {
            assertEquals((byte) i, row0Bytes[i], "row 0 byte " + i);
        }

        byte[] row1Bytes = new byte[16];
        bb1.get(row1Bytes);
        for (int i = 0; i < 16; i++) {
            assertEquals((byte) (i + 16), row1Bytes[i], "row 1 byte " + i);
        }
    }

    // ==================================================================
    // 4. testParseInsertMessage_MultipleScalarFields
    // ==================================================================

    @Test
    public void testParseInsertMessage_MultipleScalarFields() {
        DescribeCollectionResp schema = buildMultiScalarSchema(1L);
        MilvusEventParser parser = new MilvusEventParser(schema, "id");

        String jsonPayload = "{\"k\":\"v\"}";

        InsertRequest insertReq = InsertRequest.newBuilder()
                .setCollectionName(COLLECTION_NAME)
                .setNumRows(1)
                .addFieldsData(FieldData.newBuilder()
                        .setFieldName("id")
                        .setType(io.milvus.grpc.DataType.Int64)
                        .setScalars(ScalarField.newBuilder()
                                .setLongData(LongArray.newBuilder().addData(1L).build())
                                .build())
                        .build())
                .addFieldsData(FieldData.newBuilder()
                        .setFieldName("bool_field")
                        .setType(io.milvus.grpc.DataType.Bool)
                        .setScalars(ScalarField.newBuilder()
                                .setBoolData(BoolArray.newBuilder().addData(true).build())
                                .build())
                        .build())
                .addFieldsData(FieldData.newBuilder()
                        .setFieldName("int32_field")
                        .setType(io.milvus.grpc.DataType.Int32)
                        .setScalars(ScalarField.newBuilder()
                                .setIntData(IntArray.newBuilder().addData(42).build())
                                .build())
                        .build())
                .addFieldsData(FieldData.newBuilder()
                        .setFieldName("int64_field")
                        .setType(io.milvus.grpc.DataType.Int64)
                        .setScalars(ScalarField.newBuilder()
                                .setLongData(LongArray.newBuilder().addData(100L).build())
                                .build())
                        .build())
                .addFieldsData(FieldData.newBuilder()
                        .setFieldName("float_field")
                        .setType(io.milvus.grpc.DataType.Float)
                        .setScalars(ScalarField.newBuilder()
                                .setFloatData(FloatArray.newBuilder().addData(1.5f).build())
                                .build())
                        .build())
                .addFieldsData(FieldData.newBuilder()
                        .setFieldName("double_field")
                        .setType(io.milvus.grpc.DataType.Double)
                        .setScalars(ScalarField.newBuilder()
                                .setDoubleData(DoubleArray.newBuilder().addData(2.5).build())
                                .build())
                        .build())
                .addFieldsData(FieldData.newBuilder()
                        .setFieldName("varchar_field")
                        .setType(io.milvus.grpc.DataType.VarChar)
                        .setScalars(ScalarField.newBuilder()
                                .setStringData(StringArray.newBuilder().addData("hello").build())
                                .build())
                        .build())
                .addFieldsData(FieldData.newBuilder()
                        .setFieldName("json_field")
                        .setType(io.milvus.grpc.DataType.JSON)
                        .setScalars(ScalarField.newBuilder()
                                .setJsonData(JSONArray.newBuilder()
                                        .addData(milvus.com.google.protobuf.ByteString
                                                .copyFromUtf8(jsonPayload))
                                        .build())
                                .build())
                        .build())
                .build();

        ImmutableMessage msg = buildWALMessage("Insert", insertReq.toByteArray(), "1", "100");

        List<SeaTunnelRow> rows = parser.parseImmutableMessage(msg);
        assertEquals(1, rows.size(), "should produce 1 row");
        assertEquals(RowKind.INSERT, rows.get(0).getRowKind());

        SeaTunnelRow row = rows.get(0);
        // Schema field order: [0]=id, [1]=bool_field, [2]=int32_field, [3]=int64_field,
        //                     [4]=float_field, [5]=double_field, [6]=varchar_field, [7]=json_field
        assertEquals(1L, row.getField(0), "id");
        assertEquals(true, row.getField(1), "bool_field");
        assertEquals(42, row.getField(2), "int32_field");
        assertEquals(100L, row.getField(3), "int64_field");
        assertEquals(1.5f, row.getField(4), "float_field");
        assertEquals(2.5, row.getField(5), "double_field");
        assertEquals("hello", row.getField(6), "varchar_field");
        assertEquals(jsonPayload, row.getField(7), "json_field");
    }

    // ==================================================================
    // 5. testParseDeleteMessage_Int64Pk
    // ==================================================================

    @Test
    public void testParseDeleteMessage_Int64Pk() {
        DescribeCollectionResp schema = buildTestSchema(1L);
        MilvusEventParser parser = new MilvusEventParser(schema, "id");

        IDs primaryKeys = IDs.newBuilder()
                .setIntId(LongArray.newBuilder()
                        .addData(10L).addData(20L).addData(30L).build())
                .build();
        byte[] deleteBytes = buildDeleteRequestWithPrimaryKeys(COLLECTION_NAME, "", primaryKeys);

        ImmutableMessage msg = buildWALMessage("Delete", deleteBytes, "1", "100");

        List<SeaTunnelRow> rows = parser.parseImmutableMessage(msg);
        assertEquals(3, rows.size(), "should produce 3 delete rows");
        assertEquals(RowKind.DELETE, rows.get(0).getRowKind());
        assertEquals(RowKind.DELETE, rows.get(1).getRowKind());
        assertEquals(RowKind.DELETE, rows.get(2).getRowKind());

        // PK values at field index 0 (id is the primary key)
        assertEquals(10L, rows.get(0).getField(0));
        assertEquals(20L, rows.get(1).getField(0));
        assertEquals(30L, rows.get(2).getField(0));

        // TableId should be the collection name.
        assertEquals(COLLECTION_NAME, rows.get(0).getTableId());
    }

    // ==================================================================
    // 6. testParseDeleteMessage_StringPk
    // ==================================================================

    @Test
    public void testParseDeleteMessage_StringPk() {
        DescribeCollectionResp schema = buildVarCharPkSchema(1L);
        MilvusEventParser parser = new MilvusEventParser(schema, "id");

        IDs primaryKeys = IDs.newBuilder()
                .setStrId(StringArray.newBuilder()
                        .addData("x").addData("y").build())
                .build();
        byte[] deleteBytes = buildDeleteRequestWithPrimaryKeys(COLLECTION_NAME, "", primaryKeys);

        ImmutableMessage msg = buildWALMessage("Delete", deleteBytes, "1", "100");

        List<SeaTunnelRow> rows = parser.parseImmutableMessage(msg);
        assertEquals(2, rows.size(), "should produce 2 delete rows");
        assertEquals(RowKind.DELETE, rows.get(0).getRowKind());
        assertEquals(RowKind.DELETE, rows.get(1).getRowKind());

        // String PK values at field index 0
        assertEquals("x", rows.get(0).getField(0));
        assertEquals("y", rows.get(1).getField(0));
    }

    // ==================================================================
    // 7. testParseDeleteMessage_NoPrimaryKeys
    // ==================================================================

    @Test
    public void testParseDeleteMessage_NoPrimaryKeys() {
        DescribeCollectionResp schema = buildTestSchema(1L);
        MilvusEventParser parser = new MilvusEventParser(schema, "id");

        // Build a DeleteRequest with only an expr (no primary_keys field 12 injected).
        DeleteRequest deleteReq = DeleteRequest.newBuilder()
                .setCollectionName(COLLECTION_NAME)
                .setExpr("id in [1, 2, 3]")
                .build();

        ImmutableMessage msg = buildWALMessage("Delete", deleteReq.toByteArray(), "1", "100");

        List<SeaTunnelRow> rows = parser.parseImmutableMessage(msg);
        assertTrue(rows.isEmpty(), "delete without primary_keys should return empty list");
    }

    // ==================================================================
    // 8. testParseNonDmlMessage_TimeTick
    // ==================================================================

    @Test
    public void testParseNonDmlMessage_TimeTick() {
        DescribeCollectionResp schema = buildTestSchema(1L);
        MilvusEventParser parser = new MilvusEventParser(schema, "id");

        // A TimeTick message carries no DML payload — just an empty body and the type property.
        ImmutableMessage msg = buildWALMessage("TimeTick", new byte[0], "1", "100");

        List<SeaTunnelRow> rows = parser.parseImmutableMessage(msg);
        assertTrue(rows.isEmpty(), "non-DML TimeTick message should return empty list");
    }

    // ==================================================================
    // 9. testParseMessage_CollectionFilter
    // ==================================================================

    @Test
    public void testParseMessage_CollectionFilter() {
        // Parser is configured for collectionID=1, so collection filtering is enabled.
        DescribeCollectionResp schema = buildTestSchema(1L);
        MilvusEventParser parser = new MilvusEventParser(schema, "id");

        InsertRequest insertReq = InsertRequest.newBuilder()
                .setCollectionName(COLLECTION_NAME)
                .setNumRows(2)
                .addFieldsData(FieldData.newBuilder()
                        .setFieldName("id")
                        .setType(io.milvus.grpc.DataType.Int64)
                        .setScalars(ScalarField.newBuilder()
                                .setLongData(LongArray.newBuilder()
                                        .addData(1L).addData(2L).build())
                                .build())
                        .build())
                .build();

        // 1) Message carrying collection="2" should be filtered out (does not match target=1).
        ImmutableMessage mismatchMsg =
                buildWALMessage("Insert", insertReq.toByteArray(), "2", "100");
        List<SeaTunnelRow> mismatchRows = parser.parseImmutableMessage(mismatchMsg);
        assertTrue(mismatchRows.isEmpty(), "message with mismatched collection should be filtered");

        // 2) Message carrying collection="1" should be parsed normally.
        ImmutableMessage matchMsg =
                buildWALMessage("Insert", insertReq.toByteArray(), "1", "100");
        List<SeaTunnelRow> matchRows = parser.parseImmutableMessage(matchMsg);
        assertEquals(2, matchRows.size(), "message with matching collection should be parsed");
        assertEquals(RowKind.INSERT, matchRows.get(0).getRowKind());
        assertEquals(1L, matchRows.get(0).getField(0));
        assertEquals(2L, matchRows.get(1).getField(0));
    }

    // ==================================================================
    // 10. testExtractTimetick
    // ==================================================================

    @Test
    public void testExtractTimetick() {
        DescribeCollectionResp schema = buildTestSchema(1L);
        MilvusEventParser parser = new MilvusEventParser(schema, "id");

        // Valid timetick value
        ImmutableMessage validMsg = buildWALMessage("Insert", new byte[0], "1", "12345");
        assertEquals(12345L, parser.extractTimetick(validMsg), "valid timetick should be parsed");

        // Missing timetick property (build message without the property)
        ImmutableMessage missingTimetickMsg = ImmutableMessage.newBuilder()
                .setId(MessageID.newBuilder().setId("no-tt").build())
                .putProperties("messages.type", "Insert")
                .putProperties("messages.collection", "1")
                .build();
        assertEquals(-1L, parser.extractTimetick(missingTimetickMsg),
                "missing timetick should return -1");

        // Empty timetick value
        ImmutableMessage emptyTimetickMsg = buildWALMessage("Insert", new byte[0], "1", "");
        assertEquals(-1L, parser.extractTimetick(emptyTimetickMsg),
                "empty timetick should return -1");

        // Unparsable timetick value
        ImmutableMessage unparsableMsg = buildWALMessage("Insert", new byte[0], "1", "not-a-number");
        assertEquals(-1L, parser.extractTimetick(unparsableMsg),
                "unparsable timetick should return -1");

        // Null message
        assertEquals(-1L, parser.extractTimetick(null),
                "null message should return -1");

        // Unsigned large value (within uint64 range)
        ImmutableMessage unsignedMsg = buildWALMessage("Insert", new byte[0], "1", "18446744073709551615");
        // Long.parseUnsignedLong treats this as the max uint64, which is -1L as a signed long.
        assertEquals(-1L, parser.extractTimetick(unsignedMsg),
                "max uint64 timetick should be parsed as -1L (signed)");
    }

    // ==================================================================
    // 11. testExtractMessageIdString
    // ==================================================================

    @Test
    public void testExtractMessageIdString() {
        DescribeCollectionResp schema = buildTestSchema(1L);
        MilvusEventParser parser = new MilvusEventParser(schema, "id");

        // Message with explicit id
        ImmutableMessage msgWithId =
                buildWALMessageWithId("test-id-123", "Insert", new byte[0], "1", "100");
        assertEquals("test-id-123", parser.extractMessageIdString(msgWithId),
                "should extract the message id string");

        // Message with empty id
        ImmutableMessage msgEmptyId =
                buildWALMessageWithId("", "Insert", new byte[0], "1", "100");
        assertEquals("", parser.extractMessageIdString(msgEmptyId),
                "empty id should return empty string");

        // Null message
        assertEquals("", parser.extractMessageIdString(null),
                "null message should return empty string");

        // Message with a multi-segment id containing special characters
        ImmutableMessage msgSpecial =
                buildWALMessageWithId("by-dev-rootcoord-dml_0:42:abc", "Insert", new byte[0], "1", "100");
        assertEquals("by-dev-rootcoord-dml_0:42:abc",
                parser.extractMessageIdString(msgSpecial),
                "should preserve special characters in the id");
    }
}
