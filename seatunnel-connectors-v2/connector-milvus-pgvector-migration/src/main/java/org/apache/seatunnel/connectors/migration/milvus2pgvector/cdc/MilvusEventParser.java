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

import io.milvus.grpc.DeleteRequest;
import io.milvus.grpc.FieldData;
import io.milvus.grpc.IDs;
import io.milvus.grpc.InsertRequest;
import io.milvus.grpc.LongArray;
import io.milvus.grpc.StringArray;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;

import milvus.com.google.protobuf.InvalidProtocolBufferException;
import milvus.com.google.protobuf.UnknownFieldSet;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/*
 * NOTE: The SDK's io.milvus.grpc.* proto classes are shaded by connector-milvus's
 * maven-shade-plugin (com.google → milvus.com.google). When the migration module
 * depends on the shaded connector-milvus JAR, the SDK classes on the compile
 * classpath use milvus.com.google.protobuf.ByteString instead of
 * com.google.protobuf.ByteString. We bridge the two protobuf universes by
 * converting via byte[] (toByteArray / parseFrom(byte[])).
 */

/**
 * Converts Milvus CDC WAL event messages to {@link SeaTunnelRow} objects.
 *
 * <p>The new event-stream CDC pipeline consumes {@link ImmutableMessage} frames
 * from the {@code DumpMessages} gRPC stream. Each frame's {@code payload} field
 * contains a serialized Milvus DML message ({@code InsertRequest} or
 * {@code DeleteRequest} from {@code milvus.proto.milvus}). The message type is
 * identified by the {@code messages.type} entry in the {@code properties} map.
 *
 * <p>This parser expands the columnar InsertRequest into one SeaTunnelRow per
 * row ({@code num_rows} rows per message) and produces one SeaTunnelRow per
 * deleted primary key for DeleteRequest messages. Delete events set
 * {@link RowKind#DELETE} so downstream sinks can apply the deletion.
 *
 * <p><b>Known limitation:</b> The SDK 2.6.8 {@code io.milvus.grpc.DeleteRequest}
 * class predates the WAL-level {@code primary_keys} field (proto field 12,
 * type {@code milvus.proto.schema.IDs}). To recover the deleted primary keys
 * we read the raw {@code primary_keys} bytes from the message's
 * {@link UnknownFieldSet} and re-parse them as {@link IDs}.
 */
@Slf4j
public class MilvusEventParser {

    /** Property key carrying the WAL message type name (e.g. "Insert", "Delete"). */
    public static final String PROP_MESSAGE_TYPE = "messages.type";

    /** Property key carrying the source collection ID (string form). */
    public static final String PROP_COLLECTION_ID = "messages.collection";

    /** Property key carrying the source vchannel name. */
    public static final String PROP_VCHANNEL = "messages.vchannel";

    /** Property key carrying the message timetick (string form of uint64). */
    public static final String PROP_TIMETICK = "messages.timetick";

    /** Field number of the deprecated {@code int64_primary_keys} field (LongArray). */
    private static final int FIELD_INT64_PRIMARY_KEYS = 9;

    /** Field number of the current {@code primary_keys} field (IDs message). */
    private static final int FIELD_PRIMARY_KEYS = 12;

    private final DescribeCollectionResp collectionDesc;
    private final String primaryKeyField;
    private final List<String> fieldNames;
    private final List<CreateCollectionReq.FieldSchema> fieldSchemas;
    private final long targetCollectionId;
    private final boolean collectionFilterEnabled;

    public MilvusEventParser(DescribeCollectionResp collectionDesc, String primaryKeyField) {
        this.collectionDesc = collectionDesc;
        this.primaryKeyField = primaryKeyField;
        this.fieldSchemas = collectionDesc.getCollectionSchema().getFieldSchemaList();
        this.fieldNames = new ArrayList<>();
        for (CreateCollectionReq.FieldSchema field : this.fieldSchemas) {
            this.fieldNames.add(field.getName());
        }
        this.targetCollectionId = collectionDesc.getCollectionID();
        // Enable collection filtering only when target collection ID is positive;
        // otherwise we cannot reliably filter and pass every DML event through.
        this.collectionFilterEnabled = targetCollectionId > 0;
    }

    /**
     * Parse an {@link ImmutableMessage} into zero or more {@link SeaTunnelRow}
     * instances. A single Insert WAL message typically expands into multiple
     * rows (one per record in the batch). Non-DML messages (TimeTick, DDL)
     * return an empty list.
     *
     * @param msg            the immutable WAL message frame
     * @return parsed rows (empty if the message should be skipped)
     */
    public List<SeaTunnelRow> parseImmutableMessage(ImmutableMessage msg) {
        if (msg == null) {
            return Collections.emptyList();
        }
        Map<String, String> props = msg.getPropertiesMap();
        String messageType = props.get(PROP_MESSAGE_TYPE);
        if (messageType == null) {
            log.debug("Skipping WAL message without messages.type property: id={}", msg.getId());
            return Collections.emptyList();
        }

        // Filter by collection ID when available and configured.
        if (collectionFilterEnabled) {
            String collectionIdStr = props.get(PROP_COLLECTION_ID);
            if (collectionIdStr != null && !collectionIdStr.isEmpty()) {
                try {
                    long msgCollectionId = Long.parseLong(collectionIdStr);
                    if (msgCollectionId != targetCollectionId) {
                        return Collections.emptyList();
                    }
                } catch (NumberFormatException e) {
                    log.debug("Unparsable messages.collection value: {}", collectionIdStr);
                }
            }
        }

        switch (messageType) {
            case "Insert":
                return parseInsert(msg);
            case "Delete":
                return parseDelete(msg);
            default:
                log.debug("Skipping non-DML WAL message of type: {}", messageType);
                return Collections.emptyList();
        }
    }

    /**
     * Get the timetick carried by an {@link ImmutableMessage}, or {@code -1}
     * when the property is absent or unparsable.
     */
    public long extractTimetick(ImmutableMessage msg) {
        if (msg == null) {
            return -1L;
        }
        String tt = msg.getPropertiesMap().get(PROP_TIMETICK);
        if (tt == null || tt.isEmpty()) {
            return -1L;
        }
        try {
            return Long.parseUnsignedLong(tt);
        } catch (NumberFormatException e) {
            return -1L;
        }
    }

    /**
     * Extract the WAL message id as a string suitable for persisting in a
     * {@link ReplicatePosition} and round-tripping through DumpMessages.
     */
    public String extractMessageIdString(ImmutableMessage msg) {
        if (msg == null || msg.getId() == null) {
            return "";
        }
        return msg.getId().getId();
    }

    // ---- Insert handling ----

    private List<SeaTunnelRow> parseInsert(ImmutableMessage msg) {
        InsertRequest insertRequest;
        try {
            // Bridge the non-shaded ByteString (from our ImmutableMessage proto) to the
            // SDK's shaded ByteString universe via byte[].
            insertRequest = InsertRequest.parseFrom(msg.getPayload().toByteArray());
        } catch (InvalidProtocolBufferException e) {
            log.warn("Failed to parse InsertRequest payload, skipping: {}", e.getMessage());
            return Collections.emptyList();
        }

        int numRows = insertRequest.getNumRows();
        if (numRows <= 0) {
            return Collections.emptyList();
        }

        List<FieldData> fieldsData = insertRequest.getFieldsDataList();
        // Build per-field column readers indexed by schema position.
        // Not every schema field has a corresponding FieldData column (e.g.
        // auto-id PK may be absent on insert), and we tolerate nulls.
        Object[][] rows = new Object[numRows][fieldNames.size()];

        for (FieldData fieldData : fieldsData) {
            String fieldName = fieldData.getFieldName();
            int fieldIndex = fieldNames.indexOf(fieldName);
            if (fieldIndex < 0) {
                // Field not in our schema projection (e.g. dynamic field); skip.
                continue;
            }
            CreateCollectionReq.FieldSchema schema = fieldSchemas.get(fieldIndex);
            try {
                populateColumn(rows, fieldIndex, fieldData, schema, numRows);
            } catch (RuntimeException e) {
                log.debug("Failed to extract field {} from InsertRequest: {}",
                        fieldName, e.getMessage());
            }
        }

        String tableId = buildTableId(insertRequest.getCollectionName(),
                insertRequest.getPartitionName());
        List<SeaTunnelRow> result = new ArrayList<>(numRows);
        for (int r = 0; r < numRows; r++) {
            SeaTunnelRow row = new SeaTunnelRow(rows[r]);
            row.setRowKind(RowKind.INSERT);
            row.setTableId(tableId);
            result.add(row);
        }
        return result;
    }

    /**
     * Slice one FieldData column into per-row values and store them in
     * {@code rows[r][fieldIndex]}. The columnar layout differs by DataType.
     */
    private void populateColumn(
            Object[][] rows,
            int fieldIndex,
            FieldData fieldData,
            CreateCollectionReq.FieldSchema schema,
            int numRows) {
        DataType dataType = schema.getDataType();
        switch (dataType) {
            case Bool:
                if (fieldData.hasScalars()) {
                    List<Boolean> data = fieldData.getScalars().getBoolData().getDataList();
                    for (int r = 0; r < Math.min(numRows, data.size()); r++) {
                        rows[r][fieldIndex] = data.get(r);
                    }
                }
                break;
            case Int8:
            case Int16:
            case Int32:
                if (fieldData.hasScalars()) {
                    List<Integer> data = fieldData.getScalars().getIntData().getDataList();
                    for (int r = 0; r < Math.min(numRows, data.size()); r++) {
                        rows[r][fieldIndex] = data.get(r);
                    }
                }
                break;
            case Int64:
                if (fieldData.hasScalars()) {
                    List<Long> data = fieldData.getScalars().getLongData().getDataList();
                    for (int r = 0; r < Math.min(numRows, data.size()); r++) {
                        rows[r][fieldIndex] = data.get(r);
                    }
                }
                break;
            case Float:
                if (fieldData.hasScalars()) {
                    List<Float> data = fieldData.getScalars().getFloatData().getDataList();
                    for (int r = 0; r < Math.min(numRows, data.size()); r++) {
                        rows[r][fieldIndex] = data.get(r);
                    }
                }
                break;
            case Double:
                if (fieldData.hasScalars()) {
                    List<Double> data = fieldData.getScalars().getDoubleData().getDataList();
                    for (int r = 0; r < Math.min(numRows, data.size()); r++) {
                        rows[r][fieldIndex] = data.get(r);
                    }
                }
                break;
            case VarChar:
            case String:
                if (fieldData.hasScalars()) {
                    List<String> data = fieldData.getScalars().getStringData().getDataList();
                    for (int r = 0; r < Math.min(numRows, data.size()); r++) {
                        rows[r][fieldIndex] = data.get(r);
                    }
                }
                break;
            case JSON:
                if (fieldData.hasScalars()) {
                    List<milvus.com.google.protobuf.ByteString> data =
                            fieldData.getScalars().getJsonData().getDataList();
                    for (int r = 0; r < Math.min(numRows, data.size()); r++) {
                        rows[r][fieldIndex] = data.get(r).toStringUtf8();
                    }
                }
                break;
            case FloatVector:
                if (fieldData.hasVectors()) {
                    int dim = schema.getDimension() != null
                            ? schema.getDimension() : 0;
                    List<Float> data = fieldData.getVectors().getFloatVector().getDataList();
                    for (int r = 0; r < numRows; r++) {
                        int offset = r * dim;
                        if (offset + dim > data.size()) {
                            break;
                        }
                        Float[] vec = new Float[dim];
                        for (int i = 0; i < dim; i++) {
                            vec[i] = data.get(offset + i);
                        }
                        rows[r][fieldIndex] = toByteBuffer(vec);
                    }
                }
                break;
            case BinaryVector:
                if (fieldData.hasVectors()) {
                    milvus.com.google.protobuf.ByteString data =
                            fieldData.getVectors().getBinaryVector();
                    int dim = schema.getDimension() != null
                            ? schema.getDimension() : 0;
                    int byteLen = dim / 8;
                    for (int r = 0; r < numRows; r++) {
                        int offset = r * byteLen;
                        if (offset + byteLen > data.size()) {
                            break;
                        }
                        rows[r][fieldIndex] = ByteBuffer.wrap(data.toByteArray(), offset, byteLen);
                    }
                }
                break;
            default:
                log.debug("Field {} has unsupported DataType {} for columnar extraction",
                        schema.getName(), dataType);
                break;
        }
    }

    private static ByteBuffer toByteBuffer(Float[] floats) {
        java.nio.ByteBuffer bb = java.nio.ByteBuffer.allocate(floats.length * 4);
        bb.order(java.nio.ByteOrder.LITTLE_ENDIAN);
        for (Float f : floats) {
            bb.putFloat(f);
        }
        bb.flip();
        return bb;
    }

    // ---- Delete handling ----

    private List<SeaTunnelRow> parseDelete(ImmutableMessage msg) {
        DeleteRequest deleteRequest;
        try {
            // Bridge the non-shaded ByteString to the SDK's shaded ByteString via byte[].
            deleteRequest = DeleteRequest.parseFrom(msg.getPayload().toByteArray());
        } catch (InvalidProtocolBufferException e) {
            log.warn("Failed to parse DeleteRequest payload, skipping: {}", e.getMessage());
            return Collections.emptyList();
        }

        // SDK 2.6.8's DeleteRequest has no primary_keys getter; read it from
        // the protobuf unknown-field set (field 12 = IDs message). Also try
        // the deprecated int64_primary_keys field (field 9 = LongArray) for
        // backward compatibility.
        List<Long> int64Pks = new ArrayList<>();
        List<String> strPks = new ArrayList<>();
        extractPrimaryKeys(deleteRequest, int64Pks, strPks);

        if (int64Pks.isEmpty() && strPks.isEmpty()) {
            log.debug("DeleteRequest carried no extractable primary keys; expr={}",
                    deleteRequest.getExpr());
            return Collections.emptyList();
        }

        int pkIndex = fieldNames.indexOf(primaryKeyField);
        if (pkIndex < 0) {
            log.warn("Primary key field '{}' not found in collection schema; skipping delete",
                    primaryKeyField);
            return Collections.emptyList();
        }

        int total = int64Pks.size() + strPks.size();
        String tableId = buildTableId(deleteRequest.getCollectionName(),
                deleteRequest.getPartitionName());
        List<SeaTunnelRow> result = new ArrayList<>(total);
        for (Long pk : int64Pks) {
            Object[] fields = new Object[fieldNames.size()];
            fields[pkIndex] = pk;
            SeaTunnelRow row = new SeaTunnelRow(fields);
            row.setRowKind(RowKind.DELETE);
            row.setTableId(tableId);
            result.add(row);
        }
        for (String pk : strPks) {
            Object[] fields = new Object[fieldNames.size()];
            fields[pkIndex] = pk;
            SeaTunnelRow row = new SeaTunnelRow(fields);
            row.setRowKind(RowKind.DELETE);
            row.setTableId(tableId);
            result.add(row);
        }
        return result;
    }

    /**
     * Extract deleted primary keys from a {@link DeleteRequest} by inspecting
     * its {@link UnknownFieldSet}. Modern WAL DeleteRequest messages carry the
     * deleted PK list in proto field 12 ({@code IDs} message) or, on older
     * versions, field 9 ({@code LongArray}).
     */
    private void extractPrimaryKeys(
            DeleteRequest deleteRequest, List<Long> int64Pks, List<String> strPks) {
        UnknownFieldSet unknown = deleteRequest.getUnknownFields();
        if (unknown == null) {
            return;
        }

        // Prefer the current primary_keys (field 12) IDs message.
        if (unknown.hasField(FIELD_PRIMARY_KEYS)) {
            for (milvus.com.google.protobuf.ByteString raw : extractLengthDelimitedBytes(unknown, FIELD_PRIMARY_KEYS)) {
                try {
                    IDs ids = IDs.parseFrom(raw);
                    if (ids.hasIntId()) {
                        LongArray arr = ids.getIntId();
                        int64Pks.addAll(arr.getDataList());
                    } else if (ids.hasStrId()) {
                        StringArray arr = ids.getStrId();
                        strPks.addAll(arr.getDataList());
                    }
                } catch (InvalidProtocolBufferException e) {
                    log.debug("Failed to parse primary_keys IDs sub-message: {}", e.getMessage());
                }
            }
            if (!int64Pks.isEmpty() || !strPks.isEmpty()) {
                return;
            }
        }

        // Fallback: deprecated int64_primary_keys (field 9, LongArray).
        for (milvus.com.google.protobuf.ByteString raw : extractLengthDelimitedBytes(unknown, FIELD_INT64_PRIMARY_KEYS)) {
            try {
                LongArray arr = LongArray.parseFrom(raw);
                int64Pks.addAll(arr.getDataList());
            } catch (InvalidProtocolBufferException e) {
                log.debug("Failed to parse int64_primary_keys sub-message: {}", e.getMessage());
            }
        }
    }

    /**
     * Helper that returns the length-delimited bytes for a given unknown field
     * number. Used to extract sub-messages from {@link UnknownFieldSet}.
     */
    private static List<milvus.com.google.protobuf.ByteString> extractLengthDelimitedBytes(
            UnknownFieldSet unknown, int fieldNumber) {
        if (unknown == null || !unknown.hasField(fieldNumber)) {
            return Collections.emptyList();
        }
        UnknownFieldSet.Field field = unknown.getField(fieldNumber);
        return field.getLengthDelimitedList();
    }

    private static String buildTableId(String collectionName, String partitionName) {
        if (partitionName == null || partitionName.isEmpty() || "_default".equals(partitionName)) {
            return collectionName;
        }
        return collectionName + "_" + partitionName;
    }
}
