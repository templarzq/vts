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

import io.milvus.grpc.IDs;
import io.milvus.grpc.LongArray;
import io.milvus.grpc.StringArray;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import lombok.extern.slf4j.Slf4j;
import milvus.proto.msg.Msg;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Parser for ImmutableMessage from StreamingNode WAL stream.
 * Parses the WAL message payload (InsertRequest/DeleteRequest) into SeaTunnelRow.
 *
 * <p>WAL message structure:
 * <pre>
 * ImmutableMessage {
 *     id: MessageID
 *     payload: serialized milvus.proto.msg.Msg$InsertRequest or Msg$DeleteRequest
 *     properties: {
 *         _t: message type (1=TimeTick, 2=Insert, 3=Delete, ...)
 *         _tt: time tick (base36 encoded)
 *         _v: version
 *         _vc: vchannel
 *         _h: header (base64 encoded)
 *         _wt: write type
 *         _lc: last confirmed
 *     }
 * }
 * </pre>
 */
@Slf4j
public class StreamingMessageParser {

    private final DescribeCollectionResp collectionDesc;
    private final String primaryKeyField;

    // Message type key and values (from Milvus WAL properties)
    private static final String MSG_TYPE_KEY = "_t";
    private static final int MSG_TYPE_TIMETICK = 1;
    private static final int MSG_TYPE_INSERT = 2;
    private static final int MSG_TYPE_DELETE = 3;
    private static final int MSG_TYPE_FLUSH = 4;

    // Other property keys
    private static final String TIMETICK_KEY = "_tt";

    /**
     * Constructor.
     *
     * @param collectionDesc  Collection schema description
     * @param primaryKeyField Primary key field name (e.g., "id")
     */
    public StreamingMessageParser(DescribeCollectionResp collectionDesc, String primaryKeyField) {
        this.collectionDesc = collectionDesc;
        this.primaryKeyField = primaryKeyField;
    }

    /**
     * Parse message type from ImmutableMessage properties.
     */
    public int parseMessageTypeInt(ImmutableMessage message) {
        Map<String, String> properties = message.getPropertiesMap();
        String typeStr = properties.getOrDefault(MSG_TYPE_KEY, "-1");
        try {
            return Integer.parseInt(typeStr);
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    /**
     * Parse message type as human-readable string.
     */
    public String parseMessageType(ImmutableMessage message) {
        int type = parseMessageTypeInt(message);
        switch (type) {
            case MSG_TYPE_TIMETICK: return "TimeTick";
            case MSG_TYPE_INSERT: return "Insert";
            case MSG_TYPE_DELETE: return "Delete";
            case MSG_TYPE_FLUSH: return "Flush";
            default: return "Unknown(" + type + ")";
        }
    }

    /**
     * Parse ImmutableMessage into SeaTunnelRow(s).
     */
    public List<SeaTunnelRow> parseMessage(ImmutableMessage message) {
        int messageType = parseMessageTypeInt(message);

        switch (messageType) {
            case MSG_TYPE_INSERT:
                return parseInsertMessage(message);

            case MSG_TYPE_DELETE:
                return parseDeleteMessage(message);

            case MSG_TYPE_TIMETICK:
            case MSG_TYPE_FLUSH:
                return new ArrayList<>();

            default:
                log.debug("Unknown message type: {}", messageType);
                return new ArrayList<>();
        }
    }

    /**
     * Parse Insert message into SeaTunnelRow(s).
     * Uses the internal Milvus proto milvus.proto.msg.Msg$InsertRequest.
     */
    private List<SeaTunnelRow> parseInsertMessage(ImmutableMessage message) {
        List<SeaTunnelRow> rows = new ArrayList<>();

        try {
            byte[] rawPayload = message.getPayload().toByteArray();

            // Parse as internal Milvus Msg$InsertRequest
            Msg.InsertRequest insertRequest = Msg.InsertRequest.parseFrom(rawPayload);

            List<io.milvus.grpc.FieldData> fieldsDataList = insertRequest.getFieldsDataList();
            if (fieldsDataList.isEmpty()) {
                log.warn("InsertRequest has no fields_data, collection={}, numRows={}",
                        insertRequest.getCollectionName(), insertRequest.getNumRows());
                return rows;
            }

            // Get number of rows from the first field's data
            long numRows = insertRequest.getNumRows();
            if (numRows <= 0) {
                numRows = getFieldDataRowCount(fieldsDataList.get(0));
            }
            if (numRows <= 0) {
                log.warn("InsertRequest has 0 rows, collection={}", insertRequest.getCollectionName());
                return rows;
            }

            // Build field_name -> FieldData map for correct matching
            java.util.Map<String, io.milvus.grpc.FieldData> fieldDataMap = new java.util.LinkedHashMap<>();
            for (io.milvus.grpc.FieldData fd : fieldsDataList) {
                String fieldName = fd.getFieldName();
                if (fieldName != null && !fieldName.isEmpty()) {
                    fieldDataMap.put(fieldName, fd);
                }
            }
            log.debug("InsertRequest: collection={}, numRows={}, fields=[{}]",
                    insertRequest.getCollectionName(), numRows,
                    String.join(",", fieldDataMap.keySet()));

            // Get field schemas from collection description
            List<CreateCollectionReq.FieldSchema> fieldSchemas =
                    collectionDesc.getCollectionSchema().getFieldSchemaList();

            // Create SeaTunnelRow for each row
            for (int rowIdx = 0; rowIdx < numRows && rowIdx < Integer.MAX_VALUE; rowIdx++) {
                SeaTunnelRow row = new SeaTunnelRow(fieldSchemas.size());
                row.setRowKind(RowKind.INSERT);

                for (int fieldIdx = 0; fieldIdx < fieldSchemas.size(); fieldIdx++) {
                    CreateCollectionReq.FieldSchema schema = fieldSchemas.get(fieldIdx);
                    io.milvus.grpc.FieldData fd = fieldDataMap.get(schema.getName());
                    if (fd != null) {
                        Object value = extractFieldValue(fd, rowIdx, schema);
                        row.setField(fieldIdx, value);
                    }
                }
                rows.add(row);
            }

            log.info("Parsed Insert: {} rows, collection={}", rows.size(),
                    insertRequest.getCollectionName());

        } catch (Exception e) {
            log.warn("Failed to parse InsertRequest ({} bytes): {}",
                    message.getPayload().size(), e.getMessage());
        }

        return rows;
    }

    /**
     * Parse Delete message into SeaTunnelRow(s).
     * Uses the internal Milvus proto milvus.proto.msg.Msg$DeleteRequest.
     */
    private List<SeaTunnelRow> parseDeleteMessage(ImmutableMessage message) {
        List<SeaTunnelRow> rows = new ArrayList<>();

        try {
            byte[] rawPayload = message.getPayload().toByteArray();

            // Parse as internal Milvus Msg$DeleteRequest
            Msg.DeleteRequest deleteRequest = Msg.DeleteRequest.parseFrom(rawPayload);

            int numFields = collectionDesc.getCollectionSchema().getFieldSchemaList().size();

            // Method 1: Try getPrimaryKeys() for IDs type
            IDs primaryKeys = deleteRequest.getPrimaryKeys();
            if (primaryKeys != null) {
                // Int64 primary keys
                LongArray intId = primaryKeys.getIntId();
                if (intId != null && intId.getDataCount() > 0) {
                    for (Long pk : intId.getDataList()) {
                        SeaTunnelRow row = new SeaTunnelRow(numFields);
                        row.setRowKind(RowKind.DELETE);
                        row.setField(0, pk);
                        rows.add(row);
                    }
                    log.info("Parsed Delete: {} PKs (Int64 IDs), collection={}",
                            rows.size(), deleteRequest.getCollectionName());
                    return rows;
                }

                // String primary keys
                StringArray strId = primaryKeys.getStrId();
                if (strId != null && strId.getDataCount() > 0) {
                    for (String pk : strId.getDataList()) {
                        SeaTunnelRow row = new SeaTunnelRow(numFields);
                        row.setRowKind(RowKind.DELETE);
                        row.setField(0, pk);
                        rows.add(row);
                    }
                    log.info("Parsed Delete: {} PKs (VarChar IDs), collection={}",
                            rows.size(), deleteRequest.getCollectionName());
                    return rows;
                }
            }

            // Method 2: Fallback to getInt64PrimaryKeysList()
            List<Long> int64Pks = deleteRequest.getInt64PrimaryKeysList();
            if (!int64Pks.isEmpty()) {
                for (Long pk : int64Pks) {
                    SeaTunnelRow row = new SeaTunnelRow(numFields);
                    row.setRowKind(RowKind.DELETE);
                    row.setField(0, pk);
                    rows.add(row);
                }
                log.info("Parsed Delete: {} PKs (Int64 list), collection={}",
                        rows.size(), deleteRequest.getCollectionName());
                return rows;
            }

            log.debug("DeleteRequest has no extractable primary keys, collection={}",
                    deleteRequest.getCollectionName());

        } catch (Exception e) {
            log.warn("Failed to parse DeleteRequest ({} bytes): {}",
                    message.getPayload().size(), e.getMessage());
        }

        return rows;
    }

    /**
     * Count the number of rows in a FieldData.
     */
    private long getFieldDataRowCount(io.milvus.grpc.FieldData fieldData) {
        if (fieldData.hasScalars()) {
            io.milvus.grpc.ScalarField scalars = fieldData.getScalars();
            if (scalars.hasLongData()) return scalars.getLongData().getDataCount();
            if (scalars.hasIntData()) return scalars.getIntData().getDataCount();
            if (scalars.hasFloatData()) return scalars.getFloatData().getDataCount();
            if (scalars.hasDoubleData()) return scalars.getDoubleData().getDataCount();
            if (scalars.hasBoolData()) return scalars.getBoolData().getDataCount();
            if (scalars.hasStringData()) return scalars.getStringData().getDataCount();
            if (scalars.hasJsonData()) return scalars.getJsonData().getDataCount();
        }
        if (fieldData.hasVectors()) {
            io.milvus.grpc.VectorField vectors = fieldData.getVectors();
            if (vectors.hasFloatVector()) {
                return vectors.getFloatVector().getDataCount() / vectors.getDim();
            }
        }
        return 0;
    }

    /**
     * Extract a single field value from FieldData at the given row index.
     */
    private Object extractFieldValue(io.milvus.grpc.FieldData fieldData, long rowIdx,
                                     CreateCollectionReq.FieldSchema fieldSchema) {
        if (fieldData.hasScalars()) {
            io.milvus.grpc.ScalarField scalars = fieldData.getScalars();
            int idx = (int) rowIdx;

            if (scalars.hasLongData()) {
                return scalars.getLongData().getData(idx);
            }
            if (scalars.hasIntData()) {
                return (long) scalars.getIntData().getData(idx);
            }
            if (scalars.hasFloatData()) {
                return scalars.getFloatData().getData(idx);
            }
            if (scalars.hasDoubleData()) {
                return scalars.getDoubleData().getData(idx);
            }
            if (scalars.hasBoolData()) {
                return scalars.getBoolData().getData(idx);
            }
            if (scalars.hasStringData()) {
                return scalars.getStringData().getData(idx);
            }
            if (scalars.hasJsonData()) {
                return scalars.getJsonData().getData(idx).toStringUtf8();
            }
        }
        if (fieldData.hasVectors()) {
            io.milvus.grpc.VectorField vectors = fieldData.getVectors();
            if (vectors.hasFloatVector()) {
                int dim = (int) vectors.getDim();
                int offset = (int) (rowIdx * dim);
                List<Float> vec = new ArrayList<>(dim);
                for (int i = 0; i < dim; i++) {
                    vec.add(vectors.getFloatVector().getData(offset + i));
                }
                return vec;
            }
        }
        return null;
    }

    /**
     * Extract MessageID from ImmutableMessage for checkpoint.
     */
    public MessageID extractMessageID(ImmutableMessage message) {
        return message.getId();
    }

    /**
     * Extract timetick from ImmutableMessage properties.
     * Timetick is base36 encoded in the WAL.
     */
    public long extractTimetick(ImmutableMessage message) {
        Map<String, String> properties = message.getPropertiesMap();
        String timetickStr = properties.getOrDefault(TIMETICK_KEY, "0");
        try {
            return Long.parseLong(timetickStr, 36);
        } catch (NumberFormatException e) {
            log.warn("Invalid timetick format: {}, using 0", timetickStr);
            return 0;
        }
    }

    /**
     * Check if message is a data message (Insert/Delete).
     */
    public boolean isDataMessage(ImmutableMessage message) {
        int messageType = parseMessageTypeInt(message);
        return messageType == MSG_TYPE_INSERT || messageType == MSG_TYPE_DELETE;
    }
}
