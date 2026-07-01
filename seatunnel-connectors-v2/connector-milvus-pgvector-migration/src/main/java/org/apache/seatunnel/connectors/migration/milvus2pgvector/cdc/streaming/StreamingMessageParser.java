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
import org.apache.seatunnel.connectors.streaming.proto.WALMessage;

import com.google.protobuf.InvalidProtocolBufferException;
import io.milvus.grpc.DeleteRequest;
import io.milvus.grpc.InsertRequest;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
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
 *     payload: serialized WALMessage {
 *         payload: serialized InsertRequest / DeleteRequest / ...
 *         properties: message-level metadata
 *     }
 *     properties: {
 *         _t: message type (1=TimeTick, 2=Insert, 3=Delete, ...)
 *         _tt: time tick
 *         _v: version
 *         ...
 *     }
 * }
 * </pre>
 */
@Slf4j
public class StreamingMessageParser {

    private final DescribeCollectionResp collectionDesc;
    private final String primaryKeyField;
    private final long collectionId;

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
        this.collectionId = collectionDesc.getCollectionID();
    }

    /**
     * Parse message type from ImmutableMessage properties.
     *
     * @param message ImmutableMessage from WAL stream
     * @return Message type integer: 1=TimeTick, 2=Insert, 3=Delete, etc.
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
     *
     * @param message ImmutableMessage from WAL stream
     * @return List of SeaTunnelRow (may be empty for control messages like TimeTick)
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
                // Control messages, skip
                return new ArrayList<>();

            default:
                log.debug("Unknown message type: {}, properties: {}",
                        messageType, message.getPropertiesMap());
                return new ArrayList<>();
        }
    }

    /**
     * Parse Insert message into SeaTunnelRow(s).
     * The ImmutableMessage payload wraps a WALMessage, which wraps an InsertRequest.
     *
     * @param message ImmutableMessage
     * @return List of SeaTunnelRow with RowKind.INSERT
     */
    private List<SeaTunnelRow> parseInsertMessage(ImmutableMessage message) {
        List<SeaTunnelRow> rows = new ArrayList<>();

        try {
            byte[] rawPayload = message.getPayload().toByteArray();

            // Debug: dump first bytes
            if (log.isWarnEnabled()) {
                StringBuilder hex = new StringBuilder();
                int dumpLen = Math.min(rawPayload.length, 128);
                for (int i = 0; i < dumpLen; i++) {
                    hex.append(String.format("%02x ", rawPayload[i]));
                }
                log.warn("Insert msg payload hex ({} bytes): {}", rawPayload.length, hex.toString());
                log.warn("Insert msg properties: {}", message.getPropertiesMap());
            }

            // Try direct InsertRequest parsing (ImmutableMessage.payload = raw InsertRequest)
            InsertRequest insertRequest;
            try {
                insertRequest = InsertRequest.parseFrom(rawPayload);
            } catch (Exception e) {
                // Try WALMessage wrapper as fallback
                try {
                    WALMessage walMessage = WALMessage.parseFrom(rawPayload);
                    byte[] innerPayload = walMessage.getPayload().toByteArray();
                    insertRequest = InsertRequest.parseFrom(innerPayload);
                } catch (Exception e2) {
                    log.warn("Failed to parse Insert payload ({} bytes): inner={}", rawPayload.length, e2.getMessage());
                    return rows;
                }
            }

            // Step 3: Extract field data from InsertRequest
            List<io.milvus.grpc.FieldData> fieldsDataList = insertRequest.getFieldsDataList();
            if (fieldsDataList.isEmpty()) {
                log.debug("Insert message has no fields_data, skipping");
                return rows;
            }

            // Get the number of rows from the first field's data
            long numRows = getFieldDataRowCount(fieldsDataList.get(0));
            if (numRows <= 0) {
                log.debug("Insert message has 0 rows");
                return rows;
            }

            // Get field names from collection schema
            List<CreateCollectionReq.FieldSchema> fieldSchemas =
                    collectionDesc.getCollectionSchema().getFieldSchemaList();

            // Create rows
            for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
                SeaTunnelRow row = new SeaTunnelRow(fieldSchemas.size());
                row.setRowKind(RowKind.INSERT);

                for (int fieldIdx = 0; fieldIdx < fieldSchemas.size() && fieldIdx < fieldsDataList.size(); fieldIdx++) {
                    Object value = extractFieldValue(fieldsDataList.get(fieldIdx), rowIdx,
                            fieldSchemas.get(fieldIdx));
                    row.setField(fieldIdx, value);
                }
                rows.add(row);
            }

            log.debug("Parsed Insert message: {} rows", rows.size());
        } catch (Exception e) {
            log.warn("Failed to parse Insert message: {}", e.getMessage());
        }

        return rows;
    }

    /**
     * Parse Delete message into SeaTunnelRow(s).
     * Delete messages contain primary keys to be removed.
     *
     * @param message ImmutableMessage
     * @return List of SeaTunnelRow with RowKind.DELETE
     */
    private List<SeaTunnelRow> parseDeleteMessage(ImmutableMessage message) {
        List<SeaTunnelRow> rows = new ArrayList<>();

        try {
            byte[] rawPayload = message.getPayload().toByteArray();

            // Try direct DeleteRequest parsing first
            DeleteRequest deleteRequest;
            try {
                deleteRequest = DeleteRequest.parseFrom(rawPayload);
            } catch (Exception e) {
                // Try WALMessage wrapper as fallback
                try {
                    WALMessage walMessage = WALMessage.parseFrom(rawPayload);
                    deleteRequest = DeleteRequest.parseFrom(walMessage.getPayload().toByteArray());
                } catch (Exception e2) {
                    log.warn("Failed to parse Delete payload ({} bytes): {}", rawPayload.length, e2.getMessage());
                    return rows;
                }
            }

            // Step 3: Extract delete info
            // The Milvus DeleteRequest uses expressions (e.g., "id >= 0 && id < 30")
            // Try to extract PK range from the expression
            String expr = deleteRequest.getExpr();
            int numFields = collectionDesc.getCollectionSchema().getFieldSchemaList().size();

            if (expr != null && !expr.isEmpty()) {
                log.info("Delete expression: {} for collection={}", expr, deleteRequest.getCollectionName());

                // Try to parse simple range expression: "id >= M && id < N"
                try {
                    java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(
                            "id\\s*>=\\s*(-?\\d+)\\s*&&\\s*id\\s*<\\s*(-?\\d+)");
                    java.util.regex.Matcher matcher = pattern.matcher(expr);
                    if (matcher.find()) {
                        long startId = Long.parseLong(matcher.group(1));
                        long endId = Long.parseLong(matcher.group(2));
                        log.info("Parsed delete range: id {}..{}", startId, endId - 1);
                        for (long pk = startId; pk < endId; pk++) {
                            SeaTunnelRow row = new SeaTunnelRow(numFields);
                            row.setRowKind(RowKind.DELETE);
                            row.setField(0, pk);
                            rows.add(row);
                        }
                        return rows;
                    }
                } catch (Exception ex) {
                    log.warn("Failed to parse delete expression '{}': {}", expr, ex.getMessage());
                }
            }

            // Fallback: check hash_keys for Int64 PKs
            java.util.List<Integer> hashKeys = deleteRequest.getHashKeysList();
            if (!hashKeys.isEmpty()) {
                for (Integer pk : hashKeys) {
                    SeaTunnelRow row = new SeaTunnelRow(numFields);
                    row.setRowKind(RowKind.DELETE);
                    row.setField(0, (long) pk);
                    rows.add(row);
                }
                log.info("Extracted {} PKs from hash_keys (Int64)", rows.size());
                return rows;
            }

            log.debug("Delete message has no extractable primary keys, expr='{}'", expr);
        } catch (Exception e) {
            log.warn("Failed to parse Delete message: {}", e.getMessage());
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
            return vectors.getFloatVector().getDataCount() / vectors.getDim();
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

            // Int64 (Long)
            if (scalars.hasLongData()) {
                return scalars.getLongData().getData(idx);
            }
            // Int32
            if (scalars.hasIntData()) {
                return (long) scalars.getIntData().getData(idx);
            }
            // Float
            if (scalars.hasFloatData()) {
                return scalars.getFloatData().getData(idx);
            }
            // Double
            if (scalars.hasDoubleData()) {
                return scalars.getDoubleData().getData(idx);
            }
            // Boolean
            if (scalars.hasBoolData()) {
                return scalars.getBoolData().getData(idx);
            }
            // String
            if (scalars.hasStringData()) {
                return scalars.getStringData().getData(idx);
            }
            // JSON
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
     *
     * @param message ImmutableMessage
     * @return MessageID proto object
     */
    public MessageID extractMessageID(ImmutableMessage message) {
        return message.getId();
    }

    /**
     * Extract timetick from ImmutableMessage properties.
     *
     * @param message ImmutableMessage
     * @return timetick value (as long)
     */
    public long extractTimetick(ImmutableMessage message) {
        Map<String, String> properties = message.getPropertiesMap();
        String timetickStr = properties.getOrDefault(TIMETICK_KEY, "0");
        // Timetick is base36 encoded
        try {
            return Long.parseLong(timetickStr, 36);
        } catch (NumberFormatException e) {
            log.warn("Invalid timetick format: {}, using 0", timetickStr);
            return 0;
        }
    }

    /**
     * Check if message is a data message (Insert/Delete).
     *
     * @param message ImmutableMessage
     * @return true if it's a data message
     */
    public boolean isDataMessage(ImmutableMessage message) {
        int messageType = parseMessageTypeInt(message);
        return messageType == MSG_TYPE_INSERT || messageType == MSG_TYPE_DELETE;
    }
}
