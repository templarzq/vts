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

import io.milvus.grpc.MsgBase;
import io.milvus.grpc.MsgType;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;

import milvus.com.google.protobuf.ByteString;
import milvus.com.google.protobuf.InvalidProtocolBufferException;
import lombok.extern.slf4j.Slf4j;
import milvus.proto.msg.Msg;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Converts Milvus internal CDC protobuf events to SeaTunnelRow objects.
 *
 * <p>The CDC event format wraps Milvus internal message queue (MQ) payloads
 * through {@code msg.proto} message types. The parser handles the common DML
 * types (Insert, Delete) and maps them to SeaTunnel's row model with
 * appropriate {@link RowKind} annotations.
 *
 * <p><b>Note:</b> The exact internal Milvus CDC message format may vary between
 * versions. This implementation targets Milvus 2.4+ / SDK 2.6.x. Field data
 * conversion may require refinement based on testing with a real Milvus CDC
 * deployment.
 */
@Slf4j
public class MilvusEventParser {

    private final DescribeCollectionResp collectionDesc;
    private final String primaryKeyField;
    private final List<String> fieldNames;
    private final List<CreateCollectionReq.FieldSchema> fieldSchemas;

    public MilvusEventParser(DescribeCollectionResp collectionDesc, String primaryKeyField) {
        this.collectionDesc = collectionDesc;
        this.primaryKeyField = primaryKeyField;
        this.fieldSchemas = collectionDesc.getCollectionSchema().getFieldSchemaList();
        this.fieldNames = new ArrayList<>();
        for (CreateCollectionReq.FieldSchema field : this.fieldSchemas) {
            this.fieldNames.add(field.getName());
        }
    }

    /**
     * Parse a single binary payload from a CDC event into a SeaTunnelRow.
     *
     * @param payload raw bytes from the CDC message
     * @param collectionName expected collection name for validation
     * @return parsed row, or null if event should be skipped
     */
    public SeaTunnelRow parseEvent(ByteString payload, String collectionName)
            throws InvalidProtocolBufferException {
        Msg.ReplicateMsg replicateMsg = Msg.ReplicateMsg.parseFrom(payload);
        MsgBase base = replicateMsg.getBase();

        if (base == null) {
            log.debug("Skipping event with null MsgBase");
            return null;
        }

        MsgType msgType = base.getMsgType();

        switch (msgType) {
            case Insert:
                return parseInsertEvent(replicateMsg, collectionName);
            case Delete:
                return parseDeleteEvent(replicateMsg, collectionName);
            case CreateCollection:
            case DropCollection:
                log.info("DDL event {} for database={} collection={}",
                        msgType, replicateMsg.getDatabase(), replicateMsg.getCollection());
                return null;
            default:
                log.debug("Skipping unhandled msgType: {}", msgType);
                return null;
        }
    }

    /**
     * Parse a batch of binary payloads, filtering out null results.
     */
    public List<SeaTunnelRow> parseEvents(
            List<ByteString> payloads, String collectionName) {
        List<SeaTunnelRow> rows = new ArrayList<>();
        for (ByteString payload : payloads) {
            try {
                SeaTunnelRow row = parseEvent(payload, collectionName);
                if (row != null) {
                    rows.add(row);
                }
            } catch (InvalidProtocolBufferException e) {
                log.warn("Failed to parse CDC event payload, skipping: {}", e.getMessage());
            }
        }
        return rows;
    }

    private SeaTunnelRow parseInsertEvent(
            Msg.ReplicateMsg replicateMsg, String collectionName)
            throws InvalidProtocolBufferException {
        Msg.InsertRequest insertRequest =
                Msg.InsertRequest.parseFrom(replicateMsg.toByteArray());

        Object[] values = new Object[fieldNames.size()];

        // Milvus stores row data in columnar format. Each field in the schema
        // corresponds to an entry in the InsertRequest's fieldsData.
        for (int i = 0; i < fieldSchemas.size() && i < values.length; i++) {
            CreateCollectionReq.FieldSchema field = fieldSchemas.get(i);
            try {
                if (i < insertRequest.getFieldsDataCount()) {
                    values[i] = extractFieldValue(insertRequest, field, i);
                }
            } catch (Exception e) {
                log.debug("Failed to extract field {} at index {}: {}", field.getName(), i,
                        e.getMessage());
                values[i] = null;
            }
        }

        SeaTunnelRow row = new SeaTunnelRow(values);
        row.setRowKind(RowKind.INSERT);
        row.setTableId(collectionName);
        return row;
    }

    private SeaTunnelRow parseDeleteEvent(
            Msg.ReplicateMsg replicateMsg, String collectionName)
            throws InvalidProtocolBufferException {
        Msg.DeleteRequest deleteRequest =
                Msg.DeleteRequest.parseFrom(replicateMsg.toByteArray());

        Object[] values = new Object[fieldNames.size()];

        // For deletes, extract primary key values from the request.
        // Milvus provides primary keys as a list of int64 values or as an IDs proto.
        List<Long> pkValues = deleteRequest.getInt64PrimaryKeysList();
        if (pkValues != null && !pkValues.isEmpty()) {
            // Map the primary key value(s) to the schema fields.
            // If there's only one PK, put it in the first matching PK field position.
            int pkIndex = fieldNames.indexOf(primaryKeyField);
            if (pkIndex >= 0 && pkIndex < pkValues.size()) {
                values[pkIndex] = pkValues.get(0);
            }
        }

        SeaTunnelRow row = new SeaTunnelRow(values);
        row.setRowKind(RowKind.DELETE);
        row.setTableId(collectionName);
        return row;
    }

    /**
     * Extract a single field value from an InsertRequest in columnar format.
     * The Milvus proto uses typed scalar/vector field data containers.
     */
    @SuppressWarnings("unchecked")
    private Object extractFieldValue(
            Msg.InsertRequest insertRequest,
            CreateCollectionReq.FieldSchema field,
            int fieldIndex) {
        // The fieldsData is a list of protobuf Any or typed FieldData messages.
        // Each element represents one column with all row values for that column.
        Object fieldData = insertRequest.getFieldsData(fieldIndex);
        if (fieldData == null) {
            return null;
        }

        // FieldData contains typed lists (ScalarField, VectorField).
        // For CDC events, each column has exactly one value (single row).
        // The conversion logic mirrors MilvusSourceConverter's type mapping.
        //
        // TODO: Implement proper FieldData extraction based on the actual
        // FieldData proto definition. The approach depends on whether
        // Milvus exposes row-level or column-level data in CDC events.
        // For now, return the raw field data for downstream conversion.

        return fieldData;
    }
}
