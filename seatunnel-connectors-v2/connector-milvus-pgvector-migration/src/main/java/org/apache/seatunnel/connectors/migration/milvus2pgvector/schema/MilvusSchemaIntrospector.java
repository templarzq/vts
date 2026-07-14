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

package org.apache.seatunnel.connectors.migration.milvus2pgvector.schema;

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.index.request.DescribeIndexReq;
import io.milvus.v2.service.index.request.ListIndexesReq;
import io.milvus.v2.service.index.response.DescribeIndexResp;
import io.milvus.v2.service.partition.request.ListPartitionsReq;

import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.exception.MigrationErrorCode;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.exception.MigrationException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Reads a Milvus collection's full schema (fields + indexes with build params) into a {@link
 * MigrationSchema} snapshot. Unlike {@code MilvusSourceConnectorUtils}, this captures index build
 * parameters (M, efConstruction, nlist) which are needed to recreate equivalent pgvector indexes.
 */
@Slf4j
public class MilvusSchemaIntrospector implements AutoCloseable {

    private final MilvusClientV2 client;

    public MilvusSchemaIntrospector(String url, String token) {
        try {
            ConnectConfig config =
                    (token != null && !token.isEmpty())
                            ? ConnectConfig.builder().uri(url).token(token).build()
                            : ConnectConfig.builder().uri(url).build();
            this.client = new MilvusClientV2(config);
        } catch (Exception e) {
            throw new MigrationException(
                    MigrationErrorCode.SCHEMA_INTROSPECTION_FAILED,
                    "Failed to connect to Milvus at " + url,
                    e);
        }
    }

    public MigrationSchema introspect(String collectionName) {
        try {
            DescribeCollectionResp resp =
                    client.describeCollection(
                            DescribeCollectionReq.builder()
                                    .collectionName(collectionName)
                                    .build());
            CreateCollectionReq.CollectionSchema schema = resp.getCollectionSchema();

            List<MigrationSchema.ColumnDef> columns = new ArrayList<>();
            String pkName = null;
            boolean autoId = resp.getAutoID();
            for (CreateCollectionReq.FieldSchema field : schema.getFieldSchemaList()) {
                MigrationSchema.ColumnDef.ColumnDefBuilder b =
                        MigrationSchema.ColumnDef.builder()
                                .name(field.getName())
                                .dataType(field.getDataType())
                                .elementType(field.getElementType())
                                .dimension(field.getDimension())
                                .maxLength(
                                        field.getMaxLength() != null
                                                ? field.getMaxLength().longValue()
                                                : null)
                                .nullable(Boolean.TRUE.equals(field.getIsNullable()))
                                .isPrimaryKey(field.getIsPrimaryKey())
                                .isPartitionKey(field.getIsPartitionKey())
                                .comment(field.getDescription());
                columns.add(b.build());
                if (field.getIsPrimaryKey()) {
                    pkName = field.getName();
                    if (field.getAutoID() != null) {
                        autoId = field.getAutoID();
                    }
                }
            }

            List<MigrationSchema.IndexDef> indexes = collectIndexes(collectionName);

            // Partition info for PG partition table support
            List<String> partitionNames = listPartitions(collectionName);
            Integer shardsNum = resp.getShardsNum();

            return MigrationSchema.builder()
                    .collectionName(collectionName)
                    .collectionDescription(resp.getDescription())
                    .primaryKeyName(pkName)
                    .autoId(autoId)
                    .columns(columns)
                    .indexes(indexes)
                    .shardsNum(shardsNum != null ? shardsNum : 1)
                    .partitionNames(partitionNames)
                    .build();
        } catch (MigrationException e) {
            throw e;
        } catch (Exception e) {
            throw new MigrationException(
                    MigrationErrorCode.SCHEMA_INTROSPECTION_FAILED,
                    "Failed to describe Milvus collection: " + collectionName,
                    e);
        }
    }

    private List<MigrationSchema.IndexDef> collectIndexes(String collectionName) {
        List<MigrationSchema.IndexDef> out = new ArrayList<>();
        try {
            List<String> indexNames =
                    client.listIndexes(
                            ListIndexesReq.builder().collectionName(collectionName).build());
            for (String indexName : indexNames) {
                DescribeIndexResp resp =
                        client.describeIndex(
                                DescribeIndexReq.builder()
                                        .collectionName(collectionName)
                                        .indexName(indexName)
                                        .build());
                for (DescribeIndexResp.IndexDesc desc : resp.getIndexDescriptions()) {
                    out.add(
                            MigrationSchema.IndexDef.builder()
                                    .fieldName(desc.getFieldName())
                                    .indexName(desc.getIndexName())
                                    .indexType(desc.getIndexType())
                                    .metricType(desc.getMetricType())
                                    .extraParams(convertExtraParams(desc.getExtraParams()))
                                    .build());
                }
            }
        } catch (Exception e) {
            log.warn("Failed to list/describe indexes for {}: {}", collectionName, e.getMessage());
        }
        return out;
    }

    /**
     * List all partition names for the given collection.
     */
    private List<String> listPartitions(String collectionName) {
        try {
            return client.listPartitions(
                    ListPartitionsReq.builder().collectionName(collectionName).build());
        } catch (Exception e) {
            log.warn("Failed to list partitions for {}: {}", collectionName, e.getMessage());
            return Collections.emptyList();
        }
    }

    @Override
    public void close() {
        try {
            client.close();
        } catch (Exception e) {
            log.warn("Failed to close Milvus client", e);
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> convertExtraParams(Map<String, ?> raw) {
        if (raw == null) {
            return null;
        }
        Map<String, Object> result = new java.util.HashMap<>();
        for (Map.Entry<String, ?> entry : raw.entrySet()) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }
}
