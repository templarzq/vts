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

import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Schema snapshot of a Milvus collection, captured by {@link MilvusSchemaIntrospector} and consumed
 * by {@link PgVectorSchemaGenerator} + {@link MilvusIndexConverter}.
 */
@Data
@Builder
public class MigrationSchema implements Serializable {

    private static final long serialVersionUID = 1L;

    private String collectionName;
    private String collectionDescription;
    private String primaryKeyName;
    /** Whether the Milvus primary key is auto-generated. */
    private boolean autoId;
    private List<ColumnDef> columns;
    private List<IndexDef> indexes;

    /** Number of shards from Milvus collection (used as HASH modulus for sub-partitioning). */
    @Builder.Default
    private Integer shardsNum = 1;

    /** Ordered list of Milvus partition names for LIST partitioning. */
    @Builder.Default
    private List<String> partitionNames = Collections.emptyList();

    @Data
    @Builder
    public static class ColumnDef implements Serializable {
        private String name;
        private DataType dataType;
        /** For arrays. */
        private DataType elementType;
        private Integer dimension;
        private Long maxLength;
        private boolean nullable;
        private boolean isPrimaryKey;
        private boolean isPartitionKey;
        private String comment;
    }

    @Data
    @Builder
    public static class IndexDef implements Serializable {
        private String fieldName;
        private String indexName;
        private IndexParam.IndexType indexType;
        private IndexParam.MetricType metricType;
        /** Raw Milvus extra params (e.g. {"M":16,"efConstruction":500}). */
        private Map<String, Object> extraParams;
    }
}
