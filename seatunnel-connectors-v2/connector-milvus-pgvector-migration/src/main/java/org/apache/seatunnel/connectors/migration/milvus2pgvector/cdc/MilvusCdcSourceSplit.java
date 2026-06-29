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

import org.apache.seatunnel.api.source.SourceSplit;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class MilvusCdcSourceSplit implements SourceSplit {

    private static final long serialVersionUID = 1L;

    /** Unique split ID. */
    private String splitId;

    /** Milvus collection to read from. */
    private String collectionName;

    /** Optional partition name (can be null). */
    private String partitionName;

    /** Starting primary key value (exclusive, for incremental splits). */
    private long startId;

    /** Ending primary key value (inclusive, -1 for no limit, Long.MAX_VALUE for streaming). */
    private long endId;

    /** True for snapshot phase, false for incremental phase. */
    private boolean snapshot;

    /** Offset for pagination within this split. */
    private long offset;

    /** Max records per query batch (-1 for no limit). */
    private long limit;

    /**
     * CDC start position for incremental splits. Null for snapshot splits, non-null
     * when resuming from a checkpoint in the incremental phase.
     */
    private ReplicatePosition startPosition;

    @Override
    public String splitId() {
        return splitId;
    }
}
