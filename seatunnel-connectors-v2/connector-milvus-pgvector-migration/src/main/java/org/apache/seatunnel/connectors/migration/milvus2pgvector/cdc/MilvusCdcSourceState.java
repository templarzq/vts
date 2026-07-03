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

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class MilvusCdcSourceState implements Serializable {

    private static final long serialVersionUID = 1L;

    /** The maximum primary key value seen so far (watermark for incremental reads). */
    private long lastSeenId;

    /** Whether the initial snapshot phase is complete. */
    private boolean snapshotCompleted;

    /** Splits not yet processed. */
    private List<MilvusCdcSourceSplit> pendingSplits;

    /** Timestamp of last incremental poll (epoch millis). */
    private long lastPollTime;

    /** Per-split incremental positions. Key = splitId. */
    private Map<String, ReplicatePosition> splitPositions;

    /** Global high watermark (maximum timeTick across all incremental splits). */
    private long globalTimeTick;

    /**
     * WAL position captured before snapshot begins. Used as the start point for
     * the incremental phase to avoid full WAL replay via {@code DeliverPolicy.all}.
     * Per-collection mapping: key = collectionName.
     */
    private Map<String, ReplicatePosition> snapStartPositions;
}
