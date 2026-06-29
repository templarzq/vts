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

/**
 * Checkpoint unit for CDC incremental reads. Records the position within a Milvus
 * replication stream so consumption can resume from the same point after failure.
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ReplicatePosition implements Serializable {

    private static final long serialVersionUID = 1L;

    /** The cluster ID of the source Milvus cluster producing these changes. */
    private String clusterId;

    /** Physical channel name (pchannel) from which messages are consumed. */
    private String pchannel;

    /** The last confirmed message ID (monotonically increasing within a pchannel). */
    private String messageId;

    /** The timeTick watermark, monotonically increasing globally. */
    private long timeTick;

    /** Epoch millis of when this position was recorded. */
    private long timestamp;
}
