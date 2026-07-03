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

import org.apache.seatunnel.api.source.SourceEvent;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

/**
 * Sent by the reader when the checkpointed CDC position is no longer valid
 * (e.g., WAL segment has been garbage collected after prolonged downtime).
 * The enumerator responds by resetting to INITIAL mode and re-generating
 * snapshot splits.
 */
@Getter
@ToString
@AllArgsConstructor
public class CheckpointInvalidatedEvent implements SourceEvent {
    private static final long serialVersionUID = 1L;

    /** The split that encountered the invalid position. */
    private final String splitId;
    /** Human-readable reason (e.g., "WAL segment GC'd"). */
    private final String reason;
}
