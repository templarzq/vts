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
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Sent from {@link MilvusCdcSourceReader} to {@link MilvusCdcSourceSplitEnumerator}
 * to report the current incremental consumption position. The enumerator aggregates
 * these positions for checkpointing, enabling the pipeline to resume from the last
 * acknowledged position after a failure.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class IncrementalPositionEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    /** ID of the incremental split. */
    private String splitId;

    /** The latest acknowledged CDC position for this split. */
    private ReplicatePosition position;
}
