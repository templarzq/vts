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
 * when the snapshot phase has completed and the reader is transitioning to incremental
 * mode. The enumerator uses this to update its phase tracking for checkpoint purposes.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SnapshotCompletedEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    /** ID of the split that completed its snapshot. */
    private String splitId;
}
