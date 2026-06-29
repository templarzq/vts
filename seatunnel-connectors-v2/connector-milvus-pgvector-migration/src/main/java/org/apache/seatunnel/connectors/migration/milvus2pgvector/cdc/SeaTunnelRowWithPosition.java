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

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Wrapper that pairs a SeaTunnelRow with its corresponding CDC position. Used in
 * the incremental read phase to track the position of each row for checkpointing.
 */
@Data
@AllArgsConstructor
public class SeaTunnelRowWithPosition {

    /** The converted row data. */
    private SeaTunnelRow row;

    /** The CDC position at which this row was consumed. */
    private ReplicatePosition position;
}
