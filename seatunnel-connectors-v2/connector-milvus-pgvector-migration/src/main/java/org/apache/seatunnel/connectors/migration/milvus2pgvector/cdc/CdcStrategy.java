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

import java.io.Closeable;
import java.util.List;

/**
 * Pluggable strategy for acquiring CDC incremental changes from Milvus.
 * Implementations may use different mechanisms (gRPC ReplicateMessage, PK-based
 * polling, etc.) while exposing a uniform interface to the reader.
 */
public interface CdcStrategy extends Closeable {

    /**
     * Poll for incremental changes since the given position.
     *
     * @param split the incremental split being read
     * @param startPosition the position to start reading from (null means from the
     *        beginning / latest available, depending on startup mode)
     * @return list of rows with their CDC positions, empty if no new changes
     * @throws Exception on connection or deserialization errors
     */
    List<SeaTunnelRowWithPosition> pollChanges(
            MilvusCdcSourceSplit split, ReplicatePosition startPosition) throws Exception;

    /**
     * Check whether this strategy is usable (e.g., whether Milvus CDC is enabled on
     * the target cluster). Called once at initialization time.
     *
     * @return true if the strategy can be used, false to fall back
     */
    default boolean isAvailable() {
        return true;
    }
}
