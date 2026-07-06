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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link PollingIncrementalCdcStrategy} and {@link ReplicatePosition}
 * — verifies VARCHAR/non-numeric primary key position handling.
 */
public class PollingIncrementalCdcStrategyTest {

    @Test
    void testStringPrimaryKeyPositionPropagation() {
        // Verify that string PK positions are correctly stored in messageId
        String stringPk = "user-abc-123";
        ReplicatePosition pos = ReplicatePosition.builder()
                .messageId(stringPk)
                .timeTick(0)
                .timestamp(System.currentTimeMillis())
                .build();

        assertEquals(stringPk, pos.getMessageId());
        assertEquals(0, pos.getTimeTick());
    }

    @Test
    void testNumericPrimaryKeyPosition() {
        ReplicatePosition numPos = ReplicatePosition.builder()
                .timeTick(9999)
                .timestamp(System.currentTimeMillis())
                .build();

        assertEquals(9999, numPos.getTimeTick());
        assertNull(numPos.getMessageId(),
                "Numeric PK positions should have null messageId");
    }

    @Test
    void testMixedPositionBothFields() {
        // Hybrid position: both numeric watermark and string messageId
        ReplicatePosition pos = ReplicatePosition.builder()
                .messageId("uuid-last-seen")
                .timeTick(42)
                .timestamp(System.currentTimeMillis())
                .build();

        assertEquals("uuid-last-seen", pos.getMessageId());
        assertEquals(42, pos.getTimeTick());
    }

    @Test
    void testPositionWithNullMessageId() {
        ReplicatePosition pos = ReplicatePosition.builder()
                .timeTick(100)
                .build();

        assertNull(pos.getMessageId());
        assertEquals(100, pos.getTimeTick());
    }

    @Test
    void testMilvusCdcSourceConfigWithVarcharPrimaryKey() {
        MilvusCdcSourceConfig config = MilvusCdcSourceConfig.builder()
                .url("http://localhost:19530")
                .token("")
                .database("default")
                .collection("test_collection")
                .batchSize(100)
                .incrementalBatchSize(500L)
                .primaryKeyField("uuid")
                .startupMode("INITIAL")
                .cdcStrategy("polling_incremental")
                .parallelism(1)
                .build();

        assertEquals("uuid", config.getPrimaryKeyField());
        assertEquals("polling_incremental", config.getCdcStrategy());
    }

    @Test
    void testSplitStartIdPreservation() {
        MilvusCdcSourceSplit split = MilvusCdcSourceSplit.builder()
                .splitId("test-inc-0")
                .collectionName("test_collection")
                .snapshot(false)
                .startId(42)
                .endId(Long.MAX_VALUE)
                .build();

        assertEquals(42, split.getStartId());
        assertEquals(Long.MAX_VALUE, split.getEndId());
        assertFalse(split.isSnapshot());
    }
}
