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

import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.connectors.seatunnel.milvus.source.utils.MilvusSourceConverter;

import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.collection.request.GetLoadStateReq;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link PollingIncrementalCdcStrategy} focusing on VARCHAR/non-numeric
 * primary key handling and expression generation.
 */
@ExtendWith(MockitoExtension.class)
public class PollingIncrementalCdcStrategyTest {

    @Mock
    private MilvusClientV2 mockClient;

    @Mock
    private MilvusSourceConverter mockConverter;

    @Mock
    private TableSchema mockTableSchema;

    private MilvusCdcSourceConfig config;

    @BeforeEach
    void setUp() {
        config = MilvusCdcSourceConfig.builder()
                .url("http://localhost:19530")
                .token("")
                .database("default")
                .collection("test_collection")
                .batchSize(100)
                .incrementalBatchSize(500L)
                .pollIntervalMs(1000L)
                .startupMode("INITIAL")
                .cdcStrategy("polling_incremental")
                .primaryKeyField("uuid")  // VARCHAR PK
                .fetchAllFields(true)
                .channelTimeoutMs(30000L)
                .parallelism(1)
                .build();
    }

    @Test
    void testConstructorAcceptsMilvusClientV2() {
        // Verify the new constructor (with injected client) works
        PollingIncrementalCdcStrategy strategy = new PollingIncrementalCdcStrategy(
                config, mockConverter, mockTableSchema, mockClient);
        assertNotNull(strategy);
    }

    @Test
    void testPollChangesWithVarcharPrimaryKey() throws Exception {
        // Given: Collection is loaded, but no new data
        when(mockClient.getLoadState(any(GetLoadStateReq.class))).thenReturn(true);

        PollingIncrementalCdcStrategy strategy = new PollingIncrementalCdcStrategy(
                config, mockConverter, mockTableSchema, mockClient);

        MilvusCdcSourceSplit split = MilvusCdcSourceSplit.builder()
                .splitId("test-inc-0")
                .collectionName("test_collection")
                .snapshot(false)
                .startId(0)
                .endId(Long.MAX_VALUE)
                .build();

        // When: Poll with a string PK position (messageId set)
        ReplicatePosition startPos = ReplicatePosition.builder()
                .messageId("uuid-00005")
                .build();

        List<SeaTunnelRowWithPosition> results = strategy.pollChanges(split, startPos);

        // Then: Should return empty (no new data from mock), but not crash
        assertNotNull(results);
        assertTrue(results.isEmpty());

        strategy.close();
    }

    @Test
    void testPollChangesWithNumericPrimaryKey() throws Exception {
        config.setPrimaryKeyField("id");
        when(mockClient.getLoadState(any(GetLoadStateReq.class))).thenReturn(true);

        PollingIncrementalCdcStrategy strategy = new PollingIncrementalCdcStrategy(
                config, mockConverter, mockTableSchema, mockClient);

        MilvusCdcSourceSplit split = MilvusCdcSourceSplit.builder()
                .splitId("test-inc-0")
                .collectionName("test_collection")
                .snapshot(false)
                .startId(42)
                .endId(Long.MAX_VALUE)
                .build();

        ReplicatePosition startPos = ReplicatePosition.builder()
                .timeTick(42)
                .build();

        List<SeaTunnelRowWithPosition> results = strategy.pollChanges(split, startPos);

        assertNotNull(results);
        assertTrue(results.isEmpty());

        // startId should be preserved (no new data)
        assertEquals(42, split.getStartId());

        strategy.close();
    }

    @Test
    void testCloseDoesNotThrow() {
        PollingIncrementalCdcStrategy strategy = new PollingIncrementalCdcStrategy(
                config, mockConverter, mockTableSchema, mockClient);
        assertDoesNotThrow(strategy::close);
    }

    @Test
    void testPollChangesWithCollectionNotLoaded() throws Exception {
        when(mockClient.getLoadState(any(GetLoadStateReq.class))).thenReturn(false);

        PollingIncrementalCdcStrategy strategy = new PollingIncrementalCdcStrategy(
                config, mockConverter, mockTableSchema, mockClient);

        MilvusCdcSourceSplit split = MilvusCdcSourceSplit.builder()
                .splitId("test-inc-0")
                .collectionName("test_collection")
                .snapshot(false)
                .startId(0)
                .endId(Long.MAX_VALUE)
                .build();

        List<SeaTunnelRowWithPosition> results = strategy.pollChanges(split, null);

        // Should return empty when collection is not loaded
        assertNotNull(results);
        assertTrue(results.isEmpty());

        strategy.close();
    }

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

        // Numeric PK position
        ReplicatePosition numPos = ReplicatePosition.builder()
                .timeTick(9999)
                .timestamp(System.currentTimeMillis())
                .build();

        assertEquals(9999, numPos.getTimeTick());
        assertNull(numPos.getMessageId());
    }
}
