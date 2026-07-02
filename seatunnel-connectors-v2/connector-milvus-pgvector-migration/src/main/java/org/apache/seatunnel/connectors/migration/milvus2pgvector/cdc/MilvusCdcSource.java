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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.milvus.source.utils.MilvusSourceConnectorUtils;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.utils.MilvusConnectorUtils;

import com.google.auto.service.AutoService;
import io.milvus.v2.client.MilvusClientV2;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A SeaTunnel source that provides CDC (Change Data Capture) capabilities for Milvus
 * collections. This source operates in {@link Boundedness#UNBOUNDED} mode, supporting
 * both an initial snapshot phase and a continuous incremental streaming phase.
 *
 * <p>Implements {@link ChangeStreamTableSourceFactory} integration through
 * {@link MilvusCdcSourceFactory}, which handles checkpoint-based restore via
 * {@link MilvusCdcSourceState}.
 */
@Slf4j
@AutoService(SeaTunnelSource.class)
public class MilvusCdcSource
        implements SeaTunnelSource<SeaTunnelRow, MilvusCdcSourceSplit, MilvusCdcSourceState>,
                SupportParallelism,
                SupportColumnProjection {

    private final ReadonlyConfig config;
    private final MilvusCdcSourceConfig cdcConfig;
    private final MilvusCdcSourceState checkpointState;
    private final Map<TablePath, CatalogTable> sourceTables;

    /**
     * Create a new CDC source. If {@code checkpointState} is non-null, the source will
     * resume from the saved state (e.g., skip snapshot if already completed, resume
     * incremental from the last checkpointed position).
     */
    public MilvusCdcSource(ReadonlyConfig config, MilvusCdcSourceState checkpointState) {
        this.config = config;
        this.cdcConfig = MilvusCdcSourceConfig.of(config);
        this.checkpointState = checkpointState;

        // Build sourceTables. When collections=["*"], list all collections
        // from Milvus and pass those names to the utility, because the utility
        // treats "*" as a literal collection name.
        if (cdcConfig.isSyncAllCollections()) {
            this.sourceTables = discoverAllCollections();
        } else {
            MilvusSourceConnectorUtils utils = new MilvusSourceConnectorUtils(config);
            this.sourceTables = utils.getTables();
        }
    }

    private Map<TablePath, CatalogTable> discoverAllCollections() {
        Map<TablePath, CatalogTable> tables = new HashMap<>();
        try (MilvusClientV2 client = new MilvusClientV2(
                MilvusConnectorUtils.getConnectConfig(config))) {
            List<String> allCollections = client.listCollections().getCollectionNames();
            log.info("Wildcard sync: discovered {} collections", allCollections.size());

            // Build a config with the actual collection list for each collection
            // so the utility doesn't try to describe collection named "*"
            for (String col : allCollections) {
                try {
                    Map<String, Object> colConfig = new HashMap<>();
                    colConfig.put("url", config.get(
                            org.apache.seatunnel.connectors.seatunnel.milvus.source.config.MilvusSourceConfig.URL));
                    colConfig.put("token", config.get(
                            org.apache.seatunnel.connectors.seatunnel.milvus.source.config.MilvusSourceConfig.TOKEN));
                    colConfig.put("database", cdcConfig.getDatabase());
                    colConfig.put("collections", java.util.Collections.singletonList(col));
                    MilvusSourceConnectorUtils utils =
                            new MilvusSourceConnectorUtils(ReadonlyConfig.fromMap(colConfig));
                    Map<TablePath, CatalogTable> result = utils.getTables();
                    tables.putAll(result);
                } catch (Exception e) {
                    log.warn("Failed to discover collection '{}': {}", col, e.getMessage());
                }
            }
        } catch (Exception e) {
            log.error("Failed to discover all collections: {}", e.getMessage());
        }
        return tables;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.UNBOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        // Produce catalog tables for all collections that should be synced.
        // When no specific collection is configured, sync all discovered collections.
        List<CatalogTable> result = new ArrayList<>();
        for (Map.Entry<TablePath, CatalogTable> entry : sourceTables.entrySet()) {
            String tableName = entry.getValue().getTableId().getTableName();
            if (cdcConfig.shouldSyncCollection(tableName)) {
                result.add(entry.getValue());
            }
        }
        return result;
    }

    @Override
    public SourceReader<SeaTunnelRow, MilvusCdcSourceSplit> createReader(
            SourceReader.Context readerContext) throws Exception {
        return new MilvusCdcSourceReader(readerContext, config, cdcConfig, sourceTables);
    }

    @Override
    public SourceSplitEnumerator<MilvusCdcSourceSplit, MilvusCdcSourceState> createEnumerator(
            SourceSplitEnumerator.Context<MilvusCdcSourceSplit> context) throws Exception {
        return new MilvusCdcSourceSplitEnumerator(context, config, cdcConfig, sourceTables,
                checkpointState);
    }

    @Override
    public SourceSplitEnumerator<MilvusCdcSourceSplit, MilvusCdcSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<MilvusCdcSourceSplit> context,
            MilvusCdcSourceState state) throws Exception {
        return new MilvusCdcSourceSplitEnumerator(context, config, cdcConfig, sourceTables, state);
    }

    @Override
    public String getPluginName() {
        return MilvusCdcSourceConfig.CONNECTOR_IDENTITY;
    }
}
