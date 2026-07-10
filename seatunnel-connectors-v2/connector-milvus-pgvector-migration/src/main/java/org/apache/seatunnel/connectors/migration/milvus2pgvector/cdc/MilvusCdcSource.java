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

import com.google.auto.service.AutoService;
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
        // If the collection has been dropped (e.g. during CDC runtime), the
        // describeCollection call will fail. We throw a clear exception so
        // the user understands the situation instead of seeing a raw stack
        // trace. Data already synced to the sink remains intact.
        Map<TablePath, CatalogTable> tables;
        try {
            if (cdcConfig.isSyncAllCollections()) {
                tables = discoverAllCollections();
            } else {
                MilvusSourceConnectorUtils utils = new MilvusSourceConnectorUtils(config);
                tables = utils.getTables();
            }
        } catch (Exception e) {
            String msg = e.getMessage() == null ? "" : e.getMessage().toLowerCase();
            if (msg.contains("can't find collection") || msg.contains("not found")
                    || msg.contains("doesn't exist") || msg.contains("not exist")) {
                log.warn("Collection not found (possibly dropped). "
                        + "Data already synced to sink remains intact. {}", e.getMessage());
                throw new RuntimeException(
                        "CDC source cannot start: collection '"
                                + cdcConfig.getCollection()
                                + "' not found in Milvus (possibly dropped). "
                                + "Data already synced to the sink remains intact. "
                                + "If the collection was dropped intentionally, "
                                + "no further action is needed.", e);
            }
            throw e;
        }
        this.sourceTables = tables;
    }

    private Map<TablePath, CatalogTable> discoverAllCollections() {
        // Remove "collections" key from config so MilvusSourceConnectorUtils
        // falls into its "list all collections" branch (empty list check).
        // The "*" wildcard otherwise causes it to describe collection named "*".
        Map<String, Object> configMap = new HashMap<>();
        config.toMap().forEach((k, v) -> configMap.put(k, v));
        configMap.remove("collections");
        ReadonlyConfig cleanedConfig = ReadonlyConfig.fromMap(configMap);
        MilvusSourceConnectorUtils utils = new MilvusSourceConnectorUtils(cleanedConfig);
        Map<TablePath, CatalogTable> allTables = utils.getTables();
        log.info("Wildcard sync: discovered {} collections", allTables.size());
        return allTables;
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
