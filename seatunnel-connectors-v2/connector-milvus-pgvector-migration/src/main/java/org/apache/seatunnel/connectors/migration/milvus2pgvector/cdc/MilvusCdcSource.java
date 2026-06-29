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

import java.util.ArrayList;
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
        MilvusSourceConnectorUtils utils = new MilvusSourceConnectorUtils(config);
        this.sourceTables = utils.getTables();
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.UNBOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return new ArrayList<>(sourceTables.values());
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
