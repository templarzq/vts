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
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.factory.ChangeStreamTableSourceCheckpoint;
import org.apache.seatunnel.api.table.factory.ChangeStreamTableSourceFactory;
import org.apache.seatunnel.api.table.factory.ChangeStreamTableSourceState;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;

import com.google.auto.service.AutoService;

import java.io.IOException;
import java.io.Serializable;

/** SPI factory for creating and restoring Milvus CDC sources from checkpoints. */
@AutoService(Factory.class)
public class MilvusCdcSourceFactory implements ChangeStreamTableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return MilvusCdcSourceConfig.CONNECTOR_IDENTITY;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        MilvusCdcSourceConfig.URL,
                        MilvusCdcSourceConfig.TOKEN)
                .optional(
                        MilvusCdcSourceConfig.COLLECTION,
                        MilvusCdcSourceConfig.COLLECTIONS,
                        MilvusCdcSourceConfig.DATABASE,
                        MilvusCdcSourceConfig.BATCH_SIZE,
                        MilvusCdcSourceConfig.INCREMENTAL_BATCH_SIZE,
                        MilvusCdcSourceConfig.POLL_INTERVAL_MS,
                        MilvusCdcSourceConfig.STARTUP_MODE,
                        MilvusCdcSourceConfig.CDC_STRATEGY,
                        MilvusCdcSourceConfig.PRIMARY_KEY_FIELD,
                        MilvusCdcSourceConfig.FETCH_ALL_FIELDS,
                        MilvusCdcSourceConfig.CHANNEL_TIMEOUT_MS,
                        MilvusCdcSourceConfig.CLIENT_PEM_PATH,
                        MilvusCdcSourceConfig.CLIENT_KEY_PATH,
                        MilvusCdcSourceConfig.CA_PEM_PATH,
                        MilvusCdcSourceConfig.SERVER_NAME,
                        MilvusCdcSourceConfig.PARALLELISM,
                        MilvusCdcSourceConfig.CDC_PCHANNEL,
                        MilvusCdcSourceConfig.CDC_ETCD_ENDPOINT,
                        MilvusCdcSourceConfig.CDC_SOURCE_CLUSTER_ID,
                        MilvusCdcSourceConfig.CDC_START_MESSAGE_ID,
                        MilvusCdcSourceConfig.STREAMING_NODE_ADDRESS,
                        MilvusCdcSourceConfig.CDC_USE_STREAMING_NODE)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return MilvusCdcSource.class;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            org.apache.seatunnel.api.table.connector.TableSource<T, SplitT, StateT>
            restoreSource(
                    TableSourceFactoryContext context,
                    ChangeStreamTableSourceState<StateT, SplitT> state) {
        return () -> (SeaTunnelSource<T, SplitT, StateT>)
                new MilvusCdcSource(context.getOptions(), (MilvusCdcSourceState) state.getEnumeratorState());
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            org.apache.seatunnel.api.table.connector.TableSource<T, SplitT, StateT>
            createSource(TableSourceFactoryContext context) {
        return () -> (SeaTunnelSource<T, SplitT, StateT>)
                new MilvusCdcSource(context.getOptions(), null);
    }

    @Override
    public <SplitT extends SourceSplit> Serializer<SplitT> getSplitSerializer() {
        return new DefaultSerializer<>();
    }

    @Override
    public <StateT extends Serializable> Serializer<StateT> getEnumeratorStateSerializer() {
        return new DefaultSerializer<>();
    }
}
