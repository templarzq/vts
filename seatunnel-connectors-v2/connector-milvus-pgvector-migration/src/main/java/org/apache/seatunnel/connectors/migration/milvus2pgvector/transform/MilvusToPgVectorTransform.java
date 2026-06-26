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

package org.apache.seatunnel.connectors.migration.milvus2pgvector.transform;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.exception.MigrationErrorCode;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.exception.MigrationException;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportMapTransform;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MilvusToPgVectorTransform extends AbstractCatalogSupportMapTransform {

    public static final String PLUGIN_NAME = "MilvusToPgVector";

    private final MilvusToPgVectorTransformConfig config;

    public MilvusToPgVectorTransform(
            @NonNull MilvusToPgVectorTransformConfig config,
            @NonNull CatalogTable catalogTable) {
        super(catalogTable);
        this.config = config;
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        return inputRow;
    }

    @Override
    protected TableSchema transformTableSchema() {
        TableSchema original = inputCatalogTable.getTableSchema();
        if (!config.isAllowPrecisionLoss()) {
            checkNoBFloat16(original);
        }
        return original;
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        String tableName =
                config.getPgTable() != null && !config.getPgTable().isEmpty()
                        ? config.getPgTable()
                        : inputCatalogTable.getTableId().getTableName();
        return TableIdentifier.of(
                "pgvector",
                inputCatalogTable.getTableId().getDatabaseName(),
                config.getPgSchema(),
                tableName);
    }

    private void checkNoBFloat16(TableSchema schema) {
        if (schema == null || schema.getColumns() == null) {
            return;
        }
        for (Column col : schema.getColumns()) {
            SeaTunnelDataType<?> type = col.getDataType();
            if (type instanceof VectorType) {
                if (type.getSqlType()
                        == VectorType.VECTOR_BFLOAT16_TYPE.getSqlType()) {
                    throw new MigrationException(
                            MigrationErrorCode.PRECISION_LOSS_NOT_ALLOWED,
                            "Column "
                                    + col.getName()
                                    + " is BFloat16Vector; set allow_precision_loss=true to permit"
                                    + " halfvec conversion");
                }
            }
        }
    }
}
