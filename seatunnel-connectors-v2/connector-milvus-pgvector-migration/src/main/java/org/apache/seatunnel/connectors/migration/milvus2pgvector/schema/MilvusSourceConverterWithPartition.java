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

package org.apache.seatunnel.connectors.migration.milvus2pgvector.schema;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.milvus.source.utils.MilvusSourceConverter;

import io.milvus.response.QueryResultsWrapper;

import java.util.List;

/**
 * Decorator over {@link MilvusSourceConverter} that fills the {@code __partition_name} column
 * value in the converted {@link SeaTunnelRow}.
 *
 * <p>When PG partition mode is enabled, the source table schema includes a {@code __partition_name}
 * column. This decorator fills that column with the partition name from the Milvus record so that
 * JDBC sink can route the row to the correct PG partition.
 *
 * <p>This class lives in the migration module to avoid intrusive changes to the shared
 * {@code connector-milvus} module.
 */
public class MilvusSourceConverterWithPartition extends MilvusSourceConverter {

    public MilvusSourceConverterWithPartition(TableSchema tableSchema) {
        super(tableSchema);
    }

    @Override
    public SeaTunnelRow convertToSeaTunnelRow(
            QueryResultsWrapper.RowRecord record, TableSchema tableSchema,
            String collectionName, String partitionName) {
        SeaTunnelRow row = super.convertToSeaTunnelRow(
                record, tableSchema, collectionName, partitionName);
        fillPartitionColumn(row, partitionName, tableSchema);
        return row;
    }

    /**
     * Set the {@code __partition_name} field in the row to the given partition name.
     * The base converter has already allocated the field (from the schema) with a null value;
     * we override it here.
     */
    private void fillPartitionColumn(SeaTunnelRow row, String partitionName,
                                      TableSchema tableSchema) {
        int idx = findColumnIndex(tableSchema, PgVectorSchemaGenerator.PARTITION_COLUMN_NAME);
        if (idx >= 0) {
            row.setField(idx, partitionName != null ? partitionName : "_default");
        }
        row.setPartitionName(partitionName);
    }

    /**
     * Find the column index by name in the given table schema.
     */
    private static int findColumnIndex(TableSchema tableSchema, String columnName) {
        List<Column> columns = tableSchema.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equals(columnName)) {
                return i;
            }
        }
        return -1;
    }
}
