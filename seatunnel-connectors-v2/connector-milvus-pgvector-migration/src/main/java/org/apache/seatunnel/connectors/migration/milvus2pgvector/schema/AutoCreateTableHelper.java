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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Creates the target pgvector table from the Milvus collection schema if it
 * does not exist. Reads JDBC connection info from the sink section of the job
 * config — no duplicated connection params required in the source.
 */
@Slf4j
public final class AutoCreateTableHelper {

    private AutoCreateTableHelper() {}

    /**
     * Ensure the target table exists, creating it from the Milvus collection
     * schema if it does not. Reads {@code url}, {@code user}, {@code password},
     * and {@code table} (format: "schema.table") from the sink section of the
     * full job config.
     *
     * @param config         full job ReadonlyConfig (not just the source subtree)
     * @param collectionDesc Milvus collection description
     * @return true if the table existed or was created successfully
     */
    public static boolean ensureTable(ReadonlyConfig config,
                                       DescribeCollectionResp collectionDesc) {
        // Read explicit sink_jdbc_url from source config (avoids source/sink key conflict).
        // schema_save_mode only works in cluster mode, so we need this for local mode.
        String url = config.toMap().get("sink_jdbc_url");
        if (url == null || url.isEmpty()) {
            log.info("No sink_jdbc_url configured, skipping auto-create-table");
            return false;
        }

        String tableSource = config.toMap().get("table");
        String user = config.toMap().get("user");
        String password = config.toMap().get("password");

        String schema = "public";
        String table;
        if (tableSource != null && tableSource.contains(".")) {
            int dot = tableSource.indexOf('.');
            schema = tableSource.substring(0, dot);
            table = tableSource.substring(dot + 1);
        } else {
            table = tableSource != null ? tableSource : collectionDesc.getCollectionName();
        }

        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            if (tableExists(conn, schema, table)) {
                log.info("Target table \"{}\".\"{}\" already exists", schema, table);
                return true;
            }

            MigrationSchema migrationSchema = buildSchema(collectionDesc);
            String ddl = PgVectorSchemaGenerator.generateCreateTableDdl(
                    migrationSchema, schema, table, false);
            log.info("Auto-creating target table:\n{}", ddl);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(ddl);
            }
            log.info("Target table \"{}\".\"{}\" created successfully", schema, table);
            return true;
        } catch (Exception e) {
            log.warn("Auto-create-table failed for \"{}\".\"{}\": {}", schema, table, e.getMessage());
            return false;
        }
    }

    private static boolean tableExists(Connection conn, String schema, String table)
            throws Exception {
        try (Statement stmt = conn.createStatement()) {
            String sql = "SELECT EXISTS (SELECT FROM information_schema.tables "
                    + "WHERE table_schema = '" + schema.replace("'", "''")
                    + "' AND table_name = '" + table.replace("'", "''") + "')";
            ResultSet rs = stmt.executeQuery(sql);
            return rs.next() && rs.getBoolean(1);
        }
    }

    private static MigrationSchema buildSchema(DescribeCollectionResp resp) {
        CreateCollectionReq.CollectionSchema cs = resp.getCollectionSchema();
        List<MigrationSchema.ColumnDef> columns = new ArrayList<>();
        String pkName = null;

        for (CreateCollectionReq.FieldSchema field : cs.getFieldSchemaList()) {
            columns.add(MigrationSchema.ColumnDef.builder()
                    .name(field.getName())
                    .dataType(field.getDataType())
                    .elementType(field.getElementType())
                    .dimension(field.getDimension())
                    .maxLength(field.getMaxLength() != null
                            ? field.getMaxLength().longValue() : null)
                    .nullable(Boolean.TRUE.equals(field.getIsNullable()))
                    .isPrimaryKey(field.getIsPrimaryKey())
                    .isPartitionKey(field.getIsPartitionKey())
                    .build());
            if (field.getIsPrimaryKey()) {
                pkName = field.getName();
            }
        }

        return MigrationSchema.builder()
                .collectionName(resp.getCollectionName())
                .primaryKeyName(pkName)
                .columns(columns)
                .build();
    }
}
