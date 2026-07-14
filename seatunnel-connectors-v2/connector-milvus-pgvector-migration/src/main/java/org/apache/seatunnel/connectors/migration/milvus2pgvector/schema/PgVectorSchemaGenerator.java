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

import io.milvus.v2.common.DataType;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/**
 * Generates pgvector DDL (CREATE TABLE + CREATE INDEX) from a {@link MigrationSchema} snapshot.
 *
 * <p>The CREATE TABLE uses Milvus field names as column names (pgvector default schema). The
 * primary key becomes a PRIMARY KEY constraint. Index DDL is generated per vector index in the
 * source collection via {@link MilvusIndexConverter}.
 *
 * <p>Supports two modes:
 * <ul>
 *   <li>Non-partitioned (default): creates a regular table, backward compatible</li>
 *   <li>Partitioned: creates a LIST + HASH two-level partitioned table based on Milvus
 *       partitions and shards</li>
 * </ul>
 */
@Slf4j
public final class PgVectorSchemaGenerator {

    private PgVectorSchemaGenerator() {}

    /** Name of the partition column added to the PG table for LIST partitioning. */
    public static final String PARTITION_COLUMN_NAME = "__partition_name";

    /** PG max identifier length (63 bytes per PG identifier convention). */
    private static final int PG_MAX_IDENT_LENGTH = 63;

    /**
     * Generate the CREATE TABLE DDL with optional DROP TABLE prefix (non-partitioned mode).
     * Delegates to the overloaded method with {@code enablePartition=false}.
     */
    public static String generateCreateTableDdl(
            MigrationSchema schema, String pgSchema, String pgTable, boolean dropExisting) {
        return generateCreateTableDdl(schema, pgSchema, pgTable, dropExisting, false);
    }

    /**
     * Generate the CREATE TABLE DDL with optional DROP TABLE prefix.
     *
     * <p>When {@code enablePartition=true} and the schema has partition info, generates a
     * two-level partitioned table (LIST on partition_name, HASH on primary key). Otherwise
     * falls back to non-partitioned DDL.
     */
    public static String generateCreateTableDdl(
            MigrationSchema schema, String pgSchema, String pgTable,
            boolean dropExisting, boolean enablePartition) {
        if (enablePartition
                && schema.getPartitionNames() != null
                && !schema.getPartitionNames().isEmpty()) {
            return generatePartitionedTableDdl(schema, pgSchema, pgTable, dropExisting);
        }
        return generateNonPartitionedTableDdl(schema, pgSchema, pgTable, dropExisting);
    }

    // ---- Non-partitioned DDL ----

    private static String generateNonPartitionedTableDdl(
            MigrationSchema schema, String pgSchema, String pgTable, boolean dropExisting) {
        StringBuilder sb = new StringBuilder();
        if (dropExisting) {
            sb.append("DROP TABLE IF EXISTS ").append(quoteQualified(pgSchema, pgTable)).append(";\n");
        }
        sb.append("CREATE TABLE IF NOT EXISTS ").append(quoteQualified(pgSchema, pgTable)).append(" (\n");
        List<String> columnDefs = buildColumnDefs(schema, false);
        sb.append(String.join(",\n", columnDefs));
        sb.append("\n);");
        return sb.toString();
    }

    // ---- Partitioned DDL ----

    private static String generatePartitionedTableDdl(
            MigrationSchema schema, String pgSchema, String pgTable, boolean dropExisting) {
        StringBuilder sb = new StringBuilder();
        if (dropExisting) {
            sb.append("DROP TABLE IF EXISTS ")
                    .append(quoteQualified(pgSchema, pgTable))
                    .append(" CASCADE;\n");
        }

        String pkName = schema.getPrimaryKeyName();
        String qualifiedMain = quoteQualified(pgSchema, pgTable);
        List<String> partitionNames = schema.getPartitionNames();
        int shardsNum = schema.getShardsNum() != null ? schema.getShardsNum() : 1;

        // 1. Main partitioned table
        sb.append("CREATE TABLE IF NOT EXISTS ").append(qualifiedMain).append(" (\n");
        List<String> columnDefs = buildColumnDefs(schema, true);
        if (pkName != null) {
            columnDefs.add("    PRIMARY KEY ("
                    + quoteIdent(pkName) + ", " + quoteIdent(PARTITION_COLUMN_NAME) + ")");
        }
        sb.append(String.join(",\n", columnDefs));
        sb.append("\n) PARTITION BY LIST (").append(quoteIdent(PARTITION_COLUMN_NAME)).append(");\n");

        // 2. LIST partitions
        boolean hasDefaultNamedPartition = false;
        for (String partitionName : partitionNames) {
            if ("default".equals(partitionName)) {
                hasDefaultNamedPartition = true;
                log.warn("Milvus partition named 'default' conflicts with PG DEFAULT partition. "
                        + "Renaming LIST partition table to '{}_default_list'.",
                        pgTable);
            }
            sb.append(generateListPartitionDdl(schema, pgSchema, pgTable, partitionName, shardsNum));
            sb.append('\n');
        }

        // 3. DEFAULT partition (catch-all)
        if (hasDefaultNamedPartition) {
            String defaultPartTable = safePartitionName(pgTable, "default_list");
            sb.append("CREATE TABLE IF NOT EXISTS ")
                    .append(quoteQualified(pgSchema, defaultPartTable))
                    .append(" PARTITION OF ").append(qualifiedMain)
                    .append(" DEFAULT;\n");
        } else {
            String defaultPartTable = pgTable + "_default";
            sb.append("CREATE TABLE IF NOT EXISTS ")
                    .append(quoteQualified(pgSchema, defaultPartTable))
                    .append(" PARTITION OF ").append(qualifiedMain)
                    .append(" DEFAULT;\n");
        }

        return sb.toString();
    }

    private static String generateListPartitionDdl(
            MigrationSchema schema, String pgSchema, String pgTable,
            String partitionName, int shardsNum) {
        String pkName = schema.getPrimaryKeyName();
        String qualifiedMain = quoteQualified(pgSchema, pgTable);
        String listPartTable = safePartitionName(pgTable, partitionName);
        String escapedPartValue = partitionName.replace("'", "''");

        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE IF NOT EXISTS ")
                .append(quoteQualified(pgSchema, listPartTable));

        if (shardsNum > 1 && pkName != null) {
            // LIST + HASH two-level
            sb.append(" PARTITION OF ").append(qualifiedMain)
                    .append(" FOR VALUES IN ('").append(escapedPartValue).append("')")
                    .append(" PARTITION BY HASH (").append(quoteIdent(pkName)).append(");\n");
            for (int i = 0; i < shardsNum; i++) {
                String hashPartTable = listPartTable + "_p" + i;
                if (hashPartTable.length() > PG_MAX_IDENT_LENGTH) {
                    hashPartTable = truncatePartitionName(hashPartTable);
                }
                sb.append("CREATE TABLE IF NOT EXISTS ")
                        .append(quoteQualified(pgSchema, hashPartTable))
                        .append(" PARTITION OF ").append(quoteQualified(pgSchema, listPartTable))
                        .append(" FOR VALUES WITH (MODULUS ").append(shardsNum)
                        .append(", REMAINDER ").append(i).append(");\n");
            }
        } else {
            // Only LIST partition (no HASH sub-partitioning)
            sb.append(" PARTITION OF ").append(qualifiedMain)
                    .append(" FOR VALUES IN ('").append(escapedPartValue).append("');\n");
        }

        return sb.toString();
    }

    // ---- Shared helpers ----

    private static List<String> buildColumnDefs(MigrationSchema schema, boolean addPartitionColumn) {
        List<String> columnDefs = new ArrayList<>();
        for (MigrationSchema.ColumnDef col : schema.getColumns()) {
            columnDefs.add("    " + quoteIdent(col.getName()) + " " + toPgType(col));
        }
        if (addPartitionColumn) {
            columnDefs.add("    " + quoteIdent(PARTITION_COLUMN_NAME)
                    + " VARCHAR(256) NOT NULL DEFAULT '_default'");
        }
        return columnDefs;
    }

    /**
     * Generate a safe PG partition table name from the base table name and partition name.
     * Truncates if the combined name exceeds PG's 63-byte identifier limit.
     */
    static String safePartitionName(String pgTable, String partitionName) {
        String raw = pgTable + "_" + partitionName;
        if (raw.getBytes(StandardCharsets.UTF_8).length <= PG_MAX_IDENT_LENGTH) {
            return raw;
        }
        return truncatePartitionName(raw);
    }

    private static String truncatePartitionName(String name) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest(name.getBytes(StandardCharsets.UTF_8));
            // 8-char hex suffix
            StringBuilder suffix = new StringBuilder(8);
            for (int i = 0; i < 4; i++) {
                suffix.append(String.format("%02x", hash[i] & 0xFF));
            }
            // Leave room for "_" + 8-char suffix
            int maxPrefixLen = PG_MAX_IDENT_LENGTH - 1 - suffix.length();
            String prefix = name.substring(0, Math.min(name.length(), maxPrefixLen));
            // Ensure we don't split multi-byte characters
            while (prefix.getBytes(StandardCharsets.UTF_8).length > maxPrefixLen) {
                prefix = prefix.substring(0, prefix.length() - 1);
            }
            return prefix + "_" + suffix;
        } catch (NoSuchAlgorithmException e) {
            // Fallback: just truncate to 63 bytes
            byte[] bytes = name.getBytes(StandardCharsets.UTF_8);
            return new String(bytes, 0, Math.min(bytes.length, PG_MAX_IDENT_LENGTH),
                    StandardCharsets.UTF_8);
        }
    }

    // ---- Existing public API (unchanged) ----

    public static List<String> generateCreateIndexDdls(
            MigrationSchema schema, String pgSchema, String pgTable) {
        List<String> ddls = new ArrayList<>();
        if (schema.getIndexes() == null) {
            return ddls;
        }
        for (MigrationSchema.IndexDef idx : schema.getIndexes()) {
            String ddl =
                    MilvusIndexConverter.convert(idx, pgTable, pgSchema, idx.getFieldName());
            if (ddl != null) {
                ddls.add(ddl);
            }
        }
        return ddls;
    }

    /** Map a single Milvus column definition to its pgvector column type string. */
    static String toPgType(MigrationSchema.ColumnDef col) {
        DataType type = col.getDataType();
        if (type == null) {
            throw new IllegalArgumentException("Column " + col.getName() + " has null dataType");
        }
        switch (type) {
            case VarChar:
                return TypeMapping.mapVarChar(col.getMaxLength());
            case Array:
                return TypeMapping.mapArray(col.getElementType());
            case FloatVector:
            case Float16Vector:
            case BFloat16Vector:
            case BinaryVector:
            case SparseFloatVector:
                return TypeMapping.mapVector(type, col.getDimension());
            case Bool:
            case Int8:
            case Int16:
            case Int32:
            case Int64:
            case Float:
            case Double:
            case String:
            case JSON:
            case Geometry:
            case Timestamptz:
                return TypeMapping.mapScalar(type);
            default:
                throw new IllegalArgumentException(
                        "Unsupported Milvus type for column " + col.getName() + ": " + type);
        }
    }

    static String quoteIdent(String ident) {
        return "\"" + ident.replace("\"", "\"\"") + "\"";
    }

    static String quoteQualified(String schema, String table) {
        if (schema == null || schema.isEmpty()) {
            return quoteIdent(table);
        }
        return quoteIdent(schema) + "." + quoteIdent(table);
    }

    /** Bundle returned by generators that produce multiple DDL strings. */
    @Data
    @Builder
    public static class DdlBundle {
        private String createTableDdl;
        private List<String> createIndexDdls;
    }
}
