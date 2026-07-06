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

import java.util.ArrayList;
import java.util.List;

/**
 * Generates pgvector DDL (CREATE TABLE + CREATE INDEX) from a {@link MigrationSchema} snapshot.
 *
 * <p>The CREATE TABLE uses Milvus field names as column names (pgvector default schema). The
 * primary key becomes a PRIMARY KEY constraint. Index DDL is generated per vector index in the
 * source collection via {@link MilvusIndexConverter}.
 */
public final class PgVectorSchemaGenerator {

    private PgVectorSchemaGenerator() {}

    /** Generate the CREATE TABLE DDL with optional DROP TABLE prefix. */
    public static String generateCreateTableDdl(
            MigrationSchema schema, String pgSchema, String pgTable, boolean dropExisting) {
        StringBuilder sb = new StringBuilder();
        if (dropExisting) {
            sb.append("DROP TABLE IF EXISTS ").append(quoteQualified(pgSchema, pgTable)).append(";\n");
        }
        sb.append("CREATE TABLE IF NOT EXISTS ").append(quoteQualified(pgSchema, pgTable)).append(" (\n");
        List<String> columnDefs = new ArrayList<>();
        for (MigrationSchema.ColumnDef col : schema.getColumns()) {
            columnDefs.add("    " + quoteIdent(col.getName()) + " " + toPgType(col));
        }
        if (schema.getPrimaryKeyName() != null) {
            columnDefs.add("    PRIMARY KEY (" + quoteIdent(schema.getPrimaryKeyName()) + ")");
        }
        sb.append(String.join(",\n", columnDefs));
        sb.append("\n);");
        return sb.toString();
    }

    /**
     * Generate all CREATE INDEX DDL strings for the schema's vector indexes. Returns an empty list
     * if there are no convertible indexes.
     */
    public static List<String> generateCreateIndexDdls(
            MigrationSchema schema, String pgSchema, String pgTable) {
        List<String> ddls = new ArrayList<>();
        if (schema.getIndexes() == null) {
            return ddls;
        }
        for (MigrationSchema.IndexDef idx : schema.getIndexes()) {
            // Milvus indexes are always on a single field — use the field name as the pg column
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
