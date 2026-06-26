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
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.exception.MigrationException;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class MilvusToPgVectorTransformTest {

    private static CatalogTable buildCatalogTable(Column... columns) {
        TableSchema schema = TableSchema.builder().columns(Arrays.asList(columns)).build();
        TableIdentifier id =
                TableIdentifier.of("milvus", "default", null, "my_collection");
        return CatalogTable.of(
                id, schema, Collections.emptyMap(), Collections.emptyList(), "test table");
    }

    private static MilvusToPgVectorTransformConfig config(String pgSchema, String pgTable, boolean allowLoss) {
        MilvusToPgVectorTransformConfig c = new MilvusToPgVectorTransformConfig();
        c.setPgSchema(pgSchema);
        c.setPgTable(pgTable);
        c.setAllowPrecisionLoss(allowLoss);
        return c;
    }

    @Test
    public void testTableIdentifierConversionWithExplicitTable() {
        CatalogTable input =
                buildCatalogTable(
                        PhysicalColumn.of(
                                "id", BasicType.LONG_TYPE, 0L, false, null, "pk"),
                        PhysicalColumn.of(
                                "embedding",
                                VectorType.VECTOR_FLOAT_TYPE,
                                4L,
                                false,
                                null,
                                "vector"));
        MilvusToPgVectorTransform transform =
                new MilvusToPgVectorTransform(
                        config("public", "target_table", true), input);

        CatalogTable produced = transform.getProducedCatalogTable();
        TableIdentifier outId = produced.getTableId();
        assertEquals("pgvector", outId.getCatalogName());
        assertEquals("default", outId.getDatabaseName());
        assertEquals("public", outId.getSchemaName());
        assertEquals("target_table", outId.getTableName());
    }

    @Test
    public void testTableIdentifierDefaultsToCollectionName() {
        CatalogTable input =
                buildCatalogTable(
                        PhysicalColumn.of(
                                "id", BasicType.LONG_TYPE, 0L, false, null, "pk"));
        MilvusToPgVectorTransform transform =
                new MilvusToPgVectorTransform(config("public", null, true), input);

        CatalogTable produced = transform.getProducedCatalogTable();
        assertEquals("my_collection", produced.getTableId().getTableName());
    }

    @Test
    public void testEmptyPgTableFallsBackToCollectionName() {
        CatalogTable input =
                buildCatalogTable(
                        PhysicalColumn.of(
                                "id", BasicType.LONG_TYPE, 0L, false, null, "pk"));
        MilvusToPgVectorTransform transform =
                new MilvusToPgVectorTransform(config("myschema", "", true), input);

        CatalogTable produced = transform.getProducedCatalogTable();
        assertEquals("myschema", produced.getTableId().getSchemaName());
        assertEquals("my_collection", produced.getTableId().getTableName());
    }

    @Test
    public void testRowPassthrough() {
        CatalogTable input =
                buildCatalogTable(
                        PhysicalColumn.of(
                                "id", BasicType.LONG_TYPE, 0L, false, null, "pk"),
                        PhysicalColumn.of(
                                "embedding",
                                VectorType.VECTOR_FLOAT_TYPE,
                                4L,
                                false,
                                null,
                                "vector"));
        MilvusToPgVectorTransform transform =
                new MilvusToPgVectorTransform(config("public", "t", true), input);
        // Force produced catalog table initialization (needed before map() is callable)
        transform.getProducedCatalogTable();

        SeaTunnelRow row =
                new SeaTunnelRow(
                        new Object[] {
                            42L, ByteBuffer.wrap(new byte[] {0, 0, -128, 63, 0, 0, 0, 64})
                        });
        SeaTunnelRow out = transform.map(row);
        assertSame(row, out, "transformRow must return the input row unchanged (passthrough)");
        assertEquals(42L, out.getField(0));
    }

    @Test
    public void testBFloat16ThrowsWhenPrecisionLossNotAllowed() {
        CatalogTable input =
                buildCatalogTable(
                        PhysicalColumn.of(
                                "id", BasicType.LONG_TYPE, 0L, false, null, "pk"),
                        PhysicalColumn.of(
                                "embedding",
                                VectorType.VECTOR_BFLOAT16_TYPE,
                                4L,
                                false,
                                null,
                                "bfloat16 vector"));
        MilvusToPgVectorTransform transform =
                new MilvusToPgVectorTransform(config("public", "t", false), input);
        assertThrows(
                MigrationException.class,
                transform::getProducedCatalogTable,
                "BFloat16 column with allowPrecisionLoss=false should throw");
    }

    @Test
    public void testBFloat16AllowedWhenPrecisionLossPermitted() {
        CatalogTable input =
                buildCatalogTable(
                        PhysicalColumn.of(
                                "id", BasicType.LONG_TYPE, 0L, false, null, "pk"),
                        PhysicalColumn.of(
                                "embedding",
                                VectorType.VECTOR_BFLOAT16_TYPE,
                                4L,
                                false,
                                null,
                                "bfloat16 vector"));
        MilvusToPgVectorTransform transform =
                new MilvusToPgVectorTransform(config("public", "t", true), input);
        // Should not throw
        CatalogTable produced = transform.getProducedCatalogTable();
        assertEquals("t", produced.getTableId().getTableName());
    }

    @Test
    public void testFloat16AllowedRegardlessOfPrecisionLossFlag() {
        CatalogTable input =
                buildCatalogTable(
                        PhysicalColumn.of(
                                "id", BasicType.LONG_TYPE, 0L, false, null, "pk"),
                        PhysicalColumn.of(
                                "embedding",
                                VectorType.VECTOR_FLOAT16_TYPE,
                                4L,
                                false,
                                null,
                                "float16 vector"));
        MilvusToPgVectorTransform transform =
                new MilvusToPgVectorTransform(config("public", "t", false), input);
        // Float16 → halfvec is lossless, so allowPrecisionLoss=false should NOT throw
        CatalogTable produced = transform.getProducedCatalogTable();
        assertEquals("t", produced.getTableId().getTableName());
    }

    @Test
    public void testSchemaPreservedThroughTransform() {
        CatalogTable input =
                buildCatalogTable(
                        PhysicalColumn.of(
                                "id", BasicType.LONG_TYPE, 0L, false, null, "pk"),
                        PhysicalColumn.of(
                                "embedding",
                                VectorType.VECTOR_FLOAT_TYPE,
                                4L,
                                false,
                                null,
                                "vector"),
                        PhysicalColumn.of(
                                "meta", BasicType.STRING_TYPE, 0L, true, null, "metadata"));
        MilvusToPgVectorTransform transform =
                new MilvusToPgVectorTransform(config("public", "t", true), input);

        CatalogTable produced = transform.getProducedCatalogTable();
        // Schema should be preserved (same columns, same types)
        assertEquals(
                input.getTableSchema().getColumns().size(),
                produced.getTableSchema().getColumns().size());
        for (int i = 0; i < input.getTableSchema().getColumns().size(); i++) {
            Column in = input.getTableSchema().getColumns().get(i);
            Column out = produced.getTableSchema().getColumns().get(i);
            assertEquals(in.getName(), out.getName());
            assertEquals(in.getDataType(), out.getDataType());
        }
    }
}
