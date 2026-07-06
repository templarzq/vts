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

import org.apache.seatunnel.connectors.migration.milvus2pgvector.exception.MigrationException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TypeMappingTest {

    // ---- scalar types ----

    @Test
    public void testBoolMapping() {
        assertEquals("BOOLEAN", TypeMapping.mapScalar(DataType.Bool));
    }

    @Test
    public void testInt8AndInt16MapToSmallint() {
        assertEquals("SMALLINT", TypeMapping.mapScalar(DataType.Int8));
        assertEquals("SMALLINT", TypeMapping.mapScalar(DataType.Int16));
    }

    @Test
    public void testInt32Mapping() {
        assertEquals("INTEGER", TypeMapping.mapScalar(DataType.Int32));
    }

    @Test
    public void testInt64Mapping() {
        assertEquals("BIGINT", TypeMapping.mapScalar(DataType.Int64));
    }

    @Test
    public void testFloatMapping() {
        assertEquals("REAL", TypeMapping.mapScalar(DataType.Float));
    }

    @Test
    public void testDoubleMapping() {
        assertEquals("DOUBLE PRECISION", TypeMapping.mapScalar(DataType.Double));
    }

    @Test
    public void testStringMapping() {
        assertEquals("TEXT", TypeMapping.mapScalar(DataType.String));
    }

    @Test
    public void testJsonMapping() {
        assertEquals("JSONB", TypeMapping.mapScalar(DataType.JSON));
    }

    @Test
    public void testGeometryMapping() {
        assertEquals("BYTEA", TypeMapping.mapScalar(DataType.Geometry));
    }

    @Test
    public void testTimestamptzMapping() {
        assertEquals("TIMESTAMPTZ", TypeMapping.mapScalar(DataType.Timestamptz));
    }

    @Test
    public void testScalarThrowsForVectorType() {
        assertThrows(
                MigrationException.class, () -> TypeMapping.mapScalar(DataType.FloatVector));
    }

    // ---- VarChar ----

    @Test
    public void testVarCharWithExplicitLength() {
        assertEquals("VARCHAR(255)", TypeMapping.mapVarChar(255L));
    }

    @Test
    public void testVarCharWithNullLengthUsesDefault() {
        assertEquals(
                "VARCHAR(" + TypeMapping.DEFAULT_VARCHAR_LENGTH + ")",
                TypeMapping.mapVarChar(null));
    }

    @Test
    public void testVarCharWithZeroLengthUsesDefault() {
        assertEquals(
                "VARCHAR(" + TypeMapping.DEFAULT_VARCHAR_LENGTH + ")",
                TypeMapping.mapVarChar(0L));
    }

    @Test
    public void testVarCharWithNegativeLengthUsesDefault() {
        assertEquals(
                "VARCHAR(" + TypeMapping.DEFAULT_VARCHAR_LENGTH + ")",
                TypeMapping.mapVarChar(-1L));
    }

    // ---- vector types ----

    @Test
    public void testFloatVectorMapping() {
        assertEquals("vector(128)", TypeMapping.mapVector(DataType.FloatVector, 128));
    }

    @Test
    public void testFloat16VectorMapping() {
        assertEquals("vector(64)", TypeMapping.mapVector(DataType.Float16Vector, 64));
    }

    @Test
    public void testBFloat16VectorMapping() {
        assertEquals("vector(32)", TypeMapping.mapVector(DataType.BFloat16Vector, 32));
    }

    @Test
    public void testBinaryVectorMapping() {
        assertEquals("bit(256)", TypeMapping.mapVector(DataType.BinaryVector, 256));
    }

    @Test
    public void testSparseFloatVectorMappingIgnoresDimension() {
        assertEquals("sparsevec", TypeMapping.mapVector(DataType.SparseFloatVector, null));
        assertEquals("sparsevec", TypeMapping.mapVector(DataType.SparseFloatVector, 0));
    }

    @Test
    public void testVectorWithNullDimensionThrows() {
        assertThrows(
                MigrationException.class,
                () -> TypeMapping.mapVector(DataType.FloatVector, null));
    }

    @Test
    public void testVectorWithZeroDimensionThrows() {
        assertThrows(
                MigrationException.class,
                () -> TypeMapping.mapVector(DataType.FloatVector, 0));
    }

    @Test
    public void testVectorWithNegativeDimensionThrows() {
        assertThrows(
                MigrationException.class,
                () -> TypeMapping.mapVector(DataType.FloatVector, -1));
    }

    @Test
    public void testMapVectorThrowsForScalarType() {
        assertThrows(
                MigrationException.class,
                () -> TypeMapping.mapVector(DataType.Int64, 10));
    }

    // ---- array types ----

    @Test
    public void testArrayBoolMapping() {
        assertEquals("BOOLEAN[]", TypeMapping.mapArray(DataType.Bool));
    }

    @Test
    public void testArrayInt8AndInt16Mapping() {
        assertEquals("SMALLINT[]", TypeMapping.mapArray(DataType.Int8));
        assertEquals("SMALLINT[]", TypeMapping.mapArray(DataType.Int16));
    }

    @Test
    public void testArrayInt32Mapping() {
        assertEquals("INTEGER[]", TypeMapping.mapArray(DataType.Int32));
    }

    @Test
    public void testArrayInt64Mapping() {
        assertEquals("BIGINT[]", TypeMapping.mapArray(DataType.Int64));
    }

    @Test
    public void testArrayFloatMapping() {
        assertEquals("REAL[]", TypeMapping.mapArray(DataType.Float));
    }

    @Test
    public void testArrayDoubleMapping() {
        assertEquals("DOUBLE PRECISION[]", TypeMapping.mapArray(DataType.Double));
    }

    @Test
    public void testArrayVarCharMapping() {
        assertEquals("TEXT[]", TypeMapping.mapArray(DataType.VarChar));
    }

    @Test
    public void testArrayStringMapping() {
        assertEquals("TEXT[]", TypeMapping.mapArray(DataType.String));
    }

    @Test
    public void testArrayThrowsForUnsupportedElementType() {
        assertThrows(MigrationException.class, () -> TypeMapping.mapArray(DataType.FloatVector));
    }
}
