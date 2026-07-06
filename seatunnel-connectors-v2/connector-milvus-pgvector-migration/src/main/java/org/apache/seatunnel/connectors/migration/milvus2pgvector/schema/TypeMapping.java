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

import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.exception.MigrationErrorCode;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.exception.MigrationException;

/**
 * Maps Milvus {@link DataType} to pgvector column types.
 *
 * <p>Milvus vector types map to pgvector vector types:
 *
 * <ul>
 *   <li>{@link DataType#FloatVector} → {@code vector(N)}
 *   <li>{@link DataType#Float16Vector} → {@code vector(N)} (lossless, stored as float32)
 *   <li>{@link DataType#BFloat16Vector} → {@code vector(N)} (lossless, stored as float32)
 *   <li>{@link DataType#BinaryVector} → {@code bit(N)}
 *   <li>{@link DataType#SparseFloatVector} → {@code sparsevec}
 * </ul>
 */
@Slf4j
public final class TypeMapping {

    public static final int DEFAULT_VARCHAR_LENGTH = 65535;

    private TypeMapping() {}

    /** Map a scalar Milvus DataType (non-vector, non-array) to its pgvector column type string. */
    public static String mapScalar(DataType type) {
        switch (type) {
            case Bool:
                return "BOOLEAN";
            case Int8:
            case Int16:
                return "SMALLINT";
            case Int32:
                return "INTEGER";
            case Int64:
                return "BIGINT";
            case Float:
                return "REAL";
            case Double:
                return "DOUBLE PRECISION";
            case String:
                return "TEXT";
            case JSON:
                return "JSONB";
            case Geometry:
                return "BYTEA";
            case Timestamptz:
                return "TIMESTAMPTZ";
            default:
                throw new MigrationException(
                        MigrationErrorCode.UNSUPPORTED_MILVUS_TYPE,
                        "Milvus type " + type + " is not a scalar type");
        }
    }

    /** Map a VarChar with max length; falls back to DEFAULT_VARCHAR_LENGTH if missing. */
    public static String mapVarChar(Long maxLength) {
        if (maxLength == null || maxLength <= 0) {
            return "VARCHAR(" + DEFAULT_VARCHAR_LENGTH + ")";
        }
        return "VARCHAR(" + maxLength + ")";
    }

    /**
     * Map a Milvus vector DataType to a pgvector vector column type with dimension.
     * All vector types are stored as {@code vector(N)} (float32) for zero precision loss
     * — BFloat16 and FP16 are strict subsets of float32 and can be represented exactly.
     *
     * @param type must be one of FloatVector, Float16Vector, BFloat16Vector, BinaryVector,
     *     SparseFloatVector
     * @param dimension vector dimension; ignored for SparseFloatVector
     */
    public static String mapVector(DataType type, Integer dimension) {
        switch (type) {
            case FloatVector:
            case Float16Vector:
            case BFloat16Vector:
                requireDimension(type, dimension);
                return "vector(" + dimension + ")";
            case BinaryVector:
                requireDimension(type, dimension);
                return "bit(" + dimension + ")";
            case SparseFloatVector:
                return "sparsevec";
            default:
                throw new MigrationException(
                        MigrationErrorCode.UNSUPPORTED_MILVUS_TYPE,
                        "Milvus type " + type + " is not a vector type");
        }
    }

    /** Map an Array element type to its pgvector array column type string (e.g. {@code INTEGER[]}). */
    public static String mapArray(DataType elementType) {
        // PostgreSQL arrays of varchar/text both use TEXT[] to avoid length-tracking complications
        switch (elementType) {
            case Bool:
                return "BOOLEAN[]";
            case Int8:
            case Int16:
                return "SMALLINT[]";
            case Int32:
                return "INTEGER[]";
            case Int64:
                return "BIGINT[]";
            case Float:
                return "REAL[]";
            case Double:
                return "DOUBLE PRECISION[]";
            case VarChar:
            case String:
                return "TEXT[]";
            default:
                throw new MigrationException(
                        MigrationErrorCode.UNSUPPORTED_MILVUS_TYPE,
                        "Milvus Array element type " + elementType + " is not supported");
        }
    }

    private static void requireDimension(DataType type, Integer dimension) {
        if (dimension == null || dimension <= 0) {
            throw new MigrationException(
                    MigrationErrorCode.UNSUPPORTED_MILVUS_TYPE,
                    "Milvus " + type + " requires a positive dimension, got: " + dimension);
        }
    }
}
