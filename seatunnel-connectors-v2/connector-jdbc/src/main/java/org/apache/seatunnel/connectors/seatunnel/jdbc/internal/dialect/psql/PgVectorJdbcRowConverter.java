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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SqlType;

import lombok.extern.slf4j.Slf4j;
import org.postgresql.util.PGobject;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Extends {@link PostgresJdbcRowConverter} to write all pgvector column types:
 *
 * <ul>
 *   <li>{@code vector} — from FLOAT_VECTOR (handled by parent)
 *   <li>{@code halfvec} — from FLOAT16_VECTOR / BFLOAT16_VECTOR (bf16 loses precision)
 *   <li>{@code bit} — from BINARY_VECTOR
 *   <li>{@code sparsevec} — from SPARSE_FLOAT_VECTOR
 * </ul>
 */
@Slf4j
public class PgVectorJdbcRowConverter extends PostgresJdbcRowConverter {

    private static final long serialVersionUID = 1L;

    @Override
    public String converterName() {
        return PgVectorDialect.DIALECT_NAME;
    }

    @Override
    protected void setValueToStatementByDataType(
            Object value,
            PreparedStatement statement,
            SeaTunnelDataType<?> seaTunnelDataType,
            int statementIndex,
            @Nullable String sourceType)
            throws SQLException {
        SqlType sqlType = seaTunnelDataType.getSqlType();
        switch (sqlType) {
            case FLOAT16_VECTOR:
                writeHalfVec((ByteBuffer) value, statement, statementIndex);
                return;
            case BFLOAT16_VECTOR:
                log.warn(
                        "Writing BFLOAT16_VECTOR to halfvec may lose precision "
                                + "(bfloat16 has 7 mantissa bits vs halfvec's 10).");
                writeBfloat16AsHalfVec((ByteBuffer) value, statement, statementIndex);
                return;
            case BINARY_VECTOR:
                writeBit((ByteBuffer) value, statement, statementIndex);
                return;
            case SPARSE_FLOAT_VECTOR:
                writeSparseVec((Map<?, ?>) value, statement, statementIndex);
                return;
            default:
                // FLOAT_VECTOR and all non-vector types are handled identically to the parent
                // PostgresJdbcRowConverter (which itself defers to AbstractJdbcRowConverter for
                // non-FLOAT_VECTOR types).
                super.setValueToStatementByDataType(
                        value, statement, seaTunnelDataType, statementIndex, sourceType);
        }
    }

    /** pgvector {@code halfvec} expects the same {@code [v1,v2,...]} text form as {@code vector}. */
    private void writeHalfVec(ByteBuffer buffer, PreparedStatement statement, int index)
            throws SQLException {
        if (buffer == null) {
            statement.setNull(index, java.sql.Types.OTHER);
            return;
        }
        float[] floats = decodeFloat16(buffer);
        statement.setObject(index, buildPgObject("halfvec", floatsToBracketString(floats)));
    }

    /**
     * BFloat16 (Brain Float) — 1 sign + 8 exponent + 7 mantissa. We convert to float32 then write
     * as halfvec. Precision is lost because halfvec stores 16-bit float (5 exponent + 10 mantissa).
     */
    private void writeBfloat16AsHalfVec(ByteBuffer buffer, PreparedStatement statement, int index)
            throws SQLException {
        if (buffer == null) {
            statement.setNull(index, java.sql.Types.OTHER);
            return;
        }
        float[] floats = decodeBfloat16(buffer);
        statement.setObject(index, buildPgObject("halfvec", floatsToBracketString(floats)));
    }

    /** pgvector {@code bit} accepts a textual 0/1 string like {@code "10110010"}. */
    private void writeBit(ByteBuffer buffer, PreparedStatement statement, int index)
            throws SQLException {
        if (buffer == null) {
            statement.setNull(index, java.sql.Types.OTHER);
            return;
        }
        StringBuilder bits = new StringBuilder(buffer.remaining() * 8);
        while (buffer.hasRemaining()) {
            byte b = buffer.get();
            for (int i = 7; i >= 0; i--) {
                bits.append((b >> i) & 0x1);
            }
        }
        statement.setObject(index, buildPgObject("bit", bits.toString()));
    }

    /**
     * pgvector {@code sparsevec} text form: {@code {index:value,...}/dimension}. Indices are
     * 1-indexed in pgvector; Milvus uses 0-indexed keys, so we add 1. The dimension is the highest
     * key + 1.
     */
    private void writeSparseVec(Map<?, ?> sparseMap, PreparedStatement statement, int index)
            throws SQLException {
        if (sparseMap == null) {
            statement.setNull(index, java.sql.Types.OTHER);
            return;
        }
        TreeMap<Integer, Float> sorted = new TreeMap<>();
        int maxIndex = 0;
        for (Map.Entry<?, ?> entry : sparseMap.entrySet()) {
            int k = ((Number) entry.getKey()).intValue();
            float v = ((Number) entry.getValue()).floatValue();
            sorted.put(k, v);
            if (k > maxIndex) {
                maxIndex = k;
            }
        }
        int dimension = maxIndex + 1;
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<Integer, Float> e : sorted.entrySet()) {
            if (!first) {
                sb.append(",");
            }
            first = false;
            // pgvector sparsevec uses 1-indexed positions
            sb.append(e.getKey() + 1).append(":").append(e.getValue());
        }
        sb.append("}/").append(dimension);
        statement.setObject(index, buildPgObject("sparsevec", sb.toString()));
    }

    // ---- fp16 / bf16 decoders ----

    /** IEEE 754 half precision (binary16): 1 sign + 5 exponent + 10 mantissa. */
    private static float[] decodeFloat16(ByteBuffer buffer) {
        int count = buffer.remaining() / 2;
        float[] out = new float[count];
        for (int i = 0; i < count; i++) {
            short h = buffer.getShort();
            out[i] = half16ToFloat(h);
        }
        return out;
    }

    /** BFloat16: 1 sign + 8 exponent + 7 mantissa. Stored as the upper 16 bits of an IEEE 754 float. */
    private static float[] decodeBfloat16(ByteBuffer buffer) {
        int count = buffer.remaining() / 2;
        float[] out = new float[count];
        for (int i = 0; i < count; i++) {
            short h = buffer.getShort();
            out[i] = bfloat16ToFloat(h);
        }
        return out;
    }

    private static float half16ToFloat(short h) {
        int bits = h & 0xFFFF;
        int sign = (bits >>> 15) & 0x1;
        int exp = (bits >>> 10) & 0x1F;
        int mant = bits & 0x3FF;
        float result;
        if (exp == 0) {
            // subnormal
            result = (mant == 0)
                    ? (sign == 0 ? 0.0f : -0.0f)
                    : (float) (mant * Math.pow(2, -24));
        } else if (exp == 0x1F) {
            // inf or nan
            result = (mant == 0) ? Float.POSITIVE_INFINITY : Float.NaN;
        } else {
            // normal
            result = (float) ((Math.pow(-1, sign)) * Math.pow(2, exp - 15) * (1 + mant / 1024.0));
        }
        return sign == 0 ? result : -Math.abs(result);
    }

    private static float bfloat16ToFloat(short h) {
        // bfloat16 is the upper 16 bits of a 32-bit IEEE float — left-shift by 16 bits
        int bits = (h & 0xFFFF) << 16;
        return Float.intBitsToFloat(bits);
    }

    private static String floatsToBracketString(float[] floats) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < floats.length; i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append(sanitizeFloat(floats[i], i));
        }
        sb.append("]");
        return sb.toString();
    }

    private static PGobject buildPgObject(String typeName, String value) throws SQLException {
        PGobject obj = new PGobject();
        obj.setType(typeName);
        obj.setValue(value);
        return obj;
    }
}
