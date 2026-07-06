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

import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;

/**
 * Static utility for converting Milvus vector values (ByteBuffer / Map) to pgvector-compatible
 * string representations. Used by the CLI direct-migration path and the validation framework where
 * a {@code PGobject} is not needed — only the string value.
 *
 * <p>The decoding logic mirrors {@link
 * org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PgVectorJdbcRowConverter}
 * but produces plain Java strings/arrays instead of PGobject instances.
 */
@Slf4j
public final class VectorFormatConverter {

    private VectorFormatConverter() {}

    /** Convert a FLOAT_VECTOR ByteBuffer (float32 little-endian) to pgvector {@code [v1,v2,...]}. */
    public static String floatBufferToString(ByteBuffer buffer) {
        if (buffer == null) {
            return null;
        }
        int count = buffer.remaining() / 4;
        float[] floats = new float[count];
        for (int i = 0; i < count; i++) {
            floats[i] = buffer.getFloat();
        }
        return floatsToBracketString(floats);
    }

    /** Decode IEEE 754 half precision (binary16) ByteBuffer to float[]. */
    public static float[] float16BufferToFloatArray(ByteBuffer buffer) {
        if (buffer == null) {
            return new float[0];
        }
        int count = buffer.remaining() / 2;
        float[] out = new float[count];
        for (int i = 0; i < count; i++) {
            out[i] = half16ToFloat(buffer.getShort());
        }
        return out;
    }

    /** Decode BFloat16 ByteBuffer to float[]. */
    public static float[] bfloat16BufferToFloatArray(ByteBuffer buffer) {
        if (buffer == null) {
            return new float[0];
        }
        int count = buffer.remaining() / 2;
        float[] out = new float[count];
        for (int i = 0; i < count; i++) {
            out[i] = bfloat16ToFloat(buffer.getShort());
        }
        return out;
    }

    /** Convert a FLOAT16 or BFLOAT16 float[] to pgvector {@code [v1,v2,...]} (stored as float32 vector). */
    public static String floatsToHalfVecString(float[] floats) {
        return floatsToBracketString(floats);
    }

    /** Convert a BINARY_VECTOR ByteBuffer to pgvector bit string {@code "10110010..."}. */
    public static String binaryBufferToBitString(ByteBuffer buffer) {
        if (buffer == null) {
            return null;
        }
        StringBuilder bits = new StringBuilder(buffer.remaining() * 8);
        while (buffer.hasRemaining()) {
            byte b = buffer.get();
            for (int i = 7; i >= 0; i--) {
                bits.append((b >> i) & 0x1);
            }
        }
        return bits.toString();
    }

    /**
     * Convert a SPARSE_FLOAT_VECTOR Map to pgvector sparsevec string {@code
     * "{1:0.5,3:0.7}/dimension"}. Keys are 0-indexed (Milvus); output is 1-indexed (pgvector).
     */
    public static String sparseMapToSparseVecString(Map<?, ?> sparseMap) {
        if (sparseMap == null) {
            return null;
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
            sb.append(e.getKey() + 1).append(":").append(e.getValue());
        }
        sb.append("}/").append(dimension);
        return sb.toString();
    }

    /** Convert float[] to pgvector bracket string {@code [v1,v2,...]}. */
    public static String floatsToBracketString(float[] floats) {
        if (floats == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < floats.length; i++) {
            if (i > 0) {
                sb.append(",");
            }
            float f = sanitizeFloat(floats[i], i);
            sb.append(f);
        }
        sb.append("]");
        return sb.toString();
    }

    /**
     * Sanitize a float value: replace NaN with 0.0f, +/-Infinity with +/-Float.MAX_VALUE,
     * and log a warning for each replacement.
     */
    private static float sanitizeFloat(float f, int index) {
        if (Float.isNaN(f)) {
            log.warn("Float.NaN detected at vector index {}, replacing with 0.0f", index);
            return 0.0f;
        }
        if (Float.isInfinite(f)) {
            float replacement = f > 0 ? Float.MAX_VALUE : -Float.MAX_VALUE;
            log.warn("Float.Infinity ({}) detected at vector index {}, replacing with {}",
                    f, index, replacement);
            return replacement;
        }
        return f;
    }

    // ---- bit-level decoders (same logic as PgVectorJdbcRowConverter) ----

    /** IEEE 754 half precision (binary16): 1 sign + 5 exponent + 10 mantissa. */
    static float half16ToFloat(short h) {
        int bits = h & 0xFFFF;
        int sign = (bits >>> 15) & 0x1;
        int exp = (bits >>> 10) & 0x1F;
        int mant = bits & 0x3FF;
        float result;
        if (exp == 0) {
            result = (mant == 0)
                    ? (sign == 0 ? 0.0f : -0.0f)
                    : (float) (mant * Math.pow(2, -24));
        } else if (exp == 0x1F) {
            result = (mant == 0) ? Float.POSITIVE_INFINITY : Float.NaN;
        } else {
            result = (float) ((Math.pow(-1, sign)) * Math.pow(2, exp - 15) * (1 + mant / 1024.0));
        }
        return sign == 0 ? result : -Math.abs(result);
    }

    /** BFloat16: 1 sign + 8 exponent + 7 mantissa. Upper 16 bits of IEEE 754 float32. */
    static float bfloat16ToFloat(short h) {
        int bits = (h & 0xFFFF) << 16;
        return Float.intBitsToFloat(bits);
    }
}
