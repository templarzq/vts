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

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link VectorFormatConverter} — verifies that BFloat16 and FP16
 * are decoded losslessly to float32.
 */
public class VectorFormatConverterTest {

    @Test
    public void testBfloat16DecodeExactRoundTrip() {
        // BFloat16 is the upper 16 bits of float32 — values should be EXACT, not approximate.
        // 1.5f in IEEE 754: 0x3FC00000, upper 16 bits = 0x3FC0
        short bf16 = (short) 0x3FC0;
        ByteBuffer buf = ByteBuffer.allocate(2).putShort(bf16);
        buf.flip();

        float[] result = VectorFormatConverter.bfloat16BufferToFloatArray(buf);
        assertEquals(1, result.length);
        // Must be EXACT — BFloat16 is a strict subset of float32
        assertEquals(1.5f, result[0], 0.0f, "BFloat16→float32 should be exact, not approximate");
    }

    @Test
    public void testFloat16DecodeExactRoundTrip() {
        // IEEE 754 half precision: 1.5f = 0x3E00
        // sign=0, exp=01111=15, mant=1000000000=512
        // value = (-1)^0 * 2^(15-15) * (1 + 512/1024) = 1 * 1 * 1.5 = 1.5
        short fp16 = (short) 0x3E00;
        ByteBuffer buf = ByteBuffer.allocate(2).putShort(fp16);
        buf.flip();

        float[] result = VectorFormatConverter.float16BufferToFloatArray(buf);
        assertEquals(1, result.length);
        assertEquals(1.5f, result[0], 0.0f, "FP16→float32 should be exact for this value");
    }

    @Test
    public void testBfloat16ZeroValue() {
        short bf16 = (short) 0x0000;
        ByteBuffer buf = ByteBuffer.allocate(2).putShort(bf16);
        buf.flip();

        float[] result = VectorFormatConverter.bfloat16BufferToFloatArray(buf);
        assertEquals(0.0f, result[0], 0.0f);
    }

    @Test
    public void testBfloat16NegativeZero() {
        // -0 in BFloat16: sign=1, all zeros
        short bf16 = (short) 0x8000;
        ByteBuffer buf = ByteBuffer.allocate(2).putShort(bf16);
        buf.flip();

        float[] result = VectorFormatConverter.bfloat16BufferToFloatArray(buf);
        assertEquals(-0.0f, result[0], 0.0f);
    }

    @Test
    public void testBfloat16MaxNormalValue() {
        // max normal BFloat16: 0x7F7F → sign=0, exp=254, mant=127
        // value = (-1)^0 * 2^(254-127) * (1 + 127/128) ≈ 3.39e38 (same as float32 max)
        // Actually: the max exponent in BFloat16 is 8-bit, bias 127, max=254
        // 2^(254-127) * (1 + 127/128) = 2^127 * 1.9921875 ≈ 3.389e38
        short bf16 = (short) 0x7F7F;
        ByteBuffer buf = ByteBuffer.allocate(2).putShort(bf16);
        buf.flip();

        float[] result = VectorFormatConverter.bfloat16BufferToFloatArray(buf);
        assertFalse(Float.isNaN(result[0]), "Max BFloat16 should not be NaN");
        assertFalse(Float.isInfinite(result[0]),
                "Max BFloat16 should not be Inf — BFloat16 has same exponent range as float32");
    }

    @Test
    public void testFloat16MaxValue() {
        // max normal FP16: 0x7BFF → sign=0, exp=30, mant=1023
        // value = 2^(30-15) * (1 + 1023/1024) = 2^15 * 1.999 = 65504
        short fp16 = (short) 0x7BFF;
        ByteBuffer buf = ByteBuffer.allocate(2).putShort(fp16);
        buf.flip();

        float[] result = VectorFormatConverter.float16BufferToFloatArray(buf);
        assertEquals(65504.0f, result[0], 1.0f,
                "FP16 max value should be ~65504");
    }

    @Test
    public void testBfloat16NegativeValue() {
        // -3.0f in IEEE 754: 0xC0400000, upper 16 bits = 0xC040
        short bf16 = (short) 0xC040;
        ByteBuffer buf = ByteBuffer.allocate(2).putShort(bf16);
        buf.flip();

        float[] result = VectorFormatConverter.bfloat16BufferToFloatArray(buf);
        assertEquals(-3.0f, result[0], 0.0f,
                "BFloat16→float32 should preserve sign and magnitude exactly");
    }

    @Test
    public void testMultipleBfloat16Values() {
        // 3 BFloat16 values: 0.5, 1.0, 2.0
        // 0.5 → 0x3F00, 1.0 → 0x3F80, 2.0 → 0x4000
        ByteBuffer buf = ByteBuffer.allocate(6);
        buf.putShort((short) 0x3F00);
        buf.putShort((short) 0x3F80);
        buf.putShort((short) 0x4000);
        buf.flip();

        float[] result = VectorFormatConverter.bfloat16BufferToFloatArray(buf);
        assertEquals(3, result.length);
        assertEquals(0.5f, result[0], 0.0f);
        assertEquals(1.0f, result[1], 0.0f);
        assertEquals(2.0f, result[2], 0.0f);
    }

    @Test
    public void testFloat16SubnormalValue() {
        // Subnormal: exp=0, mant=1 → value = 1 * 2^(-24) ≈ 5.96e-8
        short fp16 = (short) 0x0001;
        ByteBuffer buf = ByteBuffer.allocate(2).putShort(fp16);
        buf.flip();

        float[] result = VectorFormatConverter.float16BufferToFloatArray(buf);
        assertTrue(result[0] > 0.0f, "Subnormal FP16 should be positive");
        assertTrue(result[0] < 1e-7f, "Subnormal FP16 should be very small");
    }

    @Test
    public void testBfloat16VsFloat16RangeComparison() {
        // FP16 max = 65504 (limited exponent)
        // BFloat16 max ≈ 3.39e38 (same exponent range as float32)
        // This is WHY we MUST store BFloat16 as vector(float32) — halfvec would overflow!

        // BFloat16 value around 100000 →
        // float32: 100000.0f = 0x47C35000, upper 16 bits = 0x47C3
        Short bf16Large = (short) 0x47C3;
        ByteBuffer bfBuf = ByteBuffer.allocate(2).putShort(bf16Large);
        bfBuf.flip();
        float bfResult = VectorFormatConverter.bfloat16BufferToFloatArray(bfBuf)[0];
        assertTrue(bfResult > 65000.0f,
                "BFloat16 value " + bfResult + " exceeds FP16 range (65504). "
                + "This proves BFloat16 MUST be stored as vector(float32), not halfvec.");
    }

    @Test
    public void testNullBufferReturnsEmpty() {
        float[] result = VectorFormatConverter.bfloat16BufferToFloatArray(null);
        assertEquals(0, result.length);

        result = VectorFormatConverter.float16BufferToFloatArray(null);
        assertEquals(0, result.length);
    }
}
