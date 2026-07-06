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

import org.apache.seatunnel.api.table.type.VectorType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.postgresql.util.PGobject;

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class PgVectorJdbcRowConverterTest {

    private PgVectorJdbcRowConverter converter;
    private PreparedStatement statement;

    @BeforeEach
    void setUp() {
        converter = new PgVectorJdbcRowConverter();
        statement = mock(PreparedStatement.class);
    }

    private static ByteBuffer fp16Buffer(short... shorts) {
        ByteBuffer buffer = ByteBuffer.allocate(shorts.length * 2);
        for (short s : shorts) {
            buffer.putShort(s);
        }
        buffer.flip();
        return buffer;
    }

    private PGobject capturePgObject(int statementIndex) throws Exception {
        ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        verify(statement).setObject(captor.capture(), captor.capture());
        // setObject(index, value) — first captor is index, second is value
        Object captured = captor.getAllValues().get(1);
        assertNotNull(captured, "expected PGobject to be set");
        return (PGobject) captured;
    }

    /** 1.5f in fp16 = 0x3E00 (sign=0, exp=15, mant=512). */
    @Test
    void testFloat16VectorWritesHalfVec() throws Exception {
        // 1.5 -> 0x3E00, 2.0 -> 0x4000
        ByteBuffer buffer = fp16Buffer((short) 0x3E00, (short) 0x4000);

        converter.setValueToStatementByDataType(
                buffer, statement, VectorType.VECTOR_FLOAT16_TYPE, 1, null);

        PGobject pgObject = capturePgObject(1);
        assertEquals("vector", pgObject.getType());
        assertEquals("[1.5,2.0]", pgObject.getValue());
    }

    /** 1.5f as bfloat16 = upper 16 bits of 0x3FC00000 = 0x3FC0. */
    @Test
    void testBfloat16VectorWritesVector() throws Exception {
        ByteBuffer buffer = fp16Buffer((short) 0x3FC0);

        converter.setValueToStatementByDataType(
                buffer, statement, VectorType.VECTOR_BFLOAT16_TYPE, 1, null);

        PGobject pgObject = capturePgObject(1);
        assertEquals("vector", pgObject.getType());
        assertEquals("[1.5]", pgObject.getValue());
    }

    /** Byte 0xB2 = 10110010 in binary. */
    @Test
    void testBinaryVectorWritesBit() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put((byte) 0xB2);
        buffer.flip();

        converter.setValueToStatementByDataType(
                buffer, statement, VectorType.VECTOR_BINARY_TYPE, 1, null);

        PGobject pgObject = capturePgObject(1);
        assertEquals("bit", pgObject.getType());
        assertEquals("10110010", pgObject.getValue());
    }

    /** Milvus sparse map {0:0.5, 2:0.7} → pgvector "{1:0.5,3:0.7}/3". */
    @Test
    void testSparseFloatVectorWritesSparseVec() throws Exception {
        Map<Integer, Float> sparseMap = new LinkedHashMap<>();
        sparseMap.put(0, 0.5f);
        sparseMap.put(2, 0.7f);

        converter.setValueToStatementByDataType(
                sparseMap, statement, VectorType.VECTOR_SPARSE_FLOAT_TYPE, 1, null);

        PGobject pgObject = capturePgObject(1);
        assertEquals("sparsevec", pgObject.getType());
        assertEquals("{1:0.5,3:0.7}/3", pgObject.getValue());
    }

    @Test
    void testDialectNameIsPgvector() {
        assertEquals("pgvector", new PgVectorDialect().dialectName());
    }
}
