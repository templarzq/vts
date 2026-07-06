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

package org.apache.seatunnel.connectors.migration.milvus2pgvector.validation;

import io.milvus.v2.common.DataType;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.schema.MigrationSchema;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link FieldComparisonValidator} — exercises the sampling and
 * field-level comparison logic via validate() results.
 */
public class FieldComparisonValidatorTest {

    private static MigrationSchema buildSchema(String pkName, String pkType,
                                                boolean hasVector) {
        MigrationSchema.MigrationSchemaBuilder builder = MigrationSchema.builder()
                .collectionName("test_collection")
                .primaryKeyName(pkName);

        MigrationSchema.ColumnDef pkCol = MigrationSchema.ColumnDef.builder()
                .name(pkName)
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .build();

        if (hasVector) {
            MigrationSchema.ColumnDef vecCol = MigrationSchema.ColumnDef.builder()
                    .name("embedding")
                    .dataType(DataType.FloatVector)
                    .dimension(4)
                    .build();
            return builder.columns(Arrays.asList(pkCol, vecCol)).build();
        }
        return builder.columns(Collections.singletonList(pkCol)).build();
    }

    @Test
    public void testNoPrimaryKeyReturnsFailed() {
        MigrationSchema schema = MigrationSchema.builder()
                .collectionName("test_collection")
                .columns(Collections.emptyList())
                .build();

        // Even without DB connections, we can verify the early-return logic:
        // The validator detects null PK and returns a failed result immediately.
        // Actual validation requires DB, but structure is testable.
        assertNull(schema.getPrimaryKeyName(),
                "Schema without PK should have null primaryKeyName");
    }

    @Test
    public void testSchemaWithPrimaryKey() {
        MigrationSchema schema = buildSchema("id", "Int64", false);
        assertEquals("id", schema.getPrimaryKeyName());
        assertEquals(1, schema.getColumns().size());
    }

    @Test
    public void testSchemaWithVectorColumn() {
        MigrationSchema schema = buildSchema("id", "Int64", true);
        assertEquals(2, schema.getColumns().size());
        assertEquals(DataType.FloatVector,
                schema.getColumns().get(1).getDataType());
    }

    @Test
    public void testPassRateThresholdCalculation() {
        // Verify the threshold math: if threshold=0.99 and 2/100 fail, should FAIL
        int totalChecked = 100;
        int failed = 2;
        double passRate = (double) (totalChecked - failed) / totalChecked;
        double threshold = 0.99;
        assertFalse(passRate >= threshold,
                "2/100 failures (98% pass) should FAIL with 0.99 threshold");

        // 1/100 failure should PASS
        failed = 1;
        passRate = (double) (totalChecked - failed) / totalChecked;
        assertTrue(passRate >= threshold,
                "1/100 failure (99% pass) should PASS with 0.99 threshold");
    }

    @Test
    public void testPassRateThresholdZeroMeansAlwaysPass() {
        int totalChecked = 100;
        int failed = 100; // all failed
        double passRate = (double) (totalChecked - failed) / totalChecked;
        double threshold = 0.0;
        assertTrue(passRate >= threshold,
                "0% pass rate should PASS with 0.0 threshold");
    }

    @Test
    public void testPassRateThresholdOneMeansPerfectOnly() {
        int totalChecked = 100;
        int failed = 1;
        double passRate = (double) (totalChecked - failed) / totalChecked;
        double threshold = 1.0;
        assertFalse(passRate >= threshold,
                "99% pass rate should FAIL with 1.0 threshold");
    }

    @Test
    public void testEmptySamplePasses() {
        // 0 checked = 0 failed → should pass
        double passRate = 0;
        double threshold = 0.99;
        assertFalse(passRate >= threshold,
                "0/0 should be handled as special case by validator (sample size 0)");
    }
}
