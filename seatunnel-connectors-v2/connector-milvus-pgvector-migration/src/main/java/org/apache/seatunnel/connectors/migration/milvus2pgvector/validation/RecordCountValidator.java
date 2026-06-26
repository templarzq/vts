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

import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.response.QueryResp;

import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.exception.MigrationErrorCode;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.exception.MigrationException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;

/** Compares row counts between Milvus and pgvector. */
@Slf4j
public class RecordCountValidator {

    private final MilvusClientV2 milvusClient;
    private final Connection pgConnection;
    private final String collectionName;
    private final String pgSchema;
    private final String pgTable;

    public RecordCountValidator(
            MilvusClientV2 milvusClient,
            Connection pgConnection,
            String collectionName,
            String pgSchema,
            String pgTable) {
        this.milvusClient = milvusClient;
        this.pgConnection = pgConnection;
        this.collectionName = collectionName;
        this.pgSchema = pgSchema;
        this.pgTable = pgTable;
    }

    public ValidationResult validate() {
        long start = System.currentTimeMillis();
        ValidationResult.ValidationResultBuilder b =
                ValidationResult.builder().validatorName("RecordCountValidator");
        try {
            long milvusCount = getMilvusCount();
            long pgCount = getPgCount();
            b.totalChecked(1);
            if (milvusCount == pgCount) {
                b.passed(true);
                b.addDetail("milvus=" + milvusCount + ", pgvector=" + pgCount + " — match");
            } else {
                b.passed(false).failedCount(1);
                b.addDetail("milvus=" + milvusCount + ", pgvector=" + pgCount + " — MISMATCH");
            }
            log.info("Record count: milvus={}, pgvector={}", milvusCount, pgCount);
        } catch (Exception e) {
            b.passed(false).failedCount(1).errorMessage(e.getMessage());
            log.error("Record count validation failed", e);
        }
        b.durationMs(System.currentTimeMillis() - start);
        return b.build();
    }

    private long getMilvusCount() {
        QueryResp resp =
                milvusClient.query(
                        QueryReq.builder()
                                .collectionName(collectionName)
                                .filter("")
                                .outputFields(Collections.singletonList("count(*)"))
                                .build());
        if (resp.getQueryResults() == null || resp.getQueryResults().isEmpty()) {
            return 0L;
        }
        Object val = resp.getQueryResults().get(0).getEntity().get("count(*)");
        if (val instanceof Number) {
            return ((Number) val).longValue();
        }
        return Long.parseLong(String.valueOf(val));
    }

    private long getPgCount() throws SQLException {
        String sql = "SELECT COUNT(*) FROM \"" + pgSchema + "\".\"" + pgTable + "\"";
        try (PreparedStatement ps = pgConnection.prepareStatement(sql);
                ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getLong(1);
            }
            throw new MigrationException(
                    MigrationErrorCode.VALIDATION_QUERY_FAILED,
                    "COUNT(*) returned no rows for " + pgSchema + "." + pgTable);
        }
    }
}
