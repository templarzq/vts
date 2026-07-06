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

package org.apache.seatunnel.connectors.migration.milvus2pgvector.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum MigrationErrorCode implements SeaTunnelErrorCode {
    SCHEMA_INTROSPECTION_FAILED("MIGRATE-01", "Failed to introspect Milvus collection schema"),
    UNSUPPORTED_MILVUS_TYPE("MIGRATE-02", "Unsupported Milvus data type for pgvector migration"),
    UNSUPPORTED_MILVUS_INDEX("MIGRATE-03", "Unsupported Milvus index type for pgvector migration"),
    DDL_EXECUTION_FAILED("MIGRATE-04", "Failed to execute DDL on pgvector"),
    VALIDATION_FAILED("MIGRATE-05", "Data validation failed after migration"),
    VALIDATION_QUERY_FAILED("MIGRATE-06", "Failed to query during validation"),
    RATE_LIMIT_EXCEEDED("MIGRATE-08", "Rate limit wait timed out"),
    MIGRATION_CONFIG_INVALID("MIGRATE-09", "Invalid migration configuration"),
    MIGRATION_ORCHESTRATION_FAILED("MIGRATE-10", "Migration orchestration step failed"),
    ;

    private final String code;
    private final String description;

    MigrationErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
