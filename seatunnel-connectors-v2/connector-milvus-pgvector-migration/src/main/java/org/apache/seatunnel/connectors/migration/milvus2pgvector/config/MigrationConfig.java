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

package org.apache.seatunnel.connectors.migration.milvus2pgvector.config;

import lombok.Builder;
import lombok.Data;

/**
 * Unified configuration for the Milvus → pgvector migration. Used by the CLI orchestrator and the
 * schema migrator. The SeaTunnel {@code .conf} templates carry equivalent options inline.
 */
@Data
@Builder
public class MigrationConfig {

    // ---- Milvus source ----
    private String milvusUrl;
    private String milvusToken;
    private String milvusDatabase;
    private String milvusCollection;

    // ---- pgvector target ----
    private String pgUrl;
    private String pgUser;
    private String pgPassword;
    private String pgSchema;
    private String pgTable;

    // ---- performance ----
    @Builder.Default private int batchSize = 1000;
    @Builder.Default private int parallelism = 1;
    /** Max rows per second; <= 0 means unlimited. */
    @Builder.Default private int rateLimitRowsPerSecond = 0;
    /** Max seconds to wait when rate-limited before throwing. */
    @Builder.Default private int rateLimitAcquireTimeoutSeconds = 60;

    // ---- behavior ----
    /** Skip index creation on pgvector (table DDL only). */
    @Builder.Default private boolean skipIndexMigration = false;
    /** Drop target table before migration if it exists. */
    @Builder.Default private boolean dropExistingTable = false;

    // ---- validation ----
    @Builder.Default private int validationSampleSize = 100;
    /** Minimum pass rate for field-level comparison validation (0.0–1.0). */
    @Builder.Default private double passRateThreshold = 0.99;

    // ---- audit ----
    /** Directory to write migration logs and progress state. */
    @Builder.Default private String auditLogDir = "./migration-logs";

    public boolean isRateLimited() {
        return rateLimitRowsPerSecond > 0;
    }
}
