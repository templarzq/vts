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

package org.apache.seatunnel.connectors.migration.milvus2pgvector.audit;

import org.apache.seatunnel.connectors.migration.milvus2pgvector.validation.ValidationReport;

import lombok.Builder;
import lombok.Data;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

/** Final summary of a migration run. Written to the audit log directory as JSON. */
@Data
@Builder
public class MigrationReport {

    private String collectionName;
    private String pgTable;
    private Instant startTime;
    private Instant endTime;
    private String schemaMigrationResult;
    private String dataMigrationResult;
    private ValidationReport validationReport;
    private long totalRowsMigrated;
    private long durationSeconds;
    @Builder.Default private boolean success = false;

    public String formatConsole() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n========== Migration Report ==========\n");
        sb.append("Collection      : ").append(collectionName).append('\n');
        sb.append("PG Table        : ").append(pgTable).append('\n');
        sb.append("Start           : ").append(startTime).append('\n');
        sb.append("End             : ").append(endTime).append('\n');
        sb.append("Duration        : ").append(durationSeconds).append("s\n");
        sb.append("Rows Migrated   : ").append(totalRowsMigrated).append('\n');
        sb.append("Schema Migration: ").append(schemaMigrationResult).append('\n');
        sb.append("Data Migration  : ").append(dataMigrationResult).append('\n');
        sb.append("Validation      : ");
        if (validationReport != null) {
            sb.append(validationReport.isOverallPassed() ? "PASSED" : "FAILED");
        } else {
            sb.append("SKIPPED");
        }
        sb.append('\n');
        sb.append("Overall         : ").append(success ? "SUCCESS" : "FAILED").append('\n');
        sb.append("======================================\n");
        if (validationReport != null) {
            sb.append(validationReport.formatConsole());
        }
        return sb.toString();
    }

    public void writeToFile(Path file) {
        try {
            StringBuilder json = new StringBuilder("{\n");
            json.append("  \"collectionName\": \"").append(escape(collectionName)).append("\",\n");
            json.append("  \"pgTable\": \"").append(escape(pgTable)).append("\",\n");
            json.append("  \"startTime\": \"").append(startTime).append("\",\n");
            json.append("  \"endTime\": \"").append(endTime).append("\",\n");
            json.append("  \"durationSeconds\": ").append(durationSeconds).append(",\n");
            json.append("  \"totalRowsMigrated\": ").append(totalRowsMigrated).append(",\n");
            json.append("  \"schemaMigrationResult\": \"").append(escape(schemaMigrationResult)).append("\",\n");
            json.append("  \"dataMigrationResult\": \"").append(escape(dataMigrationResult)).append("\",\n");
            json.append("  \"validationPassed\": ").append(validationReport != null && validationReport.isOverallPassed()).append(",\n");
            json.append("  \"success\": ").append(success).append("\n");
            json.append("}\n");
            if (file.getParent() != null) {
                Files.createDirectories(file.getParent());
            }
            Files.write(file, json.toString().getBytes());
        } catch (IOException e) {
            System.err.println("WARNING: Failed to write migration report: " + e.getMessage());
        }
    }

    private static String escape(String s) {
        if (s == null) {
            return "";
        }
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
