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

import lombok.Data;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/** Aggregated report of all validation results for one collection → table migration. */
@Data
public class ValidationReport {

    private String collectionName;
    private String pgTable;
    private Instant timestamp;
    private List<ValidationResult> results = new ArrayList<>();

    public boolean isOverallPassed() {
        if (results == null || results.isEmpty()) {
            return true;
        }
        return results.stream().allMatch(ValidationResult::isPassed);
    }

    public void addResult(ValidationResult result) {
        results.add(result);
    }

    /** Append a human-readable summary to {@code sb}. */
    public String formatConsole() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n========== Validation Report ==========\n");
        sb.append("Collection : ").append(collectionName).append('\n');
        sb.append("PG Table   : ").append(pgTable).append('\n');
        sb.append("Timestamp  : ").append(timestamp).append('\n');
        sb.append("Overall    : ").append(isOverallPassed() ? "PASSED" : "FAILED").append('\n');
        sb.append("----------------------------------------\n");
        for (ValidationResult r : results) {
            sb.append(String.format(
                    "  [%s] %s — checked=%d, failed=%d, %dms",
                    r.isPassed() ? "OK" : "FAIL",
                    r.getValidatorName(),
                    r.getTotalChecked(),
                    r.getFailedCount(),
                    r.getDurationMs()));
            if (r.getErrorMessage() != null && !r.getErrorMessage().isEmpty()) {
                sb.append("\n      error: ").append(r.getErrorMessage());
            }
            int shown = 0;
            for (String d : r.getDetails()) {
                if (shown >= 10) {
                    sb.append("\n      ... and ")
                            .append(r.getDetails().size() - shown)
                            .append(" more");
                    break;
                }
                sb.append("\n      ").append(d);
                shown++;
            }
            sb.append('\n');
        }
        sb.append("========================================\n");
        return sb.toString();
    }

    /** Write a JSON representation of this report to {@code file}. Best-effort — never throws. */
    public void writeToFile(Path file) {
        try {
            StringBuilder json = new StringBuilder();
            json.append("{\n");
            json.append("  \"collectionName\": \"").append(escape(collectionName)).append("\",\n");
            json.append("  \"pgTable\": \"").append(escape(pgTable)).append("\",\n");
            json.append("  \"timestamp\": \"").append(timestamp).append("\",\n");
            json.append("  \"overallPassed\": ").append(isOverallPassed()).append(",\n");
            json.append("  \"results\": [\n");
            for (int i = 0; i < results.size(); i++) {
                ValidationResult r = results.get(i);
                json.append("    {\n");
                json.append("      \"validatorName\": \"")
                        .append(escape(r.getValidatorName()))
                        .append("\",\n");
                json.append("      \"passed\": ").append(r.isPassed()).append(",\n");
                json.append("      \"totalChecked\": ").append(r.getTotalChecked()).append(",\n");
                json.append("      \"failedCount\": ").append(r.getFailedCount()).append(",\n");
                json.append("      \"durationMs\": ").append(r.getDurationMs()).append(",\n");
                json.append("      \"errorMessage\": \"")
                        .append(r.getErrorMessage() == null ? "" : escape(r.getErrorMessage()))
                        .append("\",\n");
                json.append("      \"details\": [");
                for (int j = 0; j < r.getDetails().size(); j++) {
                    json.append(j == 0 ? "" : ", ");
                    json.append("\"").append(escape(r.getDetails().get(j))).append("\"");
                }
                json.append("]\n");
                json.append("    }");
                if (i < results.size() - 1) {
                    json.append(',');
                }
                json.append('\n');
            }
            json.append("  ]\n");
            json.append("}\n");
            Files.createDirectories(file.getParent() != null ? file.getParent() : Path.of("."));
            Files.write(file, json.toString().getBytes());
        } catch (IOException e) {
            // best-effort — validation report write failure should not crash migration
            System.err.println("WARNING: Failed to write validation report to " + file + ": " + e.getMessage());
        }
    }

    private static String escape(String s) {
        if (s == null) {
            return "";
        }
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r");
    }
}
