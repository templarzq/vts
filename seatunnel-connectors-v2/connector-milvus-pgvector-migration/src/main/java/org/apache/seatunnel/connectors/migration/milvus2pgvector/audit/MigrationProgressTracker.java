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

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Optional;

/**
 * Tracks migration progress in a JSON state file. Supports breakpoint resume: on {@code --resume},
 * the orchestrator reads the state file to determine which phases have already completed.
 *
 * <p>State file: {@code {logDir}/{collectionName}_progress.json}
 */
@Slf4j
public class MigrationProgressTracker {

    public enum Phase {
        SCHEMA,
        DATA,
        VALIDATION
    }

    public enum Status {
        RUNNING,
        SUCCESS,
        FAILED
    }

    @Data
    public static class ProgressState {
        private String collectionName;
        private Phase schemaPhase = Phase.SCHEMA;
        private Status schemaStatus;
        private Status dataStatus;
        private Status validationStatus;
        private String lastCheckpointPk;
        private long rowsMigrated;
        private long rowsTotal;
        private Instant timestamp;

        public boolean isPhaseComplete(Phase phase) {
            Status s = getStatus(phase);
            return s == Status.SUCCESS;
        }

        public Status getStatus(Phase phase) {
            switch (phase) {
                case SCHEMA:
                    return schemaStatus;
                case DATA:
                    return dataStatus;
                case VALIDATION:
                    return validationStatus;
                default:
                    return null;
            }
        }
    }

    private final Path stateFile;
    private ProgressState state;

    public MigrationProgressTracker(Path logDir, String collectionName) {
        try {
            Files.createDirectories(logDir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create log directory: " + logDir, e);
        }
        this.stateFile = logDir.resolve(collectionName + "_progress.json");
        this.state = load();
        this.state.setCollectionName(collectionName);
    }

    /** Mark a phase as running. */
    public void markPhaseStart(Phase phase) {
        state.setTimestamp(Instant.now());
        setStatus(phase, Status.RUNNING);
        save();
    }

    /** Mark a phase as successfully completed. */
    public void markPhaseComplete(Phase phase) {
        state.setTimestamp(Instant.now());
        setStatus(phase, Status.SUCCESS);
        save();
    }

    /** Mark a phase as failed. */
    public void markPhaseFailed(Phase phase) {
        state.setTimestamp(Instant.now());
        setStatus(phase, Status.FAILED);
        save();
    }

    /** Save a checkpoint (primary key of the last migrated row). */
    public void saveCheckpoint(String pk) {
        state.setLastCheckpointPk(pk);
        state.setTimestamp(Instant.now());
        save();
    }

    /** Update rows migrated count. */
    public void updateRowsMigrated(long rows) {
        state.setRowsMigrated(rows);
        save();
    }

    public void setRowsTotal(long total) {
        state.setRowsTotal(total);
        save();
    }

    public boolean isPhaseComplete(Phase phase) {
        return state.isPhaseComplete(phase);
    }

    public Optional<String> getLastCheckpoint() {
        return Optional.ofNullable(state.getLastCheckpointPk());
    }

    public ProgressState getState() {
        return state;
    }

    private void setStatus(Phase phase, Status status) {
        switch (phase) {
            case SCHEMA:
                state.setSchemaStatus(status);
                break;
            case DATA:
                state.setDataStatus(status);
                break;
            case VALIDATION:
                state.setValidationStatus(status);
                break;
            default:
                break;
        }
    }

    private ProgressState load() {
        if (!Files.exists(stateFile)) {
            return new ProgressState();
        }
        try {
            String json = new String(Files.readAllBytes(stateFile));
            return parseJson(json);
        } catch (IOException e) {
            log.warn("Failed to read progress state, starting fresh: {}", e.getMessage());
            return new ProgressState();
        }
    }

    private void save() {
        try {
            Files.write(stateFile, toJson(state).getBytes());
        } catch (IOException e) {
            log.warn("Failed to save progress state: {}", e.getMessage());
        }
    }

    // ---- minimal JSON serialization (no external dependency) ----

    private static ProgressState parseJson(String json) {
        ProgressState s = new ProgressState();
        s.setCollectionName(extractString(json, "collectionName"));
        s.setSchemaStatus(parseStatus(extractString(json, "schemaStatus")));
        s.setDataStatus(parseStatus(extractString(json, "dataStatus")));
        s.setValidationStatus(parseStatus(extractString(json, "validationStatus")));
        s.setLastCheckpointPk(extractString(json, "lastCheckpointPk"));
        String rows = extractString(json, "rowsMigrated");
        if (!rows.isEmpty()) {
            try {
                s.setRowsMigrated(Long.parseLong(rows));
            } catch (NumberFormatException ignored) {
            }
        }
        String total = extractString(json, "rowsTotal");
        if (!total.isEmpty()) {
            try {
                s.setRowsTotal(Long.parseLong(total));
            } catch (NumberFormatException ignored) {
            }
        }
        return s;
    }

    private static Status parseStatus(String s) {
        if (s == null || s.isEmpty() || "null".equals(s)) {
            return null;
        }
        try {
            return Status.valueOf(s);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private static String extractString(String json, String key) {
        String search = "\"" + key + "\": \"";
        int start = json.indexOf(search);
        if (start < 0) {
            search = "\"" + key + "\":";
            start = json.indexOf(search);
            if (start < 0) {
                return "";
            }
            start += search.length();
            int end = start;
            while (end < json.length() && json.charAt(end) != ',' && json.charAt(end) != '}'
                    && json.charAt(end) != '\n') {
                end++;
            }
            return json.substring(start, end).trim().replace("\"", "");
        }
        start += search.length();
        int end = json.indexOf("\"", start);
        if (end < 0) {
            return "";
        }
        return json.substring(start, end);
    }

    private static String toJson(ProgressState s) {
        StringBuilder sb = new StringBuilder("{\n");
        sb.append("  \"collectionName\": \"").append(s.getCollectionName()).append("\",\n");
        sb.append("  \"schemaStatus\": \"").append(s.getSchemaStatus() != null ? s.getSchemaStatus() : "").append("\",\n");
        sb.append("  \"dataStatus\": \"").append(s.getDataStatus() != null ? s.getDataStatus() : "").append("\",\n");
        sb.append("  \"validationStatus\": \"").append(s.getValidationStatus() != null ? s.getValidationStatus() : "").append("\",\n");
        sb.append("  \"lastCheckpointPk\": \"").append(s.getLastCheckpointPk() != null ? s.getLastCheckpointPk() : "").append("\",\n");
        sb.append("  \"rowsMigrated\": ").append(s.getRowsMigrated()).append(",\n");
        sb.append("  \"rowsTotal\": ").append(s.getRowsTotal()).append(",\n");
        sb.append("  \"timestamp\": \"").append(s.getTimestamp() != null ? s.getTimestamp() : "").append("\"\n");
        sb.append("}\n");
        return sb.toString();
    }
}
