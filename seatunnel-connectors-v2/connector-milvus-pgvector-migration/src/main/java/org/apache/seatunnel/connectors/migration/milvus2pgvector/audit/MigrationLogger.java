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

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/** Writes migration progress to both SLF4J and a per-collection log file. */
@Slf4j
public class MigrationLogger implements AutoCloseable {

    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final BufferedWriter writer;
    private final String collectionName;

    public MigrationLogger(Path logDir, String collectionName) {
        this.collectionName = collectionName;
        try {
            Files.createDirectories(logDir);
            String stamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
            Path logFile = logDir.resolve(collectionName + "_" + stamp + ".log");
            this.writer =
                    Files.newBufferedWriter(
                            logFile,
                            StandardOpenOption.CREATE,
                            StandardOpenOption.APPEND);
            info("Migration log initialized for collection: " + collectionName);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create migration log file", e);
        }
    }

    public void info(String message) {
        String line = format("INFO", message);
        log.info(message);
        write(line);
    }

    public void warn(String message) {
        String line = format("WARN", message);
        log.warn(message);
        write(line);
    }

    public void error(String message, Throwable t) {
        String line = format("ERROR", message + " — " + t.getMessage());
        log.error(message, t);
        write(line);
    }

    public void logPhase(String phase, String status) {
        info("PHASE [" + phase + "] → " + status);
    }

    private String format(String level, String message) {
        return "[" + LocalDateTime.now().format(TS) + "] [" + level + "] " + message;
    }

    private void write(String line) {
        try {
            writer.write(line);
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            log.warn("Failed to write to migration log file", e);
        }
    }

    public String getCollectionName() {
        return collectionName;
    }

    @Override
    public void close() {
        info("Migration log closing");
        try {
            if (writer != null) {
                writer.close();
            }
        } catch (IOException e) {
            log.warn("Failed to close migration log file", e);
        }
    }
}
