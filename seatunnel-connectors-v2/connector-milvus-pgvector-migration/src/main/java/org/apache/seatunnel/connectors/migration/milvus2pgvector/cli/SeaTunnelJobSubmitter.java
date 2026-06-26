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

package org.apache.seatunnel.connectors.migration.milvus2pgvector.cli;

import org.apache.seatunnel.connectors.migration.milvus2pgvector.audit.MigrationLogger;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.config.MigrationConfig;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.exception.MigrationErrorCode;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.exception.MigrationException;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Submits SeaTunnel migration jobs by invoking the {@code seatunnel} CLI as an external process.
 *
 * <p>This avoids pulling in the heavy {@code seatunnel-engine-client} + Hazelcast client
 * dependencies into the connector module. The {@code seatunnel} executable is resolved from (in
 * order): the constructor-supplied home directory, the {@code SEATUNNEL_HOME} environment
 * variable, or {@code PATH}.
 */
@Slf4j
public class SeaTunnelJobSubmitter {

    private static final String TEMPLATE_RESOURCE = "/templates/milvus_to_pgvector.conf";
    private static final DateTimeFormatter STAMP_FMT = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");

    private final String seatunnelHome;
    private final Path workDir;

    /**
     * @param seatunnelHome SeaTunnel installation directory (contains {@code bin/seatunnel}), or
     *     {@code null} to fall back to {@code SEATUNNEL_HOME} env var or {@code PATH}.
     * @param workDir directory for generated .conf files
     */
    public SeaTunnelJobSubmitter(String seatunnelHome, Path workDir) {
        this.seatunnelHome = seatunnelHome != null ? seatunnelHome : System.getenv("SEATUNNEL_HOME");
        this.workDir = workDir;
        try {
            Files.createDirectories(workDir);
        } catch (IOException e) {
            throw new MigrationException(
                    MigrationErrorCode.MIGRATION_ORCHESTRATION_FAILED,
                    "Failed to create work directory: " + workDir,
                    e);
        }
    }

    /**
     * Submit the migration job and block until completion.
     *
     * @param config the migration configuration
     * @param logger migration logger for forwarding process output
     * @return process exit code (0 = success)
     */
    public int submitAndWait(MigrationConfig config, MigrationLogger logger) {
        Map<String, String> variables = buildVariables(config);
        Path confFile = writeConfFile(variables, logger);
        String seatunnelBin = resolveSeatunnelBin();

        logger.info("Submitting SeaTunnel job with config: " + confFile);
        logger.info("SeaTunnel binary: " + seatunnelBin);

        ProcessBuilder pb = new ProcessBuilder();
        pb.command(buildCommand(seatunnelBin, confFile, variables));
        pb.redirectErrorStream(true);

        Process process;
        try {
            process = pb.start();
        } catch (IOException e) {
            throw new MigrationException(
                    MigrationErrorCode.MIGRATION_ORCHESTRATION_FAILED,
                    "Failed to start seatunnel process: " + e.getMessage(),
                    e);
        }

        // Forward stdout/stderr to logger in real time
        try (BufferedReader reader =
                new BufferedReader(
                        new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                logger.info("[seatunnel] " + line);
            }
        } catch (IOException e) {
            logger.warn("Failed to read seatunnel output: " + e.getMessage());
        }

        try {
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                logger.error("SeaTunnel job failed with exit code " + exitCode, new RuntimeException("exit code " + exitCode));
            } else {
                logger.info("SeaTunnel job completed successfully");
            }
            return exitCode;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            process.destroyForcibly();
            throw new MigrationException(
                    MigrationErrorCode.MIGRATION_ORCHESTRATION_FAILED,
                    "SeaTunnel job was interrupted",
                    e);
        }
    }

    /** Build the seatunnel CLI command: {@code seatunnel -c <conf> -i key=value ...}. */
    private java.util.List<String> buildCommand(
            String seatunnelBin, Path confFile, Map<String, String> variables) {
        java.util.List<String> cmd = new java.util.ArrayList<>();
        cmd.add(seatunnelBin);
        cmd.add("-c");
        cmd.add(confFile.toString());
        for (Map.Entry<String, String> entry : variables.entrySet()) {
            cmd.add("-i");
            cmd.add(entry.getKey() + "=" + (entry.getValue() != null ? entry.getValue() : ""));
        }
        return cmd;
    }

    /** Build the variable substitution map from MigrationConfig. */
    private Map<String, String> buildVariables(MigrationConfig config) {
        Map<String, String> vars = new LinkedHashMap<>();
        vars.put("milvus.url", config.getMilvusUrl());
        vars.put("milvus.token", config.getMilvusToken() != null ? config.getMilvusToken() : "");
        vars.put("milvus.database", config.getMilvusDatabase());
        vars.put("milvus.collection", config.getMilvusCollection());
        vars.put("pg.url", config.getPgUrl());
        vars.put("pg.user", config.getPgUser());
        vars.put("pg.password", config.getPgPassword());
        vars.put("pg.database", extractDatabaseName(config.getPgUrl()));
        vars.put("pg.schema", config.getPgSchema());
        vars.put("pg.table", config.getPgTable());
        vars.put("batch.size", String.valueOf(config.getBatchSize()));
        return vars;
    }

    /** Extract database name from JDBC URL (e.g. {@code jdbc:postgresql://host:port/dbname}). */
    static String extractDatabaseName(String jdbcUrl) {
        if (jdbcUrl == null) {
            return "";
        }
        int slash = jdbcUrl.lastIndexOf('/');
        if (slash < 0 || slash == jdbcUrl.length() - 1) {
            return "";
        }
        String tail = jdbcUrl.substring(slash + 1);
        // Strip query params
        int q = tail.indexOf('?');
        if (q >= 0) {
            tail = tail.substring(0, q);
        }
        return tail;
    }

    /** Load the .conf template from classpath and write it to a temp file. */
    private Path writeConfFile(Map<String, String> variables, MigrationLogger logger) {
        String template;
        try (InputStream is = getClass().getResourceAsStream(TEMPLATE_RESOURCE)) {
            if (is == null) {
                throw new MigrationException(
                        MigrationErrorCode.MIGRATION_ORCHESTRATION_FAILED,
                        "Template not found on classpath: " + TEMPLATE_RESOURCE);
            }
            template = readAll(is);
        } catch (IOException e) {
            throw new MigrationException(
                    MigrationErrorCode.MIGRATION_ORCHESTRATION_FAILED,
                    "Failed to read template: " + TEMPLATE_RESOURCE,
                    e);
        }

        // SeaTunnel CLI uses -i for variable substitution, so we write the template as-is
        // and pass variables as -i key=value flags
        String stamp = LocalDateTime.now().format(STAMP_FMT);
        Path confFile = workDir.resolve("migration_" + stamp + ".conf");
        try {
            Files.write(confFile, template.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new MigrationException(
                    MigrationErrorCode.MIGRATION_ORCHESTRATION_FAILED,
                    "Failed to write conf file: " + confFile,
                    e);
        }
        return confFile;
    }

    /** Resolve the seatunnel executable path. */
    private String resolveSeatunnelBin() {
        if (seatunnelHome != null && !seatunnelHome.isEmpty()) {
            Path bin = Paths.get(seatunnelHome, "bin", "seatunnel");
            if (Files.exists(bin)) {
                return bin.toString();
            }
            // On Windows, try .bat
            Path bat = Paths.get(seatunnelHome, "bin", "seatunnel.bat");
            if (Files.exists(bat)) {
                return bat.toString();
            }
        }
        // Fall back to PATH
        return "seatunnel";
    }

    private static String readAll(InputStream is) throws IOException {
        StringBuilder sb = new StringBuilder();
        try (BufferedReader reader =
                new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line).append('\n');
            }
        }
        return sb.toString();
    }
}
