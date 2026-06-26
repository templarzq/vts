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

import org.apache.seatunnel.connectors.migration.milvus2pgvector.config.MigrationConfig;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.exception.MigrationErrorCode;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.exception.MigrationException;

import lombok.Getter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/** Manual CLI argument parser for the Milvus → pgvector migration tool. */
@Getter
public class MigrationCliArgs {

    // ---- Milvus source ----
    private String milvusUrl;
    private String milvusToken = "";
    private String milvusDatabase = "default";
    private String milvusCollection;

    // ---- pgvector target ----
    private String pgUrl;
    private String pgUser;
    private String pgPassword;
    private String pgSchema = "public";
    private String pgTable;

    // ---- performance ----
    private int batchSize = 1000;
    private int parallelism = 1;
    private int rateLimit = 0;

    // ---- behavior ----
    private boolean skipIndexMigration = false;
    private boolean dropExistingTable = false;
    private boolean noPrecisionLoss = false;

    // ---- phase control ----
    private boolean validateOnly = false;
    private boolean schemaOnly = false;
    private boolean dataOnly = false;
    private boolean resume = false;

    // ---- validation ----
    private int sampleSize = 100;
    private double similarityThreshold = 0.9999;

    // ---- audit ----
    private String logDir = "./migration-logs";

    // ---- seatunnel ----
    private String seatunnelHome;

    // ---- config file ----
    private String config;

    // ---- help ----
    private boolean help = false;

    /** All recognized boolean flags (no value expected). */
    private static final Map<String, String> FLAGS = new HashMap<>();

    static {
        FLAGS.put("--skip-index-migration", "skipIndexMigration");
        FLAGS.put("--drop-existing-table", "dropExistingTable");
        FLAGS.put("--no-precision-loss", "noPrecisionLoss");
        FLAGS.put("--validate-only", "validateOnly");
        FLAGS.put("--schema-only", "schemaOnly");
        FLAGS.put("--data-only", "dataOnly");
        FLAGS.put("--resume", "resume");
        FLAGS.put("--help", "help");
    }

    /** Parse command-line arguments into a MigrationCliArgs instance. */
    public static MigrationCliArgs parse(String[] args) {
        MigrationCliArgs cli = new MigrationCliArgs();
        // Start with environment variable for seatunnel home
        cli.seatunnelHome = System.getenv("SEATUNNEL_HOME");

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];

            if (FLAGS.containsKey(arg)) {
                setFlag(cli, FLAGS.get(arg));
                continue;
            }

            String value = null;
            if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                value = args[i + 1];
                i++;
            }

            switch (arg) {
                case "--milvus-url":
                    cli.milvusUrl = value;
                    break;
                case "--milvus-token":
                    cli.milvusToken = value != null ? value : "";
                    break;
                case "--milvus-database":
                    cli.milvusDatabase = value != null ? value : "default";
                    break;
                case "--milvus-collection":
                    cli.milvusCollection = value;
                    break;
                case "--pg-url":
                    cli.pgUrl = value;
                    break;
                case "--pg-user":
                    cli.pgUser = value;
                    break;
                case "--pg-password":
                    cli.pgPassword = value;
                    break;
                case "--pg-schema":
                    cli.pgSchema = value != null ? value : "public";
                    break;
                case "--pg-table":
                    cli.pgTable = value;
                    break;
                case "--batch-size":
                    cli.batchSize = parseInt(arg, value, 1000);
                    break;
                case "--parallelism":
                    cli.parallelism = parseInt(arg, value, 1);
                    break;
                case "--rate-limit":
                    cli.rateLimit = parseInt(arg, value, 0);
                    break;
                case "--log-dir":
                    cli.logDir = value != null ? value : "./migration-logs";
                    break;
                case "--sample-size":
                    cli.sampleSize = parseInt(arg, value, 100);
                    break;
                case "--similarity-threshold":
                    cli.similarityThreshold = parseDouble(arg, value, 0.9999);
                    break;
                case "--seatunnel-home":
                    cli.seatunnelHome = value;
                    break;
                case "--config":
                    cli.config = value;
                    break;
                default:
                    throw new MigrationException(
                            MigrationErrorCode.MIGRATION_CONFIG_INVALID,
                            "Unknown argument: " + arg + ". Use --help for usage.");
            }
        }

        // If --config is specified, load it (CLI args override file values)
        if (cli.config != null) {
            cli.loadConfigFile();
        }

        return cli;
    }

    private static void setFlag(MigrationCliArgs cli, String fieldName) {
        try {
            MigrationCliArgs.class.getDeclaredField(fieldName).setBoolean(cli, true);
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new MigrationException(
                    MigrationErrorCode.MIGRATION_CONFIG_INVALID,
                    "Internal error: cannot set flag " + fieldName,
                    e);
        }
    }

    private static int parseInt(String arg, String value, int defaultValue) {
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new MigrationException(
                    MigrationErrorCode.MIGRATION_CONFIG_INVALID,
                    "Invalid integer for " + arg + ": " + value);
        }
    }

    private static double parseDouble(String arg, String value, double defaultValue) {
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            throw new MigrationException(
                    MigrationErrorCode.MIGRATION_CONFIG_INVALID,
                    "Invalid double for " + arg + ": " + value);
        }
    }

    /**
     * Load configuration from a properties file. CLI args that were explicitly set take precedence.
     * The file format is {@code key=value} per line, with keys matching the CLI option names without
     * the {@code --} prefix (e.g. {@code milvus.url=...}).
     */
    private void loadConfigFile() {
        Properties props = new Properties();
        try {
            Path configPath = Paths.get(config);
            if (!Files.exists(configPath)) {
                throw new MigrationException(
                        MigrationErrorCode.MIGRATION_CONFIG_INVALID,
                        "Config file not found: " + config);
            }
            props.load(Files.newBufferedReader(configPath));
        } catch (IOException e) {
            throw new MigrationException(
                    MigrationErrorCode.MIGRATION_CONFIG_INVALID,
                    "Failed to read config file: " + config,
                    e);
        }

        // Only apply file values for fields not already set via CLI
        if (milvusUrl == null) {
            milvusUrl = props.getProperty("milvus.url", milvusUrl);
        }
        if (milvusToken == null || milvusToken.isEmpty()) {
            milvusToken = props.getProperty("milvus.token", "");
        }
        if (milvusDatabase == null || milvusDatabase.equals("default")) {
            milvusDatabase = props.getProperty("milvus.database", "default");
        }
        if (milvusCollection == null) {
            milvusCollection = props.getProperty("milvus.collection");
        }
        if (pgUrl == null) {
            pgUrl = props.getProperty("pg.url");
        }
        if (pgUser == null) {
            pgUser = props.getProperty("pg.user");
        }
        if (pgPassword == null) {
            pgPassword = props.getProperty("pg.password");
        }
        if (pgSchema == null || pgSchema.equals("public")) {
            pgSchema = props.getProperty("pg.schema", "public");
        }
        if (pgTable == null) {
            pgTable = props.getProperty("pg.table");
        }
        if (seatunnelHome == null) {
            seatunnelHome = props.getProperty("seatunnel.home", System.getenv("SEATUNNEL_HOME"));
        }
        if (logDir.equals("./migration-logs")) {
            logDir = props.getProperty("audit.log_dir", "./migration-logs");
        }
    }

    /** Validate that required arguments are present. */
    public void validate() {
        StringBuilder errors = new StringBuilder();
        if (milvusUrl == null || milvusUrl.isEmpty()) {
            errors.append("  --milvus-url is required\n");
        }
        if (milvusCollection == null || milvusCollection.isEmpty()) {
            errors.append("  --milvus-collection is required\n");
        }
        if (pgUrl == null || pgUrl.isEmpty()) {
            errors.append("  --pg-url is required\n");
        }
        if (pgUser == null || pgUser.isEmpty()) {
            errors.append("  --pg-user is required\n");
        }
        if (pgPassword == null) {
            errors.append("  --pg-password is required\n");
        }
        if (schemaOnly && dataOnly) {
            errors.append("  --schema-only and --data-only are mutually exclusive\n");
        }
        if (validateOnly && (schemaOnly || dataOnly)) {
            errors.append("  --validate-only cannot be combined with --schema-only or --data-only\n");
        }
        if (errors.length() > 0) {
            throw new MigrationException(
                    MigrationErrorCode.MIGRATION_CONFIG_INVALID,
                    "Invalid configuration:\n" + errors);
        }
    }

    /** Convert to {@link MigrationConfig}. pgTable defaults to collection name if not set. */
    public MigrationConfig toMigrationConfig() {
        String effectivePgTable = (pgTable == null || pgTable.isEmpty()) ? milvusCollection : pgTable;
        return MigrationConfig.builder()
                .milvusUrl(milvusUrl)
                .milvusToken(milvusToken)
                .milvusDatabase(milvusDatabase)
                .milvusCollection(milvusCollection)
                .pgUrl(pgUrl)
                .pgUser(pgUser)
                .pgPassword(pgPassword)
                .pgSchema(pgSchema)
                .pgTable(effectivePgTable)
                .batchSize(batchSize)
                .parallelism(parallelism)
                .rateLimitRowsPerSecond(rateLimit)
                .skipIndexMigration(skipIndexMigration)
                .dropExistingTable(dropExistingTable)
                .allowPrecisionLoss(!noPrecisionLoss)
                .validationSampleSize(sampleSize)
                .similarityThreshold(similarityThreshold)
                .auditLogDir(logDir)
                .build();
    }

    public boolean shouldRunSchema() {
        return !dataOnly && !validateOnly;
    }

    public boolean shouldRunData() {
        return !schemaOnly && !validateOnly;
    }

    public boolean shouldRunValidation() {
        return !schemaOnly && !dataOnly;
    }

    /** Return the usage/help text. */
    public static String usage() {
        return "Usage: MigrationCli [options]\n"
                + "\n"
                + "Required:\n"
                + "  --milvus-url <url>            Milvus connection URL (e.g. http://localhost:19530)\n"
                + "  --milvus-collection <name>    Milvus collection to migrate\n"
                + "  --pg-url <jdbc-url>           PostgreSQL JDBC URL (e.g. jdbc:postgresql://localhost:5432/db)\n"
                + "  --pg-user <user>              PostgreSQL user\n"
                + "  --pg-password <password>      PostgreSQL password\n"
                + "\n"
                + "Optional:\n"
                + "  --milvus-token <token>        Milvus auth token (default: empty)\n"
                + "  --milvus-database <db>        Milvus database (default: default)\n"
                + "  --pg-schema <schema>          PostgreSQL schema (default: public)\n"
                + "  --pg-table <table>            PostgreSQL table (default: same as collection)\n"
                + "  --batch-size <n>              Batch size (default: 1000)\n"
                + "  --parallelism <n>             Parallelism (default: 1)\n"
                + "  --rate-limit <rows/s>         Max rows per second, 0=unlimited (default: 0)\n"
                + "  --skip-index-migration        Skip creating vector indexes on pgvector\n"
                + "  --drop-existing-table         Drop target table before migration\n"
                + "  --no-precision-loss           Forbid BFloat16 → halfvec precision loss\n"
                + "  --validate-only               Only run validation (skip schema + data)\n"
                + "  --schema-only                 Only run schema migration\n"
                + "  --data-only                   Only run data migration\n"
                + "  --resume                      Resume from last checkpoint (skip completed phases)\n"
                + "  --log-dir <dir>               Log/audit directory (default: ./migration-logs)\n"
                + "  --sample-size <n>             Validation sample size (default: 100)\n"
                + "  --similarity-threshold <d>    Cosine similarity threshold (default: 0.9999)\n"
                + "  --seatunnel-home <dir>        SeaTunnel install dir (default: $SEATUNNEL_HOME)\n"
                + "  --config <file>               Properties config file (CLI args override)\n"
                + "  --help                        Print this help message\n";
    }
}
