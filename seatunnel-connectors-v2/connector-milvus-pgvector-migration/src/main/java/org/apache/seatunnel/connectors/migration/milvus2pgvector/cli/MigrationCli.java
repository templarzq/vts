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
import org.apache.seatunnel.connectors.migration.milvus2pgvector.audit.MigrationProgressTracker;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.audit.MigrationReport;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.config.MigrationConfig;

import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.nio.file.Paths;

/** CLI entry point for the Milvus → pgvector migration tool. */
@Slf4j
public class MigrationCli {

    public static void main(String[] args) {
        try {
            run(args);
        } catch (Exception e) {
            log.error("Migration failed", e);
            System.err.println("ERROR: " + e.getMessage());
            System.exit(1);
        }
    }

    private static void run(String[] args) {
        MigrationCliArgs cliArgs = MigrationCliArgs.parse(args);

        if (cliArgs.isHelp()) {
            System.out.println(MigrationCliArgs.usage());
            return;
        }

        cliArgs.validate();

        MigrationConfig config = cliArgs.toMigrationConfig();
        Path logDir = Paths.get(config.getAuditLogDir());

        try (MigrationLogger logger = new MigrationLogger(logDir, config.getMilvusCollection());
                MigrationProgressTracker tracker =
                        new MigrationProgressTracker(logDir, config.getMilvusCollection())) {

            SeaTunnelJobSubmitter submitter = null;
            if (cliArgs.shouldRunData()) {
                submitter = new SeaTunnelJobSubmitter(cliArgs.getSeatunnelHome(), logDir);
            }

            MigrationOrchestrator orchestrator =
                    new MigrationOrchestrator(
                            config,
                            logger,
                            tracker,
                            submitter,
                            cliArgs.isResume(),
                            cliArgs.shouldRunSchema(),
                            cliArgs.shouldRunData(),
                            cliArgs.shouldRunValidation());

            MigrationReport report = orchestrator.run();

            System.out.println(report.formatConsole());

            Path reportFile =
                    logDir.resolve(config.getMilvusCollection() + "_report.json");
            report.writeToFile(reportFile);
            logger.info("Report written to " + reportFile);

            System.exit(report.isSuccess() ? 0 : 1);
        }
    }
}
