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

package org.apache.seatunnel.connectors.migration.milvus2pgvector.cdc.metrics;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Generates a structured test report from {@link CdcMetricsCollector} data: results, performance
 * metrics, and problem analysis. Writes both JSON (machine-readable) and a console summary
 * (human-readable).
 *
 * <p>The report aggregates per-scenario metrics (snapshot, incremental insert/update/delete,
 * resilience scenarios), computes pass/fail status against configured thresholds, and lists
 * anomalies detected during the run.
 */
@Slf4j
public class CdcTestReport {

    private final CdcMetricsCollector collector;
    private final String environmentLabel;
    private final Path outputDir;
    private final double similarityThreshold;
    private final double minSuccessRate;
    private final long snapshotThroughputMinRowsPerSec;
    private final long incrementalLatencyP99MaxMs;
    private final List<ScenarioResult> scenarioResults = new ArrayList<>();
    private final List<String> problemAnalysis = new ArrayList<>();

    public CdcTestReport(
            CdcMetricsCollector collector,
            String environmentLabel,
            String outputDir,
            double similarityThreshold,
            double minSuccessRate,
            long snapshotThroughputMinRowsPerSec,
            long incrementalLatencyP99MaxMs) {
        this.collector = collector;
        this.environmentLabel = environmentLabel;
        this.outputDir = Paths.get(outputDir);
        this.similarityThreshold = similarityThreshold;
        this.minSuccessRate = minSuccessRate;
        this.snapshotThroughputMinRowsPerSec = snapshotThroughputMinRowsPerSec;
        this.incrementalLatencyP99MaxMs = incrementalLatencyP99MaxMs;
    }

    /** Finalize and persist the report. */
    public void generate() {
        evaluateScenarios();
        buildProblemAnalysis();
        try {
            writeJsonReport();
        } catch (IOException e) {
            log.warn("Failed to write JSON report: {}", e.getMessage());
        }
        try {
            writeMarkdownReport();
        } catch (IOException e) {
            log.warn("Failed to write Markdown report: {}", e.getMessage());
        }
        log.info("{}", formatConsoleSummary());
    }

    private void evaluateScenarios() {
        for (Map.Entry<String, CdcMetricsCollector.ScenarioMetrics> entry :
                collector.getScenarios().entrySet()) {
            CdcMetricsCollector.ScenarioMetrics m = entry.getValue();
            boolean passed = true;
            List<String> reasons = new ArrayList<>();

            if (m.getSuccessRate() < minSuccessRate) {
                passed = false;
                reasons.add(String.format(
                        "successRate=%.4f < min=%.4f", m.getSuccessRate(), minSuccessRate));
            }
            if (m.getMinSimilarity() < similarityThreshold) {
                passed = false;
                reasons.add(String.format(
                        "minSimilarity=%.6f < threshold=%.6f",
                        m.getMinSimilarity(), similarityThreshold));
            }
            if (m.getName().startsWith("snapshot.") && m.getThroughputRowsPerSec() > 0
                    && m.getThroughputRowsPerSec() < snapshotThroughputMinRowsPerSec) {
                passed = false;
                reasons.add(String.format(
                        "throughput=%.1f rows/s < min=%d rows/s",
                        m.getThroughputRowsPerSec(), snapshotThroughputMinRowsPerSec));
            }
            if (m.getName().startsWith("incremental.") && m.getP99DurationMs() > 0
                    && m.getP99DurationMs() > incrementalLatencyP99MaxMs) {
                passed = false;
                reasons.add(String.format(
                        "p99Latency=%dms > max=%dms",
                        m.getP99DurationMs(), incrementalLatencyP99MaxMs));
            }
            if (m.getTotalActualRows() < m.getTotalExpectedRows()) {
                passed = false;
                reasons.add(String.format(
                        "actualRows=%d < expectedRows=%d (data loss)",
                        m.getTotalActualRows(), m.getTotalExpectedRows()));
            }

            scenarioResults.add(new ScenarioResult(
                    m.getName(),
                    passed,
                    reasons,
                    m.getTotalDurationMs(),
                    m.getP99DurationMs(),
                    m.getThroughputRowsPerSec(),
                    m.getSuccessRate(),
                    m.getP99SyncDelayMs(),
                    m.getAvgSimilarity(),
                    m.getMinSimilarity(),
                    m.getTotalExpectedRows(),
                    m.getTotalActualRows(),
                    m.getTotalFailedRows(),
                    m.getAnomalyCount()));
        }
    }

    private void buildProblemAnalysis() {
        for (Map.Entry<String, CdcMetricsCollector.ScenarioMetrics> entry :
                collector.getScenarios().entrySet()) {
            CdcMetricsCollector.ScenarioMetrics m = entry.getValue();
            for (CdcMetricsCollector.Anomaly a : m.getAnomalies()) {
                problemAnalysis.add(String.format(
                        "[%s] %s @ %d: %s",
                        entry.getKey(), a.getType(), a.getTimestampMs(), a.getDetail()));
            }
        }
        for (ScenarioResult sr : scenarioResults) {
            if (!sr.passed) {
                problemAnalysis.add(String.format(
                        "[%s] FAILED — %s", sr.scenarioName, String.join("; ", sr.failureReasons)));
            }
        }
    }

    private void writeJsonReport() throws IOException {
        Files.createDirectories(outputDir);
        Path file = outputDir.resolve("cdc-e2e-report-" + System.currentTimeMillis() + ".json");
        StringBuilder json = new StringBuilder();
        json.append("{\n");
        json.append("  \"generatedAt\": \"").append(Instant.now()).append("\",\n");
        json.append("  \"environment\": \"").append(escape(environmentLabel)).append("\",\n");
        json.append("  \"thresholds\": {\n");
        json.append("    \"similarityThreshold\": ").append(similarityThreshold).append(",\n");
        json.append("    \"minSuccessRate\": ").append(minSuccessRate).append(",\n");
        json.append("    \"snapshotThroughputMinRowsPerSec\": ")
                .append(snapshotThroughputMinRowsPerSec).append(",\n");
        json.append("    \"incrementalLatencyP99MaxMs\": ")
                .append(incrementalLatencyP99MaxMs).append("\n");
        json.append("  },\n");
        json.append("  \"scenarios\": [\n");
        for (int i = 0; i < scenarioResults.size(); i++) {
            ScenarioResult s = scenarioResults.get(i);
            json.append("    {\n");
            json.append("      \"name\": \"").append(escape(s.scenarioName)).append("\",\n");
            json.append("      \"passed\": ").append(s.passed).append(",\n");
            json.append("      \"failureReasons\": [");
            for (int j = 0; j < s.failureReasons.size(); j++) {
                json.append(j == 0 ? "" : ", ");
                json.append("\"").append(escape(s.failureReasons.get(j))).append("\"");
            }
            json.append("],\n");
            json.append("      \"totalDurationMs\": ").append(s.totalDurationMs).append(",\n");
            json.append("      \"p99LatencyMs\": ").append(s.p99LatencyMs).append(",\n");
            json.append("      \"throughputRowsPerSec\": ")
                    .append(String.format("%.2f", s.throughputRowsPerSec)).append(",\n");
            json.append("      \"successRate\": ")
                    .append(String.format("%.4f", s.successRate)).append(",\n");
            json.append("      \"p99SyncDelayMs\": ").append(s.p99SyncDelayMs).append(",\n");
            json.append("      \"avgSimilarity\": ")
                    .append(String.format("%.6f", s.avgSimilarity)).append(",\n");
            json.append("      \"minSimilarity\": ")
                    .append(String.format("%.6f", s.minSimilarity)).append(",\n");
            json.append("      \"expectedRows\": ").append(s.expectedRows).append(",\n");
            json.append("      \"actualRows\": ").append(s.actualRows).append(",\n");
            json.append("      \"failedRows\": ").append(s.failedRows).append(",\n");
            json.append("      \"anomalyCount\": ").append(s.anomalyCount).append("\n");
            json.append("    }");
            if (i < scenarioResults.size() - 1) {
                json.append(',');
            }
            json.append('\n');
        }
        json.append("  ],\n");
        json.append("  \"problemAnalysis\": [");
        for (int i = 0; i < problemAnalysis.size(); i++) {
            json.append(i == 0 ? "" : ", ");
            json.append("\"").append(escape(problemAnalysis.get(i))).append("\"");
        }
        json.append("]\n");
        json.append("}\n");
        Files.write(file, json.toString().getBytes());
        log.info("CDC E2E report written to {}", file.toAbsolutePath());
    }

    private void writeMarkdownReport() throws IOException {
        Files.createDirectories(outputDir);
        Path file = outputDir.resolve("CDC-E2E-Test-Report.md");
        StringBuilder md = new StringBuilder();

        md.append("# Milvus CDC E2E 测试报告\n\n");
        md.append("## 1. 测试环境\n\n");
        md.append("| 配置项 | 值 |\n");
        md.append("|--------|----|\n");
        md.append("| 环境 | ").append(environmentLabel).append(" |\n");
        md.append("| 生成时间 | ").append(Instant.now()).append(" |\n");
        md.append("| 向量相似度阈值 | ≥ ").append(similarityThreshold).append(" |\n");
        md.append("| 成功率阈值 | ≥ ").append(String.format("%.0f%%", minSuccessRate * 100)).append(" |\n");
        md.append("| 快照吞吐量阈值 | ≥ ").append(snapshotThroughputMinRowsPerSec).append(" rows/s |\n");
        md.append("| 增量延迟P99阈值 | ≤ ").append(incrementalLatencyP99MaxMs).append("ms |\n\n");

        md.append("## 2. 测试场景汇总\n\n");
        md.append("| 场景名称 | 状态 | 耗时 | 吞吐量 | 成功率 | P99延迟 | P99同步延迟 | 期望行数 | 实际行数 | 失败行数 | 异常数 |\n");
        md.append("|----------|------|------|--------|--------|---------|-------------|----------|----------|----------|--------|\n");
        for (ScenarioResult s : scenarioResults) {
            md.append("| ").append(s.scenarioName).append(" | ");
            md.append(s.passed ? "✅ PASS" : "❌ FAIL").append(" | ");
            md.append(s.totalDurationMs).append("ms | ");
            md.append(String.format("%.1f", s.throughputRowsPerSec)).append("/s | ");
            md.append(String.format("%.0f%%", s.successRate * 100)).append(" | ");
            md.append(s.p99LatencyMs).append("ms | ");
            md.append(s.p99SyncDelayMs).append("ms | ");
            md.append(s.expectedRows).append(" | ");
            md.append(s.actualRows).append(" | ");
            md.append(s.failedRows).append(" | ");
            md.append(s.anomalyCount).append(" |\n");
        }
        md.append("\n");

        md.append("## 3. 向量相似度分析\n\n");
        md.append("| 场景名称 | 平均相似度 | 最小相似度 |\n");
        md.append("|----------|------------|------------|\n");
        for (ScenarioResult s : scenarioResults) {
            md.append("| ").append(s.scenarioName).append(" | ");
            md.append(String.format("%.6f", s.avgSimilarity)).append(" | ");
            md.append(String.format("%.6f", s.minSimilarity)).append(" |\n");
        }
        md.append("\n");

        md.append("## 4. 问题分析\n\n");
        if (problemAnalysis.isEmpty()) {
            md.append("所有测试场景均通过阈值检查，无异常记录。\n\n");
        } else {
            md.append("共记录 **").append(problemAnalysis.size()).append("** 个异常/限制说明：\n\n");
            for (String p : problemAnalysis) {
                md.append("- ").append(p).append("\n");
            }
            md.append("\n");
        }

        md.append("## 5. 策略说明\n\n");
        md.append("### 5.1 PollingIncrementalCdcStrategy\n\n");
        md.append("基于主键水位线的轮询策略：\n");
        md.append("- 使用 `id > watermark` 表达式查询增量数据\n");
        md.append("- **限制**：无法检测删除操作（已删除的行不会出现在查询结果中）\n");
        md.append("- **限制**：无法检测同主键更新（更新不改变主键值，`id > watermark` 无法匹配）\n");
        md.append("- 适用场景：纯插入型数据流，或配合外部全量扫描补偿机制\n\n");

        md.append("### 5.2 CdcEventStreamStrategyV2\n\n");
        md.append("基于 StreamingNode gRPC 的推荐策略实现：\n");
        md.append("- 通过 Consume RPC 直接访问 Milvus WAL\n");
        md.append("- **优势**：完整支持 INSERT/DELETE/UPSERT 语义\n");
        md.append("- 支持 standalone 和 cluster 部署模式\n\n");

        md.append("## 6. 结论\n\n");
        int passedCount = 0;
        for (ScenarioResult s : scenarioResults) {
            if (s.passed) passedCount++;
        }
        md.append("- 测试场景总数：").append(scenarioResults.size()).append("\n");
        md.append("- 通过场景数：").append(passedCount).append("\n");
        md.append("- 失败场景数：").append(scenarioResults.size() - passedCount).append("\n");
        if (passedCount == scenarioResults.size()) {
            md.append("- **结论**：所有测试场景通过 ✅\n");
        } else {
            md.append("- **结论**：部分场景未通过阈值检查，请查看问题分析章节 ❌\n");
        }
        md.append("\n---\n");
        md.append("*报告自动生成于 ").append(Instant.now()).append("*\n");

        Files.write(file, md.toString().getBytes());
        log.info("CDC E2E Markdown report written to {}", file.toAbsolutePath());
    }

    public String formatConsoleSummary() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n==================== CDC E2E Test Report ====================\n");
        sb.append("Environment     : ").append(environmentLabel).append('\n');
        sb.append("Generated At    : ").append(Instant.now()).append('\n');
        sb.append("Thresholds      : similarity=").append(similarityThreshold)
                .append(", successRate>=").append(minSuccessRate)
                .append(", snapshotThroughput>=").append(snapshotThroughputMinRowsPerSec)
                .append(" rows/s, incrementalP99<=").append(incrementalLatencyP99MaxMs).append("ms\n");
        sb.append("--------------------------------------------------------------\n");
        sb.append(String.format("%-22s %-6s %10s %10s %10s %8s %10s%n",
                "Scenario", "Status", "Duration", "Throughput", "Success%", "P99Lat", "P99Delay"));
        for (ScenarioResult s : scenarioResults) {
            sb.append(String.format("%-22s %-6s %9dms %8.1f/s %9.2f%% %7dms %9dms%n",
                    s.scenarioName,
                    s.passed ? "PASS" : "FAIL",
                    s.totalDurationMs,
                    s.throughputRowsPerSec,
                    s.successRate * 100,
                    s.p99LatencyMs,
                    s.p99SyncDelayMs));
        }
        sb.append("--------------------------------------------------------------\n");
        sb.append("Vector Similarity:\n");
        for (ScenarioResult s : scenarioResults) {
            sb.append(String.format("  %-22s avg=%.6f, min=%.6f%n",
                    s.scenarioName, s.avgSimilarity, s.minSimilarity));
        }
        sb.append("--------------------------------------------------------------\n");
        if (problemAnalysis.isEmpty()) {
            sb.append("Problem Analysis: No issues detected.\n");
        } else {
            sb.append("Problem Analysis (").append(problemAnalysis.size()).append(" issues):\n");
            for (String p : problemAnalysis) {
                sb.append("  • ").append(p).append('\n');
            }
        }
        sb.append("==============================================================\n");
        return sb.toString();
    }

    public List<ScenarioResult> getScenarioResults() {
        return scenarioResults;
    }

    public List<String> getProblemAnalysis() {
        return problemAnalysis;
    }

    /** Result of evaluating one scenario against thresholds. */
    public static class ScenarioResult {
        private final String scenarioName;
        private final boolean passed;
        private final List<String> failureReasons;
        private final long totalDurationMs;
        private final long p99LatencyMs;
        private final double throughputRowsPerSec;
        private final double successRate;
        private final long p99SyncDelayMs;
        private final double avgSimilarity;
        private final double minSimilarity;
        private final long expectedRows;
        private final long actualRows;
        private final long failedRows;
        private final int anomalyCount;

        public ScenarioResult(
                String scenarioName,
                boolean passed,
                List<String> failureReasons,
                long totalDurationMs,
                long p99LatencyMs,
                double throughputRowsPerSec,
                double successRate,
                long p99SyncDelayMs,
                double avgSimilarity,
                double minSimilarity,
                long expectedRows,
                long actualRows,
                long failedRows,
                int anomalyCount) {
            this.scenarioName = scenarioName;
            this.passed = passed;
            this.failureReasons = failureReasons;
            this.totalDurationMs = totalDurationMs;
            this.p99LatencyMs = p99LatencyMs;
            this.throughputRowsPerSec = throughputRowsPerSec;
            this.successRate = successRate;
            this.p99SyncDelayMs = p99SyncDelayMs;
            this.avgSimilarity = avgSimilarity;
            this.minSimilarity = minSimilarity;
            this.expectedRows = expectedRows;
            this.actualRows = actualRows;
            this.failedRows = failedRows;
            this.anomalyCount = anomalyCount;
        }

        public String getScenarioName() {
            return scenarioName;
        }

        public boolean isPassed() {
            return passed;
        }

        public List<String> getFailureReasons() {
            return failureReasons;
        }

        public long getTotalDurationMs() {
            return totalDurationMs;
        }

        public long getP99LatencyMs() {
            return p99LatencyMs;
        }

        public double getThroughputRowsPerSec() {
            return throughputRowsPerSec;
        }

        public double getSuccessRate() {
            return successRate;
        }

        public long getP99SyncDelayMs() {
            return p99SyncDelayMs;
        }

        public double getAvgSimilarity() {
            return avgSimilarity;
        }

        public double getMinSimilarity() {
            return minSimilarity;
        }

        public long getExpectedRows() {
            return expectedRows;
        }

        public long getActualRows() {
            return actualRows;
        }

        public long getFailedRows() {
            return failedRows;
        }

        public int getAnomalyCount() {
            return anomalyCount;
        }
    }

    private static String escape(String s) {
        if (s == null) {
            return "";
        }
        return s.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r");
    }
}
