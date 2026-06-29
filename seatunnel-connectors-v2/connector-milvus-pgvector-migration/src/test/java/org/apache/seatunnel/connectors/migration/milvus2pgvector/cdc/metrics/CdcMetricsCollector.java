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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Collects quantitative metrics during CDC E2E test execution: per-operation latency, throughput,
 * success/failure counts, and synchronization delay. Thread-safe.
 *
 * <p>Each test phase records its metrics under a named scenario key (e.g. {@code
 * "snapshot.small"}, {@code "incremental.insert"}). The {@link CdcTestReport} consumes the
 * aggregated snapshot at the end of the run.
 */
@Slf4j
public class CdcMetricsCollector {

    private final ConcurrentMap<String, ScenarioMetrics> scenarios = new ConcurrentHashMap<>();
    private final AtomicLong globalStartTime = new AtomicLong(0L);

    /** Start the global test clock. */
    public void start() {
        globalStartTime.compareAndSet(0L, System.currentTimeMillis());
    }

    /** Begin timing a named scenario. Returns a handle that must be closed to record duration. */
    public ScenarioTimer startScenario(String scenarioName, int expectedRows) {
        long start = System.currentTimeMillis();
        return new ScenarioTimer(scenarioName, start, expectedRows);
    }

    /** Record a completed scenario with explicit timing and counts. */
    public void recordScenario(
            String scenarioName,
            long durationMs,
            int expectedRows,
            int actualRows,
            int failedRows,
            long sourceWriteTimestamp,
            long sinkConfirmTimestamp) {
        ScenarioMetrics m =
                scenarios.computeIfAbsent(scenarioName, k -> new ScenarioMetrics(scenarioName));
        m.recordRun(durationMs, expectedRows, actualRows, failedRows);
        if (sourceWriteTimestamp > 0 && sinkConfirmTimestamp > 0) {
            m.recordSyncDelay(sinkConfirmTimestamp - sourceWriteTimestamp);
        }
        log.info(
                "[{}] duration={}ms, expected={}, actual={}, failed={}, syncDelayMs={}",
                scenarioName,
                durationMs,
                expectedRows,
                actualRows,
                failedRows,
                sourceWriteTimestamp > 0 ? sinkConfirmTimestamp - sourceWriteTimestamp : -1);
    }

    /** Record a malformed-data or network-error event for problem analysis. */
    public void recordAnomaly(String scenarioName, String anomalyType, String detail) {
        ScenarioMetrics m =
                scenarios.computeIfAbsent(scenarioName, k -> new ScenarioMetrics(scenarioName));
        m.recordAnomaly(anomalyType, detail);
        log.warn("[{}] anomaly: {} — {}", scenarioName, anomalyType, detail);
    }

    /** Record a vector similarity sample result. */
    public void recordSimilarity(String scenarioName, double similarity) {
        ScenarioMetrics m =
                scenarios.computeIfAbsent(scenarioName, k -> new ScenarioMetrics(scenarioName));
        m.recordSimilarity(similarity);
    }

    public ConcurrentMap<String, ScenarioMetrics> getScenarios() {
        return scenarios;
    }

    public long getGlobalStartTime() {
        return globalStartTime.get();
    }

    /** Auto-closeable timer for try-with-resources usage. */
    public final class ScenarioTimer implements AutoCloseable {
        private final String name;
        private final long startMs;
        private final int expectedRows;
        private int actualRows;
        private int failedRows;
        private long sourceWriteTs;
        private long sinkConfirmTs;

        ScenarioTimer(String name, long startMs, int expectedRows) {
            this.name = name;
            this.startMs = startMs;
            this.expectedRows = expectedRows;
        }

        public ScenarioTimer actualRows(int n) {
            this.actualRows = n;
            return this;
        }

        public ScenarioTimer failedRows(int n) {
            this.failedRows = n;
            return this;
        }

        public ScenarioTimer syncDelay(long sourceWriteTs, long sinkConfirmTs) {
            this.sourceWriteTs = sourceWriteTs;
            this.sinkConfirmTs = sinkConfirmTs;
            return this;
        }

        @Override
        public void close() {
            long duration = System.currentTimeMillis() - startMs;
            recordScenario(name, duration, expectedRows, actualRows, failedRows,
                    sourceWriteTs, sinkConfirmTs);
        }
    }

    /** Per-scenario aggregated metrics. */
    public static class ScenarioMetrics {
        private final String name;
        private final List<Long> durations = Collections.synchronizedList(new ArrayList<>());
        private final List<Long> syncDelays = Collections.synchronizedList(new ArrayList<>());
        private final List<Double> similarities = Collections.synchronizedList(new ArrayList<>());
        private final List<Anomaly> anomalies = Collections.synchronizedList(new ArrayList<>());
        private final AtomicLong totalExpected = new AtomicLong(0);
        private final AtomicLong totalActual = new AtomicLong(0);
        private final AtomicLong totalFailed = new AtomicLong(0);
        private final AtomicLong runs = new AtomicLong(0);

        ScenarioMetrics(String name) {
            this.name = name;
        }

        void recordRun(long durationMs, int expected, int actual, int failed) {
            durations.add(durationMs);
            totalExpected.addAndGet(expected);
            totalActual.addAndGet(actual);
            totalFailed.addAndGet(failed);
            runs.incrementAndGet();
        }

        void recordSyncDelay(long delayMs) {
            syncDelays.add(delayMs);
        }

        void recordSimilarity(double similarity) {
            similarities.add(similarity);
        }

        void recordAnomaly(String type, String detail) {
            anomalies.add(new Anomaly(type, detail, System.currentTimeMillis()));
        }

        public String getName() {
            return name;
        }

        public long getTotalDurationMs() {
            return durations.stream().mapToLong(Long::longValue).sum();
        }

        public long getMinDurationMs() {
            return durations.stream().mapToLong(Long::longValue).min().orElse(0);
        }

        public long getMaxDurationMs() {
            return durations.stream().mapToLong(Long::longValue).max().orElse(0);
        }

        public double getAvgDurationMs() {
            return durations.stream().mapToLong(Long::longValue).average().orElse(0);
        }

        public long getP99DurationMs() {
            return percentile(durations, 99);
        }

        public long getTotalExpectedRows() {
            return totalExpected.get();
        }

        public long getTotalActualRows() {
            return totalActual.get();
        }

        public long getTotalFailedRows() {
            return totalFailed.get();
        }

        public long getRunCount() {
            return runs.get();
        }

        public double getSuccessRate() {
            long expected = totalExpected.get();
            if (expected == 0) {
                return 1.0;
            }
            return 1.0 - ((double) totalFailed.get() / expected);
        }

        public double getThroughputRowsPerSec() {
            long totalDuration = getTotalDurationMs();
            if (totalDuration == 0) {
                return 0;
            }
            return (totalActual.get() * 1000.0) / totalDuration;
        }

        public long getP99SyncDelayMs() {
            return percentileLong(syncDelays, 99);
        }

        public double getAvgSimilarity() {
            return similarities.stream().mapToDouble(Double::doubleValue).average().orElse(1.0);
        }

        public double getMinSimilarity() {
            return similarities.stream().mapToDouble(Double::doubleValue).min().orElse(1.0);
        }

        public List<Anomaly> getAnomalies() {
            return new ArrayList<>(anomalies);
        }

        public int getAnomalyCount() {
            return anomalies.size();
        }

        private static long percentile(List<Long> values, int p) {
            return percentileLong(values, p);
        }

        private static long percentileLong(List<Long> values, int p) {
            if (values.isEmpty()) {
                return 0;
            }
            List<Long> copy = new ArrayList<>(values);
            Collections.sort(copy);
            int idx = (int) Math.ceil((p / 100.0) * copy.size()) - 1;
            idx = Math.max(0, Math.min(idx, copy.size() - 1));
            return copy.get(idx);
        }
    }

    /** Single anomaly event for problem analysis. */
    public static class Anomaly {
        private final String type;
        private final String detail;
        private final long timestampMs;

        Anomaly(String type, String detail, long timestampMs) {
            this.type = type;
            this.detail = detail;
            this.timestampMs = timestampMs;
        }

        public String getType() {
            return type;
        }

        public String getDetail() {
            return detail;
        }

        public long getTimestampMs() {
            return timestampMs;
        }
    }
}
