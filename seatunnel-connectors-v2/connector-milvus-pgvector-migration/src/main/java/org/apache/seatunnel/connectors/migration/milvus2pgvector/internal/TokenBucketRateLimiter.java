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

package org.apache.seatunnel.connectors.migration.milvus2pgvector.internal;

import org.apache.seatunnel.connectors.migration.milvus2pgvector.exception.MigrationErrorCode;
import org.apache.seatunnel.connectors.migration.milvus2pgvector.exception.MigrationException;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Classic token-bucket rate limiter. Tokens replenish continuously at {@code ratePerSecond}. A
 * caller requests {@code permits} tokens; if enough are available they are consumed immediately,
 * otherwise the call blocks until enough tokens have accumulated or the acquire timeout expires.
 *
 * <p>Thread-safe. Use {@code ratePerSecond <= 0} to disable rate limiting (all acquires succeed
 * instantly).
 */
@Slf4j
public class TokenBucketRateLimiter {

    private final long ratePerSecond;
    private final long acquireTimeoutNanos;
    private final ReentrantLock lock = new ReentrantLock(true);
    private final Condition tokensAvailable = lock.newCondition();

    private double availableTokens;
    private long lastRefillNanos;

    public TokenBucketRateLimiter(long ratePerSecond, int acquireTimeoutSeconds) {
        this.ratePerSecond = ratePerSecond;
        this.acquireTimeoutNanos = TimeUnit.SECONDS.toNanos(acquireTimeoutSeconds);
        this.availableTokens = ratePerSecond;
        this.lastRefillNanos = System.nanoTime();
    }

    /**
     * Acquire {@code permits} tokens, blocking up to the configured timeout.
     *
     * @throws MigrationException if the timeout expires before enough tokens are available
     */
    public void acquire(int permits) {
        if (ratePerSecond <= 0) {
            return;
        }
        lock.lock();
        try {
            long deadline = System.nanoTime() + acquireTimeoutNanos;
            while (true) {
                refill();
                if (availableTokens >= permits) {
                    availableTokens -= permits;
                    return;
                }
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    throw new MigrationException(
                            MigrationErrorCode.RATE_LIMIT_EXCEEDED,
                            "Timed out waiting for " + permits + " permits (rate=" + ratePerSecond
                                    + "/s, available=" + availableTokens + ")");
                }
                long waitNanos = (long) ((permits - availableTokens) / ratePerSecond * 1_000_000_000L);
                waitNanos = Math.min(waitNanos, remaining);
                try {
                    tokensAvailable.awaitNanos(waitNanos);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new MigrationException(
                            MigrationErrorCode.RATE_LIMIT_EXCEEDED,
                            "Interrupted while waiting for rate-limit permits", e);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Try to acquire {@code permits} tokens without blocking.
     *
     * @return true if permits were acquired, false if insufficient tokens
     */
    public boolean tryAcquire(int permits) {
        if (ratePerSecond <= 0) {
            return true;
        }
        lock.lock();
        try {
            refill();
            if (availableTokens >= permits) {
                availableTokens -= permits;
                return true;
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    /** Current number of available tokens (after refill). For testing/monitoring. */
    public double getAvailableTokens() {
        lock.lock();
        try {
            refill();
            return availableTokens;
        } finally {
            lock.unlock();
        }
    }

    private void refill() {
        long now = System.nanoTime();
        long elapsed = now - lastRefillNanos;
        if (elapsed > 0) {
            double newTokens = elapsed / 1_000_000_000.0 * ratePerSecond;
            availableTokens = Math.min(availableTokens + newTokens, ratePerSecond);
            lastRefillNanos = now;
        }
    }
}
