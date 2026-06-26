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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TokenBucketRateLimiterTest {

    @Test
    public void testDisabledRateLimitAllowsAllImmediately() {
        // ratePerSecond <= 0 disables limiting
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(0, 1);
        long start = System.nanoTime();
        for (int i = 0; i < 1000; i++) {
            limiter.acquire(1);
        }
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertTrue(elapsedMs < 100, "Disabled limiter should not block; took " + elapsedMs + "ms");
        assertTrue(limiter.tryAcquire(1000));
    }

    @Test
    public void testInitialBurstEqualsRate() {
        // A freshly created limiter has `ratePerSecond` tokens available, so the first batch of
        // that size should be acquirable without blocking.
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(100, 5);
        long start = System.nanoTime();
        for (int i = 0; i < 100; i++) {
            limiter.acquire(1);
        }
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertTrue(
                elapsedMs < 200,
                "Initial burst of 100 tokens at rate=100/s should complete near-instantly; took "
                        + elapsedMs
                        + "ms");
    }

    @Test
    public void testTryAcquireReturnsFalseWhenInsufficient() throws InterruptedException {
        // rate=10/s, initial 10 tokens available. Consume all 10 immediately.
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(10, 5);
        assertTrue(limiter.tryAcquire(10), "Initial 10 tokens should be acquirable");
        assertFalse(limiter.tryAcquire(1), "No tokens left, tryAcquire should return false");
        // Wait long enough for at least 1 token to replenish (100ms at 10/s)
        Thread.sleep(150);
        assertTrue(limiter.tryAcquire(1), "After 150ms at least 1 token should be available");
    }

    @Test
    public void testAcquireBlocksUntilTokensRefill() {
        // rate=20/s → one token every 50ms. After consuming the initial burst, the next acquire(1)
        // should block ~50ms.
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(20, 5);
        // Consume initial burst
        limiter.tryAcquire(20);
        long start = System.nanoTime();
        limiter.acquire(1);
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertTrue(
                elapsedMs >= 30,
                "acquire(1) after draining should block ~50ms; took " + elapsedMs + "ms");
    }

    @Test
    public void testAcquireTimeoutThrowsMigrationException() {
        // rate=1/s, initial 1 token. Consume it, then try to acquire 10 with a 1s timeout — should
        // throw because 10 tokens would take 10s to accumulate.
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(1, 1);
        limiter.tryAcquire(1);
        MigrationException ex =
                assertThrows(
                        MigrationException.class,
                        () -> limiter.acquire(10),
                        "acquire beyond capacity within timeout should throw");
        assertEquals(
                MigrationErrorCode.RATE_LIMIT_EXCEEDED.getCode(),
                ex.getSeaTunnelErrorCode().getCode());
    }

    @Test
    public void testAcquireTimeoutWithZeroAcquireTimeout() {
        // acquireTimeoutSeconds=0 → deadline is "now", so any blocking acquire should fail
        // immediately.
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(1, 0);
        limiter.tryAcquire(1); // drain initial token
        assertThrows(MigrationException.class, () -> limiter.acquire(1));
    }

    @Test
    public void testGetAvailableTokensDoesNotExceedRate() throws InterruptedException {
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(5, 5);
        // Drain all tokens
        limiter.tryAcquire(5);
        assertEquals(0.0, limiter.getAvailableTokens(), 0.01);
        // Wait for refill; cap should be the rate (5)
        Thread.sleep(500);
        double available = limiter.getAvailableTokens();
        assertTrue(
                available <= 5.0 + 0.01,
                "Available tokens should not exceed rate (5); got " + available);
        assertTrue(available > 0.0, "After 500ms at 5/s, tokens should have refilled; got " + available);
    }

    @Test
    public void testTryAcquireLargePermitFailsGracefully() {
        // Requesting more permits than the rate capacity should always fail with tryAcquire.
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(10, 5);
        assertFalse(limiter.tryAcquire(11), "tryAcquire(11) at rate=10 should fail");
        assertTrue(limiter.tryAcquire(10), "tryAcquire(10) should succeed (initial burst)");
    }

    @Test
    public void testConcurrencySafety() throws InterruptedException {
        // 4 threads each acquiring 25 tokens at rate=100/s with 10s timeout.
        // Total demand = 100 tokens = exactly the rate capacity (initial burst), so all should
        // complete quickly without timeout.
        final TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(100, 10);
        Thread[] threads = new Thread[4];
        final boolean[] success = new boolean[4];
        for (int i = 0; i < 4; i++) {
            final int idx = i;
            threads[i] =
                    new Thread(
                            () -> {
                                try {
                                    for (int j = 0; j < 25; j++) {
                                        limiter.acquire(1);
                                    }
                                    success[idx] = true;
                                } catch (Exception e) {
                                    success[idx] = false;
                                }
                            });
        }
        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            t.join(5000);
        }
        for (int i = 0; i < 4; i++) {
            assertTrue(success[i], "Thread " + i + " should have completed without timeout");
        }
    }
}
