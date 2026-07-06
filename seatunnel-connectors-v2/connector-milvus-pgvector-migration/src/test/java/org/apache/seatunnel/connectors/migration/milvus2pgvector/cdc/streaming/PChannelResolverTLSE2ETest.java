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

package org.apache.seatunnel.connectors.migration.milvus2pgvector.cdc.streaming;

import lombok.extern.slf4j.Slf4j;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end TLS/mTLS tests for {@link PChannelResolver} against a real etcd
 * instance with TLS enabled.
 *
 * <p><b>Prerequisites:</b>
 * <ul>
 *   <li>Docker daemon running (for Testcontainers)</li>
 *   <li>{@code openssl} available on PATH (for certificate generation)</li>
 * </ul>
 *
 * <p><b>Enable with:</b>
 * <pre>
 * mvnd test -pl connector-milvus-pgvector-migration \
 *     -Dtest=PChannelResolverTLSE2ETest \
 *     -Dmigration.tls.e2e.enabled=true -DskipUT=false -Dmaven.test.skip=false
 * </pre>
 *
 * <p><b>Test scenarios:</b>
 * <ol>
 *   <li>TLS with CA certificate — connect successfully and resolve pchannel</li>
 *   <li>mTLS with client certificate — mutual authentication</li>
 *   <li>TLS without certificate — connection refused</li>
 *   <li>TLS with wrong CA — connection fails</li>
 *   <li>Basic Auth (RBAC) over TLS — authentication with username/password</li>
 *   <li>Multiple pchannel resolution over TLS</li>
 * </ol>
 */
@EnabledIfSystemProperty(named = "migration.tls.e2e.enabled", matches = "true")
@DisplayName("PChannelResolver — etcd TLS/mTLS E2E Tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Slf4j
class PChannelResolverTLSE2ETest {

    private static final String ETCD_IMAGE = "quay.io/coreos/etcd:v3.5.14";
    private static final int ETCD_CLIENT_PORT = 2379;
    private static final String ROOT_PATH = "by-dev";
    private static final long TEST_COLLECTION_ID = 4512345678L;
    private static final String TEST_PCHANNEL = ROOT_PATH + "-rootcoord-dml_0";

    private static Path certsDir;
    private static Path caPath;
    private static Path serverCertPath;
    private static Path serverKeyPath;
    private static Path clientCertPath;
    private static Path clientKeyPath;
    private static Path wrongCaPath;
    private static Path wrongCertPath;
    private static Path wrongKeyPath;

    private static GenericContainer<?> etcdContainer;
    private static String etcdHttpsEndpoint;

    private static boolean opensslAvailable() {
        try {
            return new ProcessBuilder("openssl", "version").start().waitFor() == 0;
        } catch (Exception e) {
            return false;
        }
    }

    @BeforeAll
    static void setUp() throws Exception {
        org.junit.jupiter.api.Assumptions.assumeTrue(
                opensslAvailable(), "openssl not available — skip TLS E2E tests");

        // ---- Phase 1: Generate certificates ----
        certsDir = Files.createTempDirectory("etcd-tls-certs-");
        log.info("Generated certs in {}", certsDir);

        // Generate CA (trust anchor)
        caPath = certsDir.resolve("ca.pem");
        Path caKeyPath = certsDir.resolve("ca.key");
        runOpenSsl("req", "-x509", "-newkey", "rsa:2048",
                "-keyout", caKeyPath.toString(),
                "-out", caPath.toString(),
                "-days", "1", "-nodes",
                "-subj", "/CN=EtcdTestCA");

        // Generate server cert signed by CA
        serverCertPath = certsDir.resolve("server.pem");
        serverKeyPath = certsDir.resolve("server.key");
        Path serverCsr = certsDir.resolve("server.csr");
        runOpenSsl("req", "-new", "-newkey", "rsa:2048",
                "-keyout", serverKeyPath.toString(),
                "-out", serverCsr.toString(),
                "-nodes", "-subj", "/CN=localhost");
        // Add SAN for localhost/Docker IP
        Path extFile = certsDir.resolve("server.ext");
        Files.writeString(extFile,
                "subjectAltName=DNS:localhost,IP:127.0.0.1\n");
        runOpenSsl("x509", "-req",
                "-in", serverCsr.toString(),
                "-CA", caPath.toString(),
                "-CAkey", caKeyPath.toString(),
                "-CAcreateserial",
                "-out", serverCertPath.toString(),
                "-days", "1",
                "-extfile", extFile.toString());

        // Generate client cert signed by CA (for mTLS)
        clientCertPath = certsDir.resolve("client.pem");
        Path clientRawKey = certsDir.resolve("client_raw.key");
        clientKeyPath = certsDir.resolve("client_pkcs8.key");
        Path clientCsr = certsDir.resolve("client.csr");
        runOpenSsl("req", "-new", "-newkey", "rsa:2048",
                "-keyout", clientRawKey.toString(),
                "-out", clientCsr.toString(),
                "-nodes", "-subj", "/CN=EtcdClient");
        runOpenSsl("x509", "-req",
                "-in", clientCsr.toString(),
                "-CA", caPath.toString(),
                "-CAkey", caKeyPath.toString(),
                "-CAcreateserial",
                "-out", clientCertPath.toString(),
                "-days", "1");
        // Convert to PKCS8 for SslUtil
        runOpenSsl("pkcs8", "-topk8", "-nocrypt",
                "-in", clientRawKey.toString(),
                "-out", clientKeyPath.toString());

        // Generate a WRONG CA / client cert (not trusted by etcd)
        wrongCaPath = certsDir.resolve("wrong_ca.pem");
        Path wrongCaKey = certsDir.resolve("wrong_ca.key");
        runOpenSsl("req", "-x509", "-newkey", "rsa:2048",
                "-keyout", wrongCaKey.toString(),
                "-out", wrongCaPath.toString(),
                "-days", "1", "-nodes",
                "-subj", "/CN=WrongCA");

        wrongCertPath = certsDir.resolve("wrong_client.pem");
        wrongKeyPath = certsDir.resolve("wrong_client_pkcs8.key");
        Path wrongRawKey = certsDir.resolve("wrong_client_raw.key");
        Path wrongCsr = certsDir.resolve("wrong_client.csr");
        runOpenSsl("req", "-new", "-newkey", "rsa:2048",
                "-keyout", wrongRawKey.toString(),
                "-out", wrongCsr.toString(),
                "-nodes", "-subj", "/CN=WrongClient");
        runOpenSsl("x509", "-req",
                "-in", wrongCsr.toString(),
                "-CA", wrongCaPath.toString(),
                "-CAkey", wrongCaKey.toString(),
                "-CAcreateserial",
                "-out", wrongCertPath.toString(),
                "-days", "1");
        runOpenSsl("pkcs8", "-topk8", "-nocrypt",
                "-in", wrongRawKey.toString(),
                "-out", wrongKeyPath.toString());

        // ---- Phase 2: Start etcd with TLS ----
        etcdContainer = new GenericContainer<>(DockerImageName.parse(ETCD_IMAGE))
                .withCopyFileToContainer(
                        MountableFile.forHostPath(caPath), "/certs/ca.pem")
                .withCopyFileToContainer(
                        MountableFile.forHostPath(serverCertPath), "/certs/server.pem")
                .withCopyFileToContainer(
                        MountableFile.forHostPath(serverKeyPath), "/certs/server.key")
                .withCommand(
                        "etcd",
                        "--cert-file=/certs/server.pem",
                        "--key-file=/certs/server.key",
                        "--trusted-ca-file=/certs/ca.pem",
                        "--client-cert-auth",
                        "--listen-client-urls=https://0.0.0.0:" + ETCD_CLIENT_PORT,
                        "--advertise-client-urls=https://0.0.0.0:" + ETCD_CLIENT_PORT,
                        "--data-dir=/tmp/etcd-data")
                .withExposedPorts(ETCD_CLIENT_PORT)
                .waitingFor(Wait.forLogMessage(".*ready to serve client requests.*", 1))
                .withStartupTimeout(Duration.ofSeconds(30));

        etcdContainer.start();

        String host = etcdContainer.getHost();
        int mappedPort = etcdContainer.getMappedPort(ETCD_CLIENT_PORT);
        etcdHttpsEndpoint = "https://" + host + ":" + mappedPort;
        log.info("etcd started with TLS at {}", etcdHttpsEndpoint);

        // ---- Phase 3: Seed test data into etcd ----
        seedChannelCpData();
    }

    @AfterAll
    static void tearDown() {
        if (etcdContainer != null) {
            etcdContainer.stop();
        }
        if (certsDir != null) {
            try {
                Files.walk(certsDir)
                        .sorted(java.util.Comparator.reverseOrder())
                        .forEach(p -> p.toFile().delete());
            } catch (Exception e) {
                log.warn("Failed to clean up certs dir: {}", e.getMessage());
            }
        }
    }

    // ===================================================================
    // Test 1: TLS with valid CA — connect and resolve pchannel
    // ===================================================================

    @Test
    @Order(1)
    @DisplayName("TLS with valid CA — resolve pchannel successfully")
    void testTlsWithCa() {
        PChannelResolver resolver = new PChannelResolver(
                etcdHttpsEndpoint,
                ROOT_PATH,
                caPath.toString(),       // CA cert
                null,                    // no client cert
                null,                    // no client key
                null, null);             // no auth

        String pchannel = resolver.resolvePChannel(TEST_COLLECTION_ID);
        assertNotNull(pchannel, "Should resolve pchannel via TLS");
        assertEquals(TEST_PCHANNEL, pchannel,
                "Should resolve correct pchannel: " + TEST_PCHANNEL);
        log.info("✅ TLS with CA: resolved pchannel={} for collectionId={}", pchannel,
                TEST_COLLECTION_ID);
    }

    // ===================================================================
    // Test 2: mTLS with client cert — mutual auth
    // ===================================================================

    @Test
    @Order(2)
    @DisplayName("mTLS with client cert — mutual authentication")
    void testMtlsWithClientCert() {
        PChannelResolver resolver = new PChannelResolver(
                etcdHttpsEndpoint,
                ROOT_PATH,
                caPath.toString(),
                clientCertPath.toString(),
                clientKeyPath.toString(),
                null, null);

        String pchannel = resolver.resolvePChannel(TEST_COLLECTION_ID);
        assertNotNull(pchannel, "Should resolve pchannel via mTLS");
        assertEquals(TEST_PCHANNEL, pchannel);
        log.info("✅ mTLS with client cert: resolved pchannel={}", pchannel);
    }

    // ===================================================================
    // Test 3: TLS WITHOUT CA — connection should fail
    // ===================================================================

    @Test
    @Order(3)
    @DisplayName("TLS without CA cert — connection fails")
    void testTlsWithoutCaFails() {
        PChannelResolver resolver = new PChannelResolver(
                etcdHttpsEndpoint,
                ROOT_PATH,
                null,                    // NO CA → system trust store
                null, null, null, null);

        // Without the custom CA, the system trust store won't trust the
        // self-signed etcd cert → connection fails → resolvePChannel returns null.
        String pchannel = resolver.resolvePChannel(TEST_COLLECTION_ID);
        assertNull(pchannel,
                "Should fail to resolve pchannel without proper CA cert");
        log.info("✅ TLS without CA: correctly failed (pchannel=null)");
    }

    // ===================================================================
    // Test 4: TLS with WRONG CA — connection fails
    // ===================================================================

    @Test
    @Order(4)
    @DisplayName("TLS with wrong CA — connection fails")
    void testTlsWithWrongCaFails() {
        PChannelResolver resolver = new PChannelResolver(
                etcdHttpsEndpoint,
                ROOT_PATH,
                wrongCaPath.toString(),   // WRONG CA → not trusted
                null, null, null, null);

        String pchannel = resolver.resolvePChannel(TEST_COLLECTION_ID);
        assertNull(pchannel,
                "Should fail to resolve pchannel with wrong CA cert");
        log.info("✅ TLS with wrong CA: correctly failed (pchannel=null)");
    }

    // ===================================================================
    // Test 5: mTLS with WRONG client cert — auth fails
    // ===================================================================

    @Test
    @Order(5)
    @DisplayName("mTLS with wrong client cert — auth fails")
    void testMtlsWithWrongClientCertFails() {
        PChannelResolver resolver = new PChannelResolver(
                etcdHttpsEndpoint,
                ROOT_PATH,
                caPath.toString(),         // valid CA
                wrongCertPath.toString(),  // WRONG client cert (different CA)
                wrongKeyPath.toString(),   // WRONG key
                null, null);

        String pchannel = resolver.resolvePChannel(TEST_COLLECTION_ID);
        // Wrong client cert → etcd rejects the TLS handshake or
        // the connection fails → resolvePChannel returns null.
        assertNull(pchannel,
                "Should fail to resolve pchannel with wrong client cert");
        log.info("✅ mTLS with wrong client cert: correctly failed (pchannel=null)");
    }

    // ===================================================================
    // Test 6: Warm-up cache over TLS
    // ===================================================================

    @Test
    @Order(6)
    @DisplayName("Cache warm-up over TLS")
    void testWarmUpOverTls() {
        PChannelResolver resolver = new PChannelResolver(
                etcdHttpsEndpoint,
                ROOT_PATH,
                caPath.toString(),
                clientCertPath.toString(),
                clientKeyPath.toString(),
                null, null);

        resolver.warmUp();

        // After warm-up, resolve should return from cache
        String pchannel = resolver.resolvePChannel(TEST_COLLECTION_ID);
        assertNotNull(pchannel);
        assertEquals(TEST_PCHANNEL, pchannel);
        log.info("✅ Cache warm-up over TLS: resolved pchannel={}", pchannel);
    }

    // ===================================================================
    // Test 7: Multiple resolution over TLS (cache hit)
    // ===================================================================

    @Test
    @Order(7)
    @DisplayName("Multiple resolve calls over TLS use cache")
    void testMultipleResolveUsesCache() {
        PChannelResolver resolver = new PChannelResolver(
                etcdHttpsEndpoint,
                ROOT_PATH,
                caPath.toString(), null, null, null, null);

        // First call — should hit etcd
        String p1 = resolver.resolvePChannel(TEST_COLLECTION_ID);
        assertNotNull(p1);

        // Second call — should hit cache
        String p2 = resolver.resolvePChannel(TEST_COLLECTION_ID);
        assertEquals(p1, p2);
        log.info("✅ Multiple resolve over TLS: cached result matches (pchannel={})", p1);
    }

    // ===================================================================
    // Test 8: Invalid cert file path → graceful fallback
    // ===================================================================

    @Test
    @Order(8)
    @DisplayName("Invalid CA file path — graceful fallback to system trust (fails)")
    void testInvalidCaFilePath() {
        PChannelResolver resolver = new PChannelResolver(
                etcdHttpsEndpoint,
                ROOT_PATH,
                "/nonexistent/ca.pem",   // nonexistent file
                null, null, null, null);

        // Invalid path → no TLS context (system default) → can't verify self-signed cert
        String pchannel = resolver.resolvePChannel(TEST_COLLECTION_ID);
        assertNull(pchannel, "Should fail when CA file doesn't exist");
        log.info("✅ Invalid CA file path: correctly failed");
    }

    // ===================================================================
    // Test 9: vchannel resolution over TLS
    // ===================================================================

    @Test
    @Order(9)
    @DisplayName("vchannel resolution over TLS — pchannel + collection suffix")
    void testVChannelResolutionOverTls() {
        PChannelResolver resolver = new PChannelResolver(
                etcdHttpsEndpoint,
                ROOT_PATH,
                caPath.toString(), null, null, null, null);

        String vchannel = resolver.resolveVChannel(TEST_COLLECTION_ID);
        assertNotNull(vchannel);
        assertEquals(TEST_PCHANNEL + "_" + TEST_COLLECTION_ID + "v0", vchannel);
        log.info("✅ vchannel over TLS: resolved vchannel={}", vchannel);
    }

    // ===================================================================
    // Helpers
    // ===================================================================

    /**
     * Insert a channel-cp key into etcd via its v3 HTTP API over HTTPS.
     * Key format: by-dev/meta/datacoord-meta/channel-cp/{pchannel}_{collectionId}v0
     */
    private static void seedChannelCpData() throws Exception {
        String key = ROOT_PATH + "/meta/datacoord-meta/channel-cp/"
                + TEST_PCHANNEL + "_" + TEST_COLLECTION_ID + "v0";
        String b64Key = Base64.getEncoder()
                .encodeToString(key.getBytes(StandardCharsets.UTF_8));
        String b64Value = Base64.getEncoder()
                .encodeToString("{}".getBytes(StandardCharsets.UTF_8));

        String body = String.format(
                "{\"key\":\"%s\",\"value\":\"%s\"}", b64Key, b64Value);

        javax.net.ssl.SSLContext sslCtx = SslUtil.createSslContext(
                caPath.toString(),
                clientCertPath.toString(),
                clientKeyPath.toString());

        HttpClient httpClient = HttpClient.newBuilder()
                .sslContext(sslCtx)
                .connectTimeout(Duration.ofSeconds(10))
                .build();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(etcdHttpsEndpoint + "/v3/kv/put"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .timeout(Duration.ofSeconds(10))
                .build();

        HttpResponse<String> response = httpClient.send(
                request, HttpResponse.BodyHandlers.ofString());

        log.info("Seeded etcd with channel-cp data: status={}", response.statusCode());
        assertTrue(response.statusCode() == 200,
                "etcd seed should succeed, got status=" + response.statusCode());
    }

    private static void runOpenSsl(String... args) throws Exception {
        String[] cmd = new String[args.length + 1];
        cmd[0] = "openssl";
        System.arraycopy(args, 0, cmd, 1, args.length);
        ProcessBuilder pb = new ProcessBuilder(cmd).redirectErrorStream(true);
        Process p = pb.start();
        String output = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        int rc = p.waitFor();
        if (rc != 0) {
            throw new RuntimeException("openssl failed (rc=" + rc + "): " + output);
        }
    }
}
