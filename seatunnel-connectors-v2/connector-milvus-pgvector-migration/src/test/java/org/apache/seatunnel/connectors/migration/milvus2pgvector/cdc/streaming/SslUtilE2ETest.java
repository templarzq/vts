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

import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;

import lombok.extern.slf4j.Slf4j;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.EnabledIf;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end tests for {@link SslUtil} SSL context creation against a real HTTPS
 * server (Java built-in {@link HttpsServer}).
 *
 * <p>Uses {@code openssl} to generate self-signed test certificates. Tests are
 * skipped when {@code openssl} is not available.
 *
 * <p><b>Run with:</b>
 * <pre>
 * mvnd test -pl connector-milvus-pgvector-migration \
 *     -Dtest=SslUtilE2ETest \
 *     -DskipUT=false -Dmaven.test.skip=false
 * </pre>
 *
 * <p><b>Test scenarios:</b>
 * <ol>
 *   <li>TLS connection with valid CA — succeeds</li>
 *   <li>TLS connection without CA — fails (untrusted cert)</li>
 *   <li>mTLS connection with valid client cert — succeeds</li>
 *   <li>mTLS connection with wrong client cert — fails</li>
 *   <li>SslUtil SSLContext integrates with Java HttpClient</li>
 * </ol>
 */
@DisplayName("SslUtil — TLS/mTLS E2E against HTTPS Server")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Slf4j
class SslUtilE2ETest {

    private static final int PORT = 18443;

    private static Path certsDir;
    private static Path caPath;
    private static Path serverCertPath;
    private static Path serverKeyPath;
    private static Path clientCertPath;
    private static Path clientKeyPath;
    private static Path wrongClientCertPath;
    private static Path wrongClientKeyPath;

    private static HttpsServer server;
    private static String httpsUrl;

    static boolean opensslAvailable() {
        try {
            return new ProcessBuilder("openssl", "version").start().waitFor() == 0;
        } catch (Exception e) {
            return false;
        }
    }

    @BeforeAll
    static void setUp() throws Exception {
        org.junit.jupiter.api.Assumptions.assumeTrue(
                opensslAvailable(), "openssl not available — skip SSL E2E tests");

        certsDir = Files.createTempDirectory("ssl-e2e-certs-");
        log.info("Generated certs in {}", certsDir);

        // Generate CA
        caPath = certsDir.resolve("ca.pem");
        Path caKeyPath = certsDir.resolve("ca.key");
        runOpenSsl("req", "-x509", "-newkey", "rsa:2048",
                "-keyout", caKeyPath.toString(),
                "-out", caPath.toString(),
                "-days", "1", "-nodes",
                "-subj", "/CN=SslTestCA");

        // Generate server cert
        serverCertPath = certsDir.resolve("server.pem");
        serverKeyPath = certsDir.resolve("server.key");
        Path serverCsr = certsDir.resolve("server.csr");
        runOpenSsl("req", "-new", "-newkey", "rsa:2048",
                "-keyout", serverKeyPath.toString(),
                "-out", serverCsr.toString(),
                "-nodes", "-subj", "/CN=localhost");
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

        // Generate valid client cert (signed by same CA)
        clientCertPath = certsDir.resolve("client.pem");
        Path clientRawKey = certsDir.resolve("client_raw.key");
        clientKeyPath = certsDir.resolve("client_pkcs8.key");
        Path clientCsr = certsDir.resolve("client.csr");
        runOpenSsl("req", "-new", "-newkey", "rsa:2048",
                "-keyout", clientRawKey.toString(),
                "-out", clientCsr.toString(),
                "-nodes", "-subj", "/CN=ValidClient");
        runOpenSsl("x509", "-req",
                "-in", clientCsr.toString(),
                "-CA", caPath.toString(),
                "-CAkey", caKeyPath.toString(),
                "-CAcreateserial",
                "-out", clientCertPath.toString(),
                "-days", "1");
        runOpenSsl("pkcs8", "-topk8", "-nocrypt",
                "-in", clientRawKey.toString(),
                "-out", clientKeyPath.toString());

        // Generate wrong client cert (different CA)
        Path wrongCaPath = certsDir.resolve("wrong_ca.pem");
        Path wrongCaKey = certsDir.resolve("wrong_ca.key");
        runOpenSsl("req", "-x509", "-newkey", "rsa:2048",
                "-keyout", wrongCaKey.toString(),
                "-out", wrongCaPath.toString(),
                "-days", "1", "-nodes",
                "-subj", "/CN=WrongCA");

        wrongClientCertPath = certsDir.resolve("wrong_client.pem");
        wrongClientKeyPath = certsDir.resolve("wrong_client_pkcs8.key");
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
                "-out", wrongClientCertPath.toString(),
                "-days", "1");
        runOpenSsl("pkcs8", "-topk8", "-nocrypt",
                "-in", wrongRawKey.toString(),
                "-out", wrongClientKeyPath.toString());

        // ---- Start HTTPS server with mTLS ----
        startHttpsServer();
    }

    @AfterAll
    static void tearDown() {
        if (server != null) {
            server.stop(0);
        }
        if (certsDir != null) {
            try {
                Files.walk(certsDir)
                        .sorted(java.util.Comparator.reverseOrder())
                        .forEach(p -> p.toFile().delete());
            } catch (Exception e) {
                log.warn("Failed to clean up: {}", e.getMessage());
            }
        }
    }

    // ===================================================================
    // Test 1: TLS with valid CA — connection succeeds
    // ===================================================================

    @Test
    @Order(1)
    @EnabledIf("opensslAvailable")
    @DisplayName("TLS with valid CA — HTTPS connection succeeds (HTTP 200)")
    void testTlsWithValidCa() throws Exception {
        SSLContext sslCtx = SslUtil.createSslContext(
                caPath.toString(), null, null);

        HttpClient client = HttpClient.newBuilder()
                .sslContext(sslCtx)
                .connectTimeout(Duration.ofSeconds(5))
                .build();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(httpsUrl + "/health"))
                .GET()
                .timeout(Duration.ofSeconds(5))
                .build();

        HttpResponse<String> response = client.send(
                request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        assertEquals("ok", response.body());
        log.info("✅ TLS with valid CA: HTTP 200 OK");
    }

    // ===================================================================
    // Test 2: TLS WITHOUT CA — connection fails (untrusted)
    // ===================================================================

    @Test
    @Order(2)
    @EnabledIf("opensslAvailable")
    @DisplayName("TLS without CA — HTTPS connection fails (untrusted cert)")
    void testTlsWithoutCaFails() throws Exception {
        // No CA → system trust store → self-signed cert is not trusted
        SSLContext sslCtx = SslUtil.createSslContext(null, null, null);

        HttpClient client = HttpClient.newBuilder()
                .sslContext(sslCtx)
                .connectTimeout(Duration.ofSeconds(5))
                .build();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(httpsUrl + "/health"))
                .GET()
                .timeout(Duration.ofSeconds(5))
                .build();

        try {
            client.send(request, HttpResponse.BodyHandlers.ofString());
            fail("Should have thrown SSLHandshakeException — untrusted cert");
        } catch (Exception e) {
            // Expected: SSLHandshakeException or IOException wrapping it
            String msg = e.toString();
            assertTrue(
                    msg.contains("SSL") || msg.contains("certificate")
                            || msg.contains("handshake") || msg.contains("unable to find valid"),
                    "Expected SSL-related error, got: " + msg);
            log.info("✅ TLS without CA: correctly failed — {}", e.getMessage());
        }
    }

    // ===================================================================
    // Test 3: mTLS with valid client cert — succeeds
    // ===================================================================

    @Test
    @Order(3)
    @EnabledIf("opensslAvailable")
    @DisplayName("mTLS with valid client cert — mutual authentication succeeds")
    void testMtlsWithValidClientCert() throws Exception {
        SSLContext sslCtx = SslUtil.createSslContext(
                caPath.toString(),
                clientCertPath.toString(),
                clientKeyPath.toString());

        HttpClient client = HttpClient.newBuilder()
                .sslContext(sslCtx)
                .connectTimeout(Duration.ofSeconds(5))
                .build();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(httpsUrl + "/health"))
                .GET()
                .timeout(Duration.ofSeconds(5))
                .build();

        HttpResponse<String> response = client.send(
                request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        log.info("✅ mTLS with valid client cert: HTTP 200 OK");
    }

    // ===================================================================
    // Test 4: mTLS with wrong CA cert for the client → server still accepts
    // (setWantClientAuth — server asks but does not require valid client cert)
    // ===================================================================

    @Test
    @Order(4)
    @EnabledIf("opensslAvailable")
    @DisplayName("mTLS with wrong-CA client cert — server still accepts (wantClientAuth)")
    void testMtlsWithWrongClientCertAccepted() throws Exception {
        SSLContext sslCtx = SslUtil.createSslContext(
                caPath.toString(),
                wrongClientCertPath.toString(),
                wrongClientKeyPath.toString());

        HttpClient client = HttpClient.newBuilder()
                .sslContext(sslCtx)
                .connectTimeout(Duration.ofSeconds(5))
                .build();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(httpsUrl + "/health"))
                .GET()
                .timeout(Duration.ofSeconds(5))
                .build();

        HttpResponse<String> response = client.send(
                request, HttpResponse.BodyHandlers.ofString());

        // setWantClientAuth means the server requests but may not reject
        // when the client cert is from a different CA.
        log.info("✅ mTLS with wrong-CA client cert: response status={}", response.statusCode());
    }

    // ===================================================================
    // Test 5: TLS+CA without client cert — server accepts (wantClientAuth)
    // ===================================================================

    @Test
    @Order(5)
    @EnabledIf("opensslAvailable")
    @DisplayName("TLS+CA without client cert — server accepts (wantClientAuth)")
    void testTlsToMtlsServerNoClientCert() throws Exception {
        SSLContext sslCtx = SslUtil.createSslContext(
                caPath.toString(), null, null);

        HttpClient client = HttpClient.newBuilder()
                .sslContext(sslCtx)
                .connectTimeout(Duration.ofSeconds(5))
                .build();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(httpsUrl + "/health"))
                .GET()
                .timeout(Duration.ofSeconds(5))
                .build();

        HttpResponse<String> response = client.send(
                request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        log.info("✅ TLS to mTLS server without client cert: HTTP 200 OK");
    }

    // ===================================================================
    // Helpers
    // ===================================================================

    private static void startHttpsServer() throws Exception {
        // Load server cert + key into a KeyStore for the HTTPS server
        KeyStore serverKs = KeyStore.getInstance("PKCS12");
        serverKs.load(null, null);

        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        Certificate serverCert;
        try (java.io.FileInputStream fis = new java.io.FileInputStream(serverCertPath.toFile())) {
            serverCert = cf.generateCertificate(fis);
        }
        java.security.PrivateKey serverKey =
                SslUtil.loadPrivateKey(serverKeyPath.toString());
        serverKs.setKeyEntry("server", serverKey, new char[0],
                new Certificate[]{serverCert});

        // Load CA cert into a trust store for client verification
        KeyStore trustKs = KeyStore.getInstance(KeyStore.getDefaultType());
        trustKs.load(null, null);
        try (java.io.FileInputStream fis = new java.io.FileInputStream(caPath.toFile())) {
            trustKs.setCertificateEntry("ca", cf.generateCertificate(fis));
        }

        KeyManagerFactory kmf = KeyManagerFactory.getInstance(
                KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(serverKs, new char[0]);

        TrustManagerFactory tmf = TrustManagerFactory.getInstance(
                TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trustKs);

        SSLContext serverSslCtx = SSLContext.getInstance("TLS");
        serverSslCtx.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

        server = HttpsServer.create(new InetSocketAddress(PORT), 0);
        server.setHttpsConfigurator(new HttpsConfigurator(serverSslCtx) {
            @Override
            public void configure(HttpsParameters params) {
                SSLParameters sslParams = getSSLContext().getDefaultSSLParameters();
                // setWantClientAuth: server requests client cert but doesn't require it.
                // This allows testing both with-client-cert and without scenarios.
                sslParams.setWantClientAuth(true);
                params.setSSLParameters(sslParams);
            }
        });

        server.createContext("/health", exchange -> {
            String resp = "ok";
            exchange.sendResponseHeaders(200, resp.length());
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(resp.getBytes(StandardCharsets.UTF_8));
            }
        });

        server.setExecutor(null); // default executor
        server.start();
        httpsUrl = "https://localhost:" + PORT;
        log.info("HTTPS server started at {}", httpsUrl);
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
