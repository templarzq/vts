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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.SSLContext;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link SslUtil} — SSL context creation and utility methods.
 *
 * <p>Uses {@code openssl} (available on most Linux/macOS dev machines) to generate
 * self-signed certificates for TLS/mTLS tests. Tests that require {@code openssl}
 * are skipped when it is not available.
 */
@DisplayName("SslUtil - SSL Context & Certificate Loading Tests")
class SslUtilTest {

    @TempDir
    Path tempDir;

    private Path caPath;
    private Path certPath;
    private Path keyPath;
    private Path nullCaCertPath;
    private Path nullCaKeyPath;
    private Path invalidPath;

    private static boolean opensslAvailable() {
        try {
            ProcessBuilder pb = new ProcessBuilder("openssl", "version");
            Process p = pb.start();
            int rc = p.waitFor();
            return rc == 0;
        } catch (Exception e) {
            return false;
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        invalidPath = tempDir.resolve("nonexistent.pem");

        if (opensslAvailable()) {
            // Generate self-signed CA cert
            caPath = tempDir.resolve("ca.pem");
            Path caKeyPath = tempDir.resolve("ca.key");
            runOpenSsl("req", "-x509", "-newkey", "rsa:2048",
                    "-keyout", caKeyPath.toString(),
                    "-out", caPath.toString(),
                    "-days", "365", "-nodes",
                    "-subj", "/CN=TestCA");

            // Generate client cert signed by CA
            certPath = tempDir.resolve("client.pem");
            Path clientKeyPath = tempDir.resolve("client.key");
            Path clientCsr = tempDir.resolve("client.csr");

            // Generate client key + CSR
            runOpenSsl("req", "-new", "-newkey", "rsa:2048",
                    "-keyout", clientKeyPath.toString(),
                    "-out", clientCsr.toString(),
                    "-nodes",
                    "-subj", "/CN=TestClient");

            // Sign client cert with CA
            runOpenSsl("x509", "-req",
                    "-in", clientCsr.toString(),
                    "-CA", caPath.toString(),
                    "-CAkey", caKeyPath.toString(),
                    "-CAcreateserial",
                    "-out", certPath.toString(),
                    "-days", "365");

            // Write client private key as PKCS8 for loadPrivateKey test
            keyPath = tempDir.resolve("client_pkcs8.key");
            runOpenSsl("pkcs8", "-topk8", "-nocrypt",
                    "-in", clientKeyPath.toString(),
                    "-out", keyPath.toString());

            // Generate client cert+key without CA (mTLS-only test)
            nullCaCertPath = tempDir.resolve("nullca_client.pem");
            nullCaKeyPath = tempDir.resolve("nullca_client_pkcs8.key");
            Path nullCaRawKey = tempDir.resolve("nullca_client.key");
            runOpenSsl("req", "-x509", "-newkey", "rsa:2048",
                    "-keyout", nullCaRawKey.toString(),
                    "-out", nullCaCertPath.toString(),
                    "-days", "365", "-nodes",
                    "-subj", "/CN=NullCaClient");
            runOpenSsl("pkcs8", "-topk8", "-nocrypt",
                    "-in", nullCaRawKey.toString(),
                    "-out", nullCaKeyPath.toString());
        } else {
            // Generate RSA key for loadPrivateKey test only
            KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
            kpg.initialize(2048);
            keyPath = tempDir.resolve("test.key");
            writePemPrivateKey(keyPath, kpg.generateKeyPair().getPrivate());
        }
    }

    // ---- hasTlsConfig ----

    @Test
    @DisplayName("hasTlsConfig returns true when CA path is set")
    void testHasTlsConfigWithCa() {
        assertTrue(SslUtil.hasTlsConfig("/some/path/ca.pem", null, null));
    }

    @Test
    @DisplayName("hasTlsConfig returns true when client cert paths are set")
    void testHasTlsConfigWithClientCerts() {
        assertTrue(SslUtil.hasTlsConfig(null, "/some/cert.pem", "/some/key.pem"));
    }

    @Test
    @DisplayName("hasTlsConfig returns true when only client key path is set")
    void testHasTlsConfigWithKeyOnly() {
        assertTrue(SslUtil.hasTlsConfig(null, null, "/some/key.pem"));
    }

    @Test
    @DisplayName("hasTlsConfig returns false when all paths are null")
    void testHasTlsConfigAllNull() {
        assertFalse(SslUtil.hasTlsConfig(null, null, null));
    }

    @Test
    @DisplayName("hasTlsConfig returns false when all paths are empty")
    void testHasTlsConfigAllEmpty() {
        assertFalse(SslUtil.hasTlsConfig("", "", ""));
    }

    @Test
    @DisplayName("hasTlsConfig returns true for blank strings (no trim — user error)")
    void testHasTlsConfigBlank() {
        // hasTlsConfig does NOT trim — whitespace-only strings are treated as non-empty paths.
        // This is intentional: trimming would mask user configuration errors.
        assertTrue(SslUtil.hasTlsConfig("  ", null, null));
    }

    // ---- hasCaConfig ----

    @Test
    @DisplayName("hasCaConfig returns false for null")
    void testHasCaConfigNull() {
        assertFalse(SslUtil.hasCaConfig(null));
    }

    @Test
    @DisplayName("hasCaConfig returns false for empty string")
    void testHasCaConfigEmpty() {
        assertFalse(SslUtil.hasCaConfig(""));
    }

    @Test
    @DisplayName("hasCaConfig returns false when file does not exist")
    void testHasCaConfigNotFound() {
        assertFalse(SslUtil.hasCaConfig(invalidPath.toString()));
    }

    // ---- hasMtlsConfig ----

    @Test
    @DisplayName("hasMtlsConfig returns false when cert is null")
    void testHasMtlsConfigNullCert() {
        assertFalse(SslUtil.hasMtlsConfig(null, "/some/key.pem"));
    }

    @Test
    @DisplayName("hasMtlsConfig returns false when key is null")
    void testHasMtlsConfigNullKey() {
        assertFalse(SslUtil.hasMtlsConfig("/some/cert.pem", null));
    }

    @Test
    @DisplayName("hasMtlsConfig returns false when files don't exist")
    void testHasMtlsConfigNotFound() {
        assertFalse(SslUtil.hasMtlsConfig(invalidPath.toString(), invalidPath.toString()));
    }

    @Test
    @DisplayName("hasMtlsConfig returns false for empty strings")
    void testHasMtlsConfigEmpty() {
        assertFalse(SslUtil.hasMtlsConfig("", ""));
    }

    // ---- createSslContext — with openssl certs ----

    @Test
    @DisplayName("createSslContext with no certs uses system trust store")
    void testCreateSslContextNoCerts() throws Exception {
        SSLContext ctx = SslUtil.createSslContext(null, null, null);
        assertNotNull(ctx);
        assertEquals("TLS", ctx.getProtocol());
    }

    @Test
    @DisplayName("createSslContext with missing CA file falls back to system trust store")
    void testCreateSslContextMissingCaFallsBack() throws Exception {
        // When the CA file does not exist, the method falls back to system trust
        // store rather than throwing. This mirrors the behavior of hasCaConfig.
        SSLContext ctx = SslUtil.createSslContext(invalidPath.toString(), null, null);
        assertNotNull(ctx);
        assertEquals("TLS", ctx.getProtocol());
    }

    // ---- loadPrivateKey ----

    @Test
    @DisplayName("loadPrivateKey loads a valid PEM PKCS#8 RSA private key")
    void testLoadPrivateKeyRsa() throws Exception {
        PrivateKey key = SslUtil.loadPrivateKey(keyPath.toString());
        assertNotNull(key);
        assertEquals("RSA", key.getAlgorithm());
    }

    @Test
    @DisplayName("loadPrivateKey throws for nonexistent file")
    void testLoadPrivateKeyNotFound() {
        assertThrows(Exception.class, () ->
                SslUtil.loadPrivateKey(invalidPath.toString()));
    }

    // ---- loadTrustManagerFactory (null/empty cases, always safe) ----

    @Test
    @DisplayName("loadTrustManagerFactory returns null when CA path is null")
    void testLoadTrustManagerFactoryNull() throws Exception {
        assertNull(SslUtil.loadTrustManagerFactory(null));
    }

    @Test
    @DisplayName("loadTrustManagerFactory returns null when CA path is empty")
    void testLoadTrustManagerFactoryEmpty() throws Exception {
        assertNull(SslUtil.loadTrustManagerFactory(""));
    }

    @Test
    @DisplayName("loadTrustManagerFactory returns null when CA file not found")
    void testLoadTrustManagerFactoryNotFound() throws Exception {
        assertNull(SslUtil.loadTrustManagerFactory(invalidPath.toString()));
    }

    // ---- loadKeyManagerFactory (null cases, always safe) ----

    @Test
    @DisplayName("loadKeyManagerFactory returns null when params are null")
    void testLoadKeyManagerFactoryNull() throws Exception {
        assertNull(SslUtil.loadKeyManagerFactory(null, null));
    }

    @Test
    @DisplayName("loadKeyManagerFactory returns null for empty strings")
    void testLoadKeyManagerFactoryEmpty() throws Exception {
        assertNull(SslUtil.loadKeyManagerFactory("", ""));
    }

    // ---- openssl-dependent tests ----

    @Test
    @EnabledIf("opensslAvailable")
    @DisplayName("[openssl] loadTrustManagerFactory with valid CA returns non-null")
    void testLoadTrustManagerFactoryValidOpenssl() throws Exception {
        javax.net.ssl.TrustManagerFactory tmf =
                SslUtil.loadTrustManagerFactory(caPath.toString());
        assertNotNull(tmf);
    }

    @Test
    @EnabledIf("opensslAvailable")
    @DisplayName("[openssl] loadKeyManagerFactory with valid cert+key returns non-null")
    void testLoadKeyManagerFactoryValidOpenssl() throws Exception {
        javax.net.ssl.KeyManagerFactory kmf =
                SslUtil.loadKeyManagerFactory(certPath.toString(), keyPath.toString());
        assertNotNull(kmf);
    }

    @Test
    @EnabledIf("opensslAvailable")
    @DisplayName("[openssl] hasCaConfig returns true when CA file exists")
    void testHasCaConfigValidOpenssl() {
        assertTrue(SslUtil.hasCaConfig(caPath.toString()));
    }

    @Test
    @EnabledIf("opensslAvailable")
    @DisplayName("[openssl] hasMtlsConfig returns true when cert+key exist")
    void testHasMtlsConfigValidOpenssl() {
        assertTrue(SslUtil.hasMtlsConfig(certPath.toString(), keyPath.toString()));
    }

    @Test
    @EnabledIf("opensslAvailable")
    @DisplayName("[openssl] createSslContext with CA only creates valid SSLContext")
    void testCreateSslContextWithCaOpenssl() throws Exception {
        SSLContext ctx = SslUtil.createSslContext(caPath.toString(), null, null);
        assertNotNull(ctx);
        assertEquals("TLS", ctx.getProtocol());
    }

    @Test
    @EnabledIf("opensslAvailable")
    @DisplayName("[openssl] createSslContext with CA + client cert creates mTLS SSLContext")
    void testCreateSslContextWithMtlsOpenssl() throws Exception {
        SSLContext ctx = SslUtil.createSslContext(
                caPath.toString(), certPath.toString(), keyPath.toString());
        assertNotNull(ctx);
        assertEquals("TLS", ctx.getProtocol());
    }

    @Test
    @EnabledIf("opensslAvailable")
    @DisplayName("[openssl] createSslContext with client cert but no CA uses system trust store for mTLS")
    void testCreateSslContextMtlsNoCaOpenssl() throws Exception {
        // mTLS without CA cert should work: system trust store for server verification,
        // client cert for client authentication.
        SSLContext ctx = SslUtil.createSslContext(
                null, nullCaCertPath.toString(), nullCaKeyPath.toString());
        assertNotNull(ctx);
        assertEquals("TLS", ctx.getProtocol());
    }

    // ---- helpers ----

    private static void runOpenSsl(String... args) throws Exception {
        String[] cmd = new String[args.length + 1];
        cmd[0] = "openssl";
        System.arraycopy(args, 0, cmd, 1, args.length);
        ProcessBuilder pb = new ProcessBuilder(cmd).redirectErrorStream(true);
        Process p = pb.start();
        String output = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        int rc = p.waitFor();
        if (rc != 0) {
            throw new RuntimeException("openssl failed with rc=" + rc + ": " + output);
        }
    }

    private static void writePemPrivateKey(Path path, PrivateKey key) throws Exception {
        String pem = "-----BEGIN PRIVATE KEY-----\n"
                + Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.UTF_8))
                        .encodeToString(key.getEncoded())
                + "\n-----END PRIVATE KEY-----\n";
        Files.writeString(path, pem);
    }
}
