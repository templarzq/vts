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

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;

/**
 * Utility class for loading SSL/TLS contexts from PEM-encoded certificate files.
 *
 * <p>Shared between {@link PChannelResolver} (etcd HTTP client) and the gRPC
 * clients ({@code StreamingNodeHandlerClient}, {@code MilvusCdcGrpcClient})
 * that need custom TLS/mTLS configuration.
 */
@Slf4j
public final class SslUtil {

    private SslUtil() {
        // utility class
    }

    /**
     * Check whether any TLS-relevant config is specified.
     */
    public static boolean hasTlsConfig(String caPath, String certPath, String keyPath) {
        return (caPath != null && !caPath.isEmpty())
                || (certPath != null && !certPath.isEmpty())
                || (keyPath != null && !keyPath.isEmpty());
    }

    /**
     * Check whether mTLS (client certificate) config is fully specified.
     */
    public static boolean hasMtlsConfig(String certPath, String keyPath) {
        return certPath != null && !certPath.isEmpty()
                && keyPath != null && !keyPath.isEmpty()
                && Files.exists(Paths.get(certPath))
                && Files.exists(Paths.get(keyPath));
    }

    /**
     * Check whether CA certificate config is specified and the file exists.
     */
    public static boolean hasCaConfig(String caPath) {
        return caPath != null && !caPath.isEmpty() && Files.exists(Paths.get(caPath));
    }

    /**
     * Create an {@link SSLContext} from PEM-encoded certificate files.
     *
     * <p>Supports:
     * <ul>
     *   <li><b>TLS</b> — provide {@code caPath} only; uses system trust store if
     *       {@code caPath} is null/empty.</li>
     *   <li><b>mTLS</b> — additionally provide {@code certPath} and
     *       {@code keyPath} for client certificate authentication.</li>
     * </ul>
     *
     * @param caPath   path to CA certificate PEM file (may be null / empty)
     * @param certPath path to client certificate PEM file (may be null / empty)
     * @param keyPath  path to client private key PEM file (may be null / empty)
     * @return initialized SSLContext
     * @throws Exception if certificate loading fails
     */
    public static SSLContext createSslContext(String caPath, String certPath, String keyPath)
            throws Exception {
        TrustManagerFactory tmf = loadTrustManagerFactory(caPath);
        KeyManagerFactory kmf = loadKeyManagerFactory(certPath, keyPath);

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(
                kmf != null ? kmf.getKeyManagers() : null,
                tmf != null ? tmf.getTrustManagers() : null,
                null);
        return sslContext;
    }

    /**
     * Load a {@link TrustManagerFactory} from a PEM CA certificate file.
     * Returns {@code null} to fall back to the system trust store.
     */
    static TrustManagerFactory loadTrustManagerFactory(String caPath) throws Exception {
        if (!hasCaConfig(caPath)) {
            return null; // use system trust store
        }
        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);
        try (InputStream is = new FileInputStream(caPath)) {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            Certificate cert = cf.generateCertificate(is);
            trustStore.setCertificateEntry("ca", cert);
        }
        TrustManagerFactory tmf =
                TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trustStore);
        return tmf;
    }

    /**
     * Load a {@link KeyManagerFactory} from PEM client certificate and private key files.
     * Returns {@code null} when mTLS is not configured.
     */
    static KeyManagerFactory loadKeyManagerFactory(String certPath, String keyPath)
            throws Exception {
        if (!hasMtlsConfig(certPath, keyPath)) {
            return null;
        }
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        keyStore.load(null, null);

        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        Certificate cert;
        try (InputStream is = new FileInputStream(certPath)) {
            cert = cf.generateCertificate(is);
        }

        java.security.PrivateKey privateKey = loadPrivateKey(keyPath);
        keyStore.setKeyEntry(
                "client", privateKey, new char[0], new Certificate[] {cert});

        KeyManagerFactory kmf =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, new char[0]);
        return kmf;
    }

    /**
     * Load a PEM-encoded RSA or EC private key (PKCS#8 or traditional format).
     */
    static java.security.PrivateKey loadPrivateKey(String keyPath) throws Exception {
        String content =
                new String(Files.readAllBytes(Paths.get(keyPath)), StandardCharsets.UTF_8);
        content =
                content
                        .replace("-----BEGIN PRIVATE KEY-----", "")
                        .replace("-----END PRIVATE KEY-----", "")
                        .replace("-----BEGIN RSA PRIVATE KEY-----", "")
                        .replace("-----END RSA PRIVATE KEY-----", "")
                        .replace("-----BEGIN EC PRIVATE KEY-----", "")
                        .replace("-----END EC PRIVATE KEY-----", "")
                        .replaceAll("\\s", "");

        byte[] keyBytes = Base64.getDecoder().decode(content);
        PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keyBytes);
        java.security.KeyFactory kf = java.security.KeyFactory.getInstance("RSA");
        try {
            return kf.generatePrivate(spec);
        } catch (java.security.spec.InvalidKeySpecException e) {
            kf = java.security.KeyFactory.getInstance("EC");
            return kf.generatePrivate(spec);
        }
    }
}
