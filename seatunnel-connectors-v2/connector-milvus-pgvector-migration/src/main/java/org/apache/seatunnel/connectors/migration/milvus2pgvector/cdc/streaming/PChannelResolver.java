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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Resolves the pchannel for a given Milvus collection by querying etcd.
 *
 * <p>In Milvus standalone mode, pchannel assignments are stored in etcd under
 * {@code <rootPath>/meta/datacoord-meta/channel-cp/}. The key format is:
 * <pre>
 *   {rootPath}/meta/datacoord-meta/channel-cp/{pchannel}_{collectionID}v0
 * </pre>
 * where {@code pchannel} is {@code {rootPath}-rootcoord-dml_{N}}.
 *
 * <p>Supports etcd TLS (mTLS) and basic auth (RBAC). Results are cached
 * in-memory to avoid repeated etcd calls.
 */
@Slf4j
public class PChannelResolver {

    private static final String ETCD_KV_RANGE_PATH = "/v3/kv/range";
    private static final Pattern CHANNEL_CP_PATTERN =
            Pattern.compile(".*/([^/]+-rootcoord-dml_\\d+)_(\\d+)v0$");

    private static final ObjectMapper JSON = new ObjectMapper();

    private final String etcdEndpoint;
    private final String rootPath;
    private final HttpClient httpClient;
    private final String authHeader;
    private final Map<Long, String> collectionPchannelCache = new ConcurrentHashMap<>();
    private Map<Long, String> allPchannelMappings;

    /**
     * @param etcdEndpoint etcd HTTP endpoint (e.g. "http://localhost:2379")
     * @param rootPath     Milvus root path in etcd (e.g. "by-dev")
     */
    public PChannelResolver(String etcdEndpoint, String rootPath) {
        this(etcdEndpoint, rootPath, null, null, null, null, null);
    }

    /**
     * Full constructor with TLS and auth support.
     *
     * @param etcdEndpoint    etcd HTTP(S) endpoint (e.g. "https://localhost:2379")
     * @param rootPath        Milvus root path in etcd (e.g. "by-dev")
     * @param caPath          CA certificate path for TLS (null = no TLS / system trust)
     * @param clientCertPath  client certificate path for mTLS (null = no client cert)
     * @param clientKeyPath   client key path for mTLS (null = no client cert)
     * @param username        etcd username for RBAC (null = no auth)
     * @param password        etcd password for RBAC (null = no auth)
     */
    public PChannelResolver(String etcdEndpoint, String rootPath,
                            String caPath, String clientCertPath, String clientKeyPath,
                            String username, String password) {
        this.etcdEndpoint = (etcdEndpoint != null && !etcdEndpoint.isEmpty()) ? etcdEndpoint : null;
        this.rootPath = rootPath;
        this.httpClient = this.etcdEndpoint != null
                ? buildHttpClient(caPath, clientCertPath, clientKeyPath)
                : null;
        this.authHeader = buildAuthHeader(username, password);
    }

    /**
     * Resolve the pchannel for a collection by its ID.
     *
     * @param collectionId Milvus collection ID
     * @return pchannel name (e.g. "by-dev-rootcoord-dml_12"), or null if not found
     */
    public String resolvePChannel(long collectionId) {
        return collectionPchannelCache.computeIfAbsent(collectionId, id -> {
            if (allPchannelMappings != null && allPchannelMappings.containsKey(id)) {
                return allPchannelMappings.get(id);
            }
            Map<Long, String> mappings = loadAllMappings();
            if (mappings != null) {
                allPchannelMappings = mappings;
                return mappings.get(id);
            }
            return null;
        });
    }

    /**
     * Get the vchannel for a collection (pchannel + collection suffix).
     */
    public String resolveVChannel(long collectionId) {
        String pchannel = resolvePChannel(collectionId);
        if (pchannel == null) {
            return null;
        }
        return pchannel + "_" + collectionId + "v0";
    }

    /**
     * Pre-warm the cache by loading all collection→pchannel mappings from etcd.
     */
    public void warmUp() {
        Map<Long, String> mappings = loadAllMappings();
        if (mappings != null) {
            allPchannelMappings = mappings;
            collectionPchannelCache.putAll(mappings);
            log.info("PChannelResolver warmed up: {} collection→pchannel mappings loaded",
                    mappings.size());
        }
    }

    /**
     * Load all collection→pchannel mappings from etcd.
     */
    private Map<Long, String> loadAllMappings() {
        if (etcdEndpoint == null) {
            log.debug("PChannelResolver: etcd endpoint not configured, skipping channel-cp lookup");
            return null;
        }
        String channelCpPrefix = rootPath + "/meta/datacoord-meta/channel-cp/";
        String keyEncoded = base64Encode(channelCpPrefix);

        String rangeEnd = channelCpPrefix.substring(0, channelCpPrefix.length() - 1)
                + (char) (channelCpPrefix.charAt(channelCpPrefix.length() - 1) + 1);
        String rangeEndEncoded = base64Encode(rangeEnd);

        String requestBody = String.format(
                "{\"key\":\"%s\",\"range_end\":\"%s\",\"limit\":200}",
                keyEncoded, rangeEndEncoded);

        try {
            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                    .uri(URI.create(etcdEndpoint + ETCD_KV_RANGE_PATH))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                    .timeout(Duration.ofSeconds(10));

            if (authHeader != null) {
                requestBuilder.header("Authorization", authHeader);
            }

            HttpResponse<String> response = httpClient.send(
                    requestBuilder.build(), HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                log.warn("etcd query returned status {}: {}", response.statusCode(), response.body());
                return null;
            }

            return parseChannelCpResponse(response.body());
        } catch (Exception e) {
            log.warn("Failed to query etcd for channel-cp mappings: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Parse the etcd KV range response using Jackson and extract
     * collection→pchannel mappings.
     */
    Map<Long, String> parseChannelCpResponse(String responseBody) {
        Map<Long, String> mappings = new ConcurrentHashMap<>();

        try {
            JsonNode root = JSON.readTree(responseBody);
            JsonNode kvs = root.path("kvs");
            if (kvs.isMissingNode() || !kvs.isArray()) {
                return mappings;
            }

            for (JsonNode kv : kvs) {
                String b64Key = kv.path("key").asText();
                if (b64Key == null || b64Key.isEmpty()) {
                    continue;
                }

                String key;
                try {
                    key = new String(Base64.getDecoder().decode(b64Key), StandardCharsets.UTF_8);
                } catch (Exception e) {
                    continue;
                }

                Matcher m = CHANNEL_CP_PATTERN.matcher(key);
                if (m.matches()) {
                    String pchannel = m.group(1);
                    long collectionId;
                    try {
                        collectionId = Long.parseLong(m.group(2));
                    } catch (NumberFormatException e) {
                        continue;
                    }
                    mappings.put(collectionId, pchannel);
                }
            }

            log.info("Parsed {} collection→pchannel mappings from etcd", mappings.size());
        } catch (Exception e) {
            log.warn("Failed to parse etcd response with Jackson: {}", e.getMessage());
        }

        return mappings;
    }

    private static String base64Encode(String s) {
        return Base64.getEncoder().encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    // ---- TLS & Auth helpers ----

    /**
     * Build an HTTP client with optional TLS/mTLS via {@link SslUtil}.
     *
     * <p>Fix: when mTLS (client cert+key) is configured without a CA cert,
     * the SSL context still uses the system trust store instead of being
     * silently downgraded to plaintext.
     */
    private static HttpClient buildHttpClient(String caPath, String clientCertPath,
                                               String clientKeyPath) {
        try {
            HttpClient.Builder builder = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(5));

            if (SslUtil.hasTlsConfig(caPath, clientCertPath, clientKeyPath)) {
                javax.net.ssl.SSLContext sslContext =
                        SslUtil.createSslContext(caPath, clientCertPath, clientKeyPath);
                builder.sslContext(sslContext);
            }

            return builder.build();
        } catch (Exception e) {
            log.error("Failed to build etcd HTTPS client: {}", e.getMessage());
            return HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        }
    }

    private static String buildAuthHeader(String username, String password) {
        if (username != null && !username.isEmpty() && password != null) {
            String creds = username + ":" + password;
            return "Basic " + Base64.getEncoder()
                    .encodeToString(creds.getBytes(StandardCharsets.UTF_8));
        }
        return null;
    }
}
