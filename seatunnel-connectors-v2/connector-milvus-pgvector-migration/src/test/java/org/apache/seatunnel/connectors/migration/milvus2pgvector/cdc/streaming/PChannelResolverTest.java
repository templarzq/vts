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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link PChannelResolver} — etcd channel-cp response parsing,
 * pchannel resolution, and TLS/auth configuration.
 */
@DisplayName("PChannelResolver - etcd Connection & Channel Resolution Tests")
class PChannelResolverTest {

    private PChannelResolver resolver;

    @BeforeEach
    void setUp() {
        resolver = new PChannelResolver("http://localhost:2379", "by-dev");
    }

    // ---- parseChannelCpResponse ----

    @Test
    @DisplayName("parseChannelCpResponse returns empty map for empty JSON")
    void testParseEmptyResponse() {
        Map<Long, String> result = resolver.parseChannelCpResponse("{}");
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    @DisplayName("parseChannelCpResponse returns empty map when kvs array is missing")
    void testParseMissingKvs() {
        Map<Long, String> result = resolver.parseChannelCpResponse(
                "{\"count\":0,\"header\":{}}");
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    @DisplayName("parseChannelCpResponse returns empty map for null JSON")
    void testParseNullResponse() {
        Map<Long, String> result = resolver.parseChannelCpResponse("");
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    @DisplayName("parseChannelCpResponse parses valid single pchannel→collection mapping")
    void testParseSingleMapping() throws Exception {
        // Build mock etcd KV range response:
        // key: by-dev/meta/datacoord-meta/channel-cp/by-dev-rootcoord-dml_0_4512345678v0
        // This maps collectionId=4512345678 → pchannel=by-dev-rootcoord-dml_0
        String etcdKey = "by-dev/meta/datacoord-meta/channel-cp/by-dev-rootcoord-dml_0_4512345678v0";
        String etcdKeyB64 = Base64.getEncoder()
                .encodeToString(etcdKey.getBytes(StandardCharsets.UTF_8));

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode root = mapper.createObjectNode();
        ArrayNode kvs = mapper.createArrayNode();
        ObjectNode kv = mapper.createObjectNode();
        kv.put("key", etcdKeyB64);
        kv.put("value", Base64.getEncoder()
                .encodeToString("{}".getBytes(StandardCharsets.UTF_8)));
        kvs.add(kv);
        root.set("kvs", kvs);

        String responseBody = mapper.writeValueAsString(root);
        Map<Long, String> result = resolver.parseChannelCpResponse(responseBody);

        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("by-dev-rootcoord-dml_0", result.get(4512345678L));
    }

    @Test
    @DisplayName("parseChannelCpResponse parses multiple pchannel→collection mappings")
    void testParseMultipleMappings() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode root = mapper.createObjectNode();
        ArrayNode kvs = mapper.createArrayNode();

        // Mapping 1: collectionId=1 → by-dev-rootcoord-dml_0
        String key1 = "by-dev/meta/datacoord-meta/channel-cp/by-dev-rootcoord-dml_0_1v0";
        addKvEntry(kvs, key1, "{}");

        // Mapping 2: collectionId=2 → by-dev-rootcoord-dml_0  (same pchannel)
        String key2 = "by-dev/meta/datacoord-meta/channel-cp/by-dev-rootcoord-dml_0_2v0";
        addKvEntry(kvs, key2, "{}");

        // Mapping 3: collectionId=100 → by-dev-rootcoord-dml_5  (different pchannel)
        String key3 = "by-dev/meta/datacoord-meta/channel-cp/by-dev-rootcoord-dml_5_100v0";
        addKvEntry(kvs, key3, "{}");

        root.set("kvs", kvs);
        String responseBody = mapper.writeValueAsString(root);

        Map<Long, String> result = resolver.parseChannelCpResponse(responseBody);

        assertNotNull(result);
        assertEquals(3, result.size());
        assertEquals("by-dev-rootcoord-dml_0", result.get(1L));
        assertEquals("by-dev-rootcoord-dml_0", result.get(2L));
        assertEquals("by-dev-rootcoord-dml_5", result.get(100L));
    }

    @Test
    @DisplayName("parseChannelCpResponse skips keys not matching channel-cp pattern")
    void testParseSkipsNonChannelCpKeys() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode root = mapper.createObjectNode();
        ArrayNode kvs = mapper.createArrayNode();

        // Valid channel-cp key
        String validKey = "by-dev/meta/datacoord-meta/channel-cp/by-dev-rootcoord-dml_0_42v0";
        addKvEntry(kvs, validKey, "{}");

        // Invalid key — not matching pattern
        String invalidKey = "by-dev/meta/session/session-123";
        addKvEntry(kvs, invalidKey, "{}");

        // Another invalid key — non-numeric collection ID
        String invalidKey2 = "by-dev/meta/datacoord-meta/channel-cp/by-dev-rootcoord-dml_0_ABCv0";
        addKvEntry(kvs, invalidKey2, "{}");

        root.set("kvs", kvs);
        String responseBody = mapper.writeValueAsString(root);

        Map<Long, String> result = resolver.parseChannelCpResponse(responseBody);

        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("by-dev-rootcoord-dml_0", result.get(42L));
    }

    @Test
    @DisplayName("parseChannelCpResponse skips entries with empty key")
    void testParseSkipsEmptyKey() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode root = mapper.createObjectNode();
        ArrayNode kvs = mapper.createArrayNode();
        ObjectNode kv = mapper.createObjectNode();
        kv.put("key", "");
        kv.put("value", Base64.getEncoder()
                .encodeToString("{}".getBytes(StandardCharsets.UTF_8)));
        kvs.add(kv);
        root.set("kvs", kvs);

        String responseBody = mapper.writeValueAsString(root);
        Map<Long, String> result = resolver.parseChannelCpResponse(responseBody);

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    @DisplayName("parseChannelCpResponse skips entries with invalid base64 key")
    void testParseSkipsInvalidBase64() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode root = mapper.createObjectNode();
        ArrayNode kvs = mapper.createArrayNode();
        ObjectNode kv = mapper.createObjectNode();
        kv.put("key", "!!!not-valid-base64!!!");
        kv.put("value", Base64.getEncoder()
                .encodeToString("{}".getBytes(StandardCharsets.UTF_8)));
        kvs.add(kv);
        root.set("kvs", kvs);

        String responseBody = mapper.writeValueAsString(root);
        Map<Long, String> result = resolver.parseChannelCpResponse(responseBody);

        // Invalid base64 key is silently skipped
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    @DisplayName("parseChannelCpResponse skips invalid (negative) collection IDs")
    void testParseSkipsNegativeCollectionId() throws Exception {
        // Negative collection IDs do not match the \d+ pattern and are correctly skipped
        String etcdKey = "by-dev/meta/datacoord-meta/channel-cp/by-dev-rootcoord-dml_0_-1v0";
        String etcdKeyB64 = Base64.getEncoder()
                .encodeToString(etcdKey.getBytes(StandardCharsets.UTF_8));

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode root = mapper.createObjectNode();
        ArrayNode kvs = mapper.createArrayNode();
        ObjectNode kv = mapper.createObjectNode();
        kv.put("key", etcdKeyB64);
        kv.put("value", Base64.getEncoder()
                .encodeToString("{}".getBytes(StandardCharsets.UTF_8)));
        kvs.add(kv);
        root.set("kvs", kvs);

        String responseBody = mapper.writeValueAsString(root);
        Map<Long, String> result = resolver.parseChannelCpResponse(responseBody);

        // Negative collectionId doesn't match \d+ pattern → skipped
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    @DisplayName("parseChannelCpResponse extracts pchannel with custom root path")
    void testParseCustomRootPath() throws Exception {
        // Use custom root path "my-cluster"
        PChannelResolver customResolver = new PChannelResolver(
                "http://localhost:2379", "my-cluster");
        String etcdKey = "my-cluster/meta/datacoord-meta/channel-cp/my-cluster-rootcoord-dml_3_999v0";
        String etcdKeyB64 = Base64.getEncoder()
                .encodeToString(etcdKey.getBytes(StandardCharsets.UTF_8));

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode root = mapper.createObjectNode();
        ArrayNode kvs = mapper.createArrayNode();
        ObjectNode kv = mapper.createObjectNode();
        kv.put("key", etcdKeyB64);
        kv.put("value", Base64.getEncoder()
                .encodeToString("{}".getBytes(StandardCharsets.UTF_8)));
        kvs.add(kv);
        root.set("kvs", kvs);

        String responseBody = mapper.writeValueAsString(root);
        Map<Long, String> result = customResolver.parseChannelCpResponse(responseBody);

        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("my-cluster-rootcoord-dml_3", result.get(999L));
    }

    // ---- Constructor: TLS + auth ----

    @Test
    @DisplayName("Full constructor accepts null TLS and auth parameters without error")
    void testFullConstructorWithNulls() {
        PChannelResolver r = new PChannelResolver(
                "http://localhost:2379",  // etcdEndpoint
                "by-dev",                 // rootPath
                null,                     // caPath
                null,                     // clientCertPath
                null,                     // clientKeyPath
                null,                     // username
                null);                    // password
        assertNotNull(r);
    }

    @Test
    @DisplayName("Full constructor with empty etcd endpoint handles gracefully")
    void testFullConstructorEmptyEndpoint() {
        PChannelResolver r = new PChannelResolver(
                "",                       // etcdEndpoint (empty)
                "by-dev",
                null, null, null, null, null);
        assertNotNull(r);
        // With empty endpoint, resolvePChannel returns null without making HTTP call
        assertNull(r.resolvePChannel(1L));
    }

    @Test
    @DisplayName("Full constructor accepts null etcd endpoint")
    void testFullConstructorNullEndpoint() {
        PChannelResolver r = new PChannelResolver(
                null,                     // etcdEndpoint (null)
                "by-dev",
                null, null, null, null, null);
        assertNotNull(r);
        assertNull(r.resolvePChannel(1L));
    }

    // ---- resolveVChannel ----

    @Test
    @DisplayName("resolveVChannel returns null when pchannel is not resolved")
    void testResolveVChannelNoEndpoint() {
        PChannelResolver r = new PChannelResolver(null, "by-dev");
        assertNull(r.resolveVChannel(42L));
    }

    // ---- helpers ----

    private static void addKvEntry(ArrayNode kvs, String key, String value) {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode kv = mapper.createObjectNode();
        kv.put("key", Base64.getEncoder()
                .encodeToString(key.getBytes(StandardCharsets.UTF_8)));
        kv.put("value", Base64.getEncoder()
                .encodeToString(value.getBytes(StandardCharsets.UTF_8)));
        kvs.add(kv);
    }
}
