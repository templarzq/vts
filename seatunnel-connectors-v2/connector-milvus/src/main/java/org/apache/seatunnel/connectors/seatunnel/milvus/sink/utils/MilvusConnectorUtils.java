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

package org.apache.seatunnel.connectors.seatunnel.milvus.sink.utils;

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.type.CommonOptions;
import org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusCommonConfig;
import org.apache.seatunnel.connectors.seatunnel.milvus.source.config.MilvusSourceConfig;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Slf4j
public class MilvusConnectorUtils {

    public static Boolean hasPartitionKey(DescribeCollectionResp describeCollectionResp) {
        return describeCollectionResp.getCollectionSchema().getFieldSchemaList().stream()
                .anyMatch(CreateCollectionReq.FieldSchema::getIsPartitionKey);
    }

    public static String getDynamicField(CatalogTable catalogTable) {
        List<Column> columns = catalogTable.getTableSchema().getColumns();
        Column dynamicField = null;
        for (Column column : columns) {
            if (column.getOptions() != null
                    && column.getOptions().containsKey(CommonOptions.METADATA.getName())
                    && (Boolean) column.getOptions().get(CommonOptions.METADATA.getName())) {
                // skip dynamic field
                dynamicField = column;
            }
        }
        return dynamicField == null ? null : dynamicField.getName();
    }

    public static List<String> getJsonField(CatalogTable catalogTable) {
        List<Column> columns = catalogTable.getTableSchema().getColumns();
        List<String> jsonColumn = new ArrayList<>();
        for (Column column : columns) {
            if (column.getOptions() != null
                    && column.getOptions().containsKey(CommonOptions.JSON.getName())
                    && (Boolean) column.getOptions().get(CommonOptions.JSON.getName())) {
                // skip dynamic field
                jsonColumn.add(column.getName());
            }
        }
        return jsonColumn;
    }

    public static Boolean enableAutoId(MilvusClientV2 milvusClient, String collectionName) {
        DescribeCollectionResp describeCollectionResp =
                milvusClient.describeCollection(
                        DescribeCollectionReq.builder().collectionName(collectionName).build());
        return describeCollectionResp.getAutoID();
    }

    public static Boolean enableDynamicSchema(MilvusClientV2 milvusClient, String collectionName) {
        DescribeCollectionResp describeCollectionResp =
                milvusClient.describeCollection(
                        DescribeCollectionReq.builder().collectionName(collectionName).build());
        return describeCollectionResp.getEnableDynamicField();
    }

    public static ConnectConfig getConnectConfig(ReadonlyConfig config) {
        ConnectConfig connectConfig = ConnectConfig.builder()
                .uri(config.get(MilvusSourceConfig.URL))
                .token(config.get(MilvusSourceConfig.TOKEN))
                .dbName(config.get(MilvusSourceConfig.DATABASE))
                .connectTimeoutMs(30000)
                .build();
        if(config.get(MilvusCommonConfig.CLIENT_PEM_PATH) != null){
            connectConfig.setClientPemPath(config.get(MilvusCommonConfig.CLIENT_PEM_PATH));
        }
        if(config.get(MilvusCommonConfig.CLIENT_KEY_PATH) != null){
            connectConfig.setClientKeyPath(config.get(MilvusCommonConfig.CLIENT_KEY_PATH));
        }
        if(config.get(MilvusCommonConfig.CA_PEM_PATH) != null){
            String caPath = config.get(MilvusCommonConfig.CA_PEM_PATH);
            connectConfig.setServerPemPath(caPath);
            connectConfig.setCaPemPath(caPath);
            // Milvus SDK 2.6.8 + gRPC 1.59.1 shaded Netty:
            // GrpcSslContexts.forClient().trustManager(File) does not correctly
            // trust the custom CA cert — JDK's X509TrustManagerImpl still validates
            // with the system trust store and rejects self-signed/private CA certs.
            // Workaround: register the CA cert into the JVM's default SSLContext
            // so the SDK's built-in trust path can validate server certs.
            registerCaCert(caPath);
        }
        if(config.get(MilvusCommonConfig.SERVER_NAME) != null){
            connectConfig.setServerName(config.get(MilvusCommonConfig.SERVER_NAME));
        }
        return connectConfig;
    }

    /**
     * Register a PEM CA certificate into the JVM default SSLContext so that
     * the Milvus SDK gRPC client (which relies on default trust managers)
     * can validate server certificates signed by this CA.
     *
     * <p>This works around a bug in Milvus SDK 2.6.8 + gRPC 1.59.1 shaded
     * Netty where {@code GrpcSslContexts.forClient().trustManager(File)}
     * does not correctly override the JDK trust manager, causing PKIX path
     * building failures for self-signed or private CA certificates.
     */
    private static synchronized void registerCaCert(String caPemPath) {
        try {
            // Load the CA certificate from PEM
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            X509Certificate caCert;
            try (InputStream is = new FileInputStream(caPemPath)) {
                caCert = (X509Certificate) cf.generateCertificate(is);
            }

            // Build a KeyStore with the CA cert
            KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
            ks.load(null, null);
            ks.setCertificateEntry("milvus-ca", caCert);

            // Create a TrustManagerFactory from this keystore
            TrustManagerFactory customTmf =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            customTmf.init(ks);

            // Get the default TrustManager
            TrustManagerFactory defaultTmf =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            defaultTmf.init((KeyStore) null);

            // Build a composite X509TrustManager that tries custom CA first,
            // then falls back to system defaults
            List<X509TrustManager> managers = new ArrayList<>();
            for (TrustManager tm : customTmf.getTrustManagers()) {
                if (tm instanceof X509TrustManager) {
                    managers.add((X509TrustManager) tm);
                }
            }
            X509TrustManager defaultTm = null;
            for (TrustManager tm : defaultTmf.getTrustManagers()) {
                if (tm instanceof X509TrustManager) {
                    defaultTm = (X509TrustManager) tm;
                    break;
                }
            }

            X509TrustManager composite = new CompositeX509TrustManager(managers, defaultTm);
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, new TrustManager[] {composite}, null);
            SSLContext.setDefault(sslContext);

            log.info("Registered CA cert into JVM default SSLContext: {}", caPemPath);
        } catch (Exception e) {
            log.warn("Failed to register CA cert (TLS may fail): {}", e.getMessage());
        }
    }

    /**
     * Composite {@link X509TrustManager} that tries multiple trust managers.
     * Accepts a cert if ANY of the delegates accepts it.
     */
    private static class CompositeX509TrustManager implements X509TrustManager {
        private final List<X509TrustManager> delegates;
        private final X509TrustManager defaultTm;

        CompositeX509TrustManager(List<X509TrustManager> delegates, X509TrustManager defaultTm) {
            this.delegates = delegates;
            this.defaultTm = defaultTm;
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType)
                throws java.security.cert.CertificateException {
            if (defaultTm != null) {
                defaultTm.checkClientTrusted(chain, authType);
            }
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType)
                throws java.security.cert.CertificateException {
            for (X509TrustManager tm : delegates) {
                try {
                    tm.checkServerTrusted(chain, authType);
                    return;
                } catch (java.security.cert.CertificateException ignored) {
                }
            }
            if (defaultTm != null) {
                defaultTm.checkServerTrusted(chain, authType);
            }
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            List<X509Certificate> certs = new ArrayList<>();
            for (X509TrustManager tm : delegates) {
                certs.addAll(Arrays.asList(tm.getAcceptedIssuers()));
            }
            if (defaultTm != null) {
                certs.addAll(Arrays.asList(defaultTm.getAcceptedIssuers()));
            }
            return certs.toArray(new X509Certificate[0]);
        }
    }
}
