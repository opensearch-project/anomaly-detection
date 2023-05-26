/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.ad;

import java.util.ArrayList;
import java.util.List;

import javax.net.ssl.SSLEngine;

import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.core5.function.Factory;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.hc.core5.reactor.ssl.TlsDetails;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.junit.After;
import org.junit.Before;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.sdk.ExtensionSettings;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.SDKClient;
import org.opensearch.sdk.SDKClient.SDKClusterAdminClient;
import org.opensearch.sdk.SDKClient.SDKRestClient;
import org.opensearch.sdk.SDKClusterService;
import org.opensearch.test.rest.OpenSearchRestTestCase;

public abstract class ADExtensionIntegTestCase extends OpenSearchRestTestCase {

    private static final String EXTENSION_SETTINGS_PATH = "/ad-extension.yml";

    private static ExtensionSettings extensionSettings;
    private static String openSearchIntegTestClusterHost;
    private static int openSearchIntegTestClusterPort;
    private static SDKClient sdkClient = null;
    private static SDKRestClient sdkRestClient = null;

    // TODO : Remove ExtensionsRunner
    private static ExtensionsRunner extensionsRunner = null;
    // TODO : Create new SDKClusterService
    private static SDKClusterService sdkClusterService = null;

    @Override
    @Before
    public void setUp() throws Exception {

        super.setUp();

        // Retrieve AD Extension Settings
        extensionSettings = ExtensionSettings.readSettingsFromYaml(EXTENSION_SETTINGS_PATH);

        // Determine opensearch integration test cluster host and port
        String cluster = getTestRestCluster();
        String[] stringUrls = cluster.split(",");
        List<HttpHost> hosts = new ArrayList<>(stringUrls.length);
        for (String stringUrl : stringUrls) {
            int portSeparator = stringUrl.lastIndexOf(':');
            if (portSeparator < 0) {
                throw new IllegalArgumentException("Illegal cluster url [" + stringUrl + "]");
            }
            String host = stringUrl.substring(0, portSeparator);
            int port = Integer.valueOf(stringUrl.substring(portSeparator + 1));
            hosts.add(buildHttpHost(host, port));
        }

        openSearchIntegTestClusterHost = hosts.get(1).getHostName();
        openSearchIntegTestClusterPort = hosts.get(1).getPort();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        if (sdkRestClient != null) {
            sdkRestClient.close();
        }
    }

    private static SDKClient sdkClient() {

        if (sdkClient == null) {
            // Set OpenSearch Address/Port to test cluster host/port
            ExtensionSettings integTestClusterExtensionSettings = new ExtensionSettings(
                extensionSettings.getExtensionName(),
                extensionSettings.getHostAddress(),
                extensionSettings.getHostPort(),
                openSearchIntegTestClusterHost,
                String.valueOf(openSearchIntegTestClusterPort)
            );
            sdkClient = new SDKClient(integTestClusterExtensionSettings);
        }
        return sdkClient;
    }

    public static SDKRestClient sdkRestClient() {
        if (sdkRestClient == null) {
            sdkRestClient = new SDKRestClient(
                sdkClient(),
                new RestHighLevelClient(builder(openSearchIntegTestClusterHost, openSearchIntegTestClusterPort))
            );
        }
        return sdkRestClient;
    }

    public static SDKRestClient sdkAdminClient() {
        return sdkRestClient().admin();
    }

    public static SDKClusterAdminClient sdkClusterAdmin() {
        return sdkRestClient().cluster();
    }

    public static SDKClusterService sdkClusterService() {
        if (sdkClusterService == null) {
            sdkClusterService = extensionsRunner.getSdkClusterService();
        }
        return sdkClusterService;
    }

    /**
     * Create and configure a RestClientBuilder
     *
     * @param hostAddress The address the client should connect to
     * @param port The port the client should connect to
     * @return An instance of the builder
     */
    private static RestClientBuilder builder(String hostAddress, int port) {
        RestClientBuilder builder = RestClient.builder(new HttpHost(hostAddress, port));
        builder.setStrictDeprecationMode(true);
        builder.setHttpClientConfigCallback(httpClientBuilder -> {
            try {
                final TlsStrategy tlsStrategy = ClientTlsStrategyBuilder
                    .create()
                    .setSslContext(SSLContextBuilder.create().loadTrustMaterial(null, (chains, authType) -> true).build())
                    // disable the certificate since our cluster currently just uses the default security
                    // configuration
                    .setHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                    // See please https://issues.apache.org/jira/browse/HTTPCLIENT-2219
                    .setTlsDetailsFactory(new Factory<SSLEngine, TlsDetails>() {
                        @Override
                        public TlsDetails create(final SSLEngine sslEngine) {
                            return new TlsDetails(sslEngine.getSession(), sslEngine.getApplicationProtocol());
                        }
                    })
                    .build();

                final PoolingAsyncClientConnectionManager connectionManager = PoolingAsyncClientConnectionManagerBuilder
                    .create()
                    .setTlsStrategy(tlsStrategy)
                    .build();
                return httpClientBuilder.setConnectionManager(connectionManager);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        return builder;
    }

}
