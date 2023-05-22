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

import java.io.IOException;

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
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.sdk.Extension;
import org.opensearch.sdk.ExtensionSettings;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.SDKClient;
import org.opensearch.sdk.SDKClient.SDKClusterAdminClient;
import org.opensearch.sdk.SDKClient.SDKRestClient;
import org.opensearch.sdk.SDKClusterService;
import org.opensearch.test.rest.OpenSearchRestTestCase;

public abstract class ADExtensionIntegTestCase extends OpenSearchRestTestCase {

    private static final String EXTENSION_SETTINGS_PATH = "/ad-extension.yml";

    private static AnomalyDetectorExtension extension = new AnomalyDetectorExtension();
    private static SDKClient sdkClient = null;
    private static SDKRestClient sdkRestClient = null;
    private static SDKClusterService sdkClusterService = null;
    private static ExtensionsRunner extensionsRunner = null;
    private static ExtensionSettings extensionSettings = null;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        extensionsRunner = new ExtensionsRunnerForTests(extension);
        extensionSettings = ExtensionSettings.readSettingsFromYaml(EXTENSION_SETTINGS_PATH);

    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (sdkRestClient != null) {
            sdkRestClient.close();
        }
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

    @SuppressWarnings("rawtypes")
    private static SDKClient createSDKClient() {
        SDKClient sdkClient = new SDKClient(extensionSettings);
        // TODO : initilize sdkClient with actions

        return sdkClient;
    }

    private static SDKClient sdkClient() {
        if (sdkClient == null) {
            sdkClient = createSDKClient();
        }
        return sdkClient;
    }

    private static SDKRestClient createSDKRestClient() {
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(
            builder(extensionSettings.getOpensearchAddress(), Integer.parseInt(extensionSettings.getOpensearchPort()))
        );
        return new SDKRestClient(sdkClient(), restHighLevelClient);
    }

    public static synchronized SDKRestClient sdkRestClient() {
        if (sdkRestClient == null) {
            sdkRestClient = createSDKRestClient();
        }
        return sdkRestClient;
    }

    public static SDKRestClient sdkAdminClient() {
        return sdkRestClient().admin();
    }

    public static SDKClusterAdminClient sdkClusterAdmin() {
        return sdkRestClient().cluster();
    }

    public static synchronized SDKClusterService sdkClusterService() {
        if (sdkClusterService == null) {
            sdkClusterService = extensionsRunner.getSdkClusterService();
        }
        return sdkClusterService;
    }

    private static class ExtensionsRunnerForTests extends ExtensionsRunner {

        protected ExtensionsRunnerForTests(Extension extension) throws IOException {
            super(extension);
        }

    }
}
