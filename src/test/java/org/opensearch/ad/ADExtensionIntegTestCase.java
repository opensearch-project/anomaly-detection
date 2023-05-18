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
import java.util.Map;

import org.opensearch.client.Response;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.sdk.Extension;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.SDKClient.SDKClusterAdminClient;
import org.opensearch.sdk.SDKClient.SDKRestClient;
import org.opensearch.sdk.SDKClusterService;
import org.opensearch.test.OpenSearchIntegTestCase;

public abstract class ADExtensionIntegTestCase extends OpenSearchIntegTestCase {

    private static AnomalyDetectorExtension extension = new AnomalyDetectorExtension();
    private static SDKRestClient sdkRestClient = null;
    private static SDKClusterService sdkClusterService = null;
    private static ExtensionsRunner extensionsRunner = null;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        extensionsRunner = new ExtensionsRunnerForTests(extension);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (sdkRestClient != null) {
            sdkRestClient.close();
        }
    }

    public static SDKRestClient createSDKRestClient() {
        return extensionsRunner.getSdkClient().initializeRestClient();
    }

    public static synchronized SDKRestClient sdkClient() {
        if (sdkRestClient == null) {
            sdkRestClient = createSDKRestClient();
        }
        return sdkRestClient;
    }

    public static SDKRestClient sdkAdminClient() {
        return sdkClient().admin();
    }

    public static SDKClusterAdminClient sdkClusterAdmin() {
        return sdkClient().cluster();
    }

    public static synchronized SDKClusterService sdkClusterService() {
        if (sdkClusterService == null) {
            sdkClusterService = extensionsRunner.getSdkClusterService();
        }
        return sdkClusterService;
    }

    protected static Map<String, Object> responseAsMap(Response response) throws IOException {
        XContentType entityContentType = XContentType.fromMediaType(response.getEntity().getContentType());
        Map<String, Object> responseEntity = XContentHelper
            .convertToMap(entityContentType.xContent(), response.getEntity().getContent(), false);
        assertNotNull(responseEntity);
        return responseEntity;
    }

    /**
     * Convert the entity from a {@link Response} into a map of maps.
     */
    public static Map<String, Object> entityAsMap(Response response) throws IOException {
        XContentType xContentType = XContentType.fromMediaType(response.getEntity().getContentType());
        // EMPTY and THROW are fine here because `.map` doesn't use named x content or deprecation
        try (
            XContentParser parser = xContentType
                .xContent()
                .createParser(
                    NamedXContentRegistry.EMPTY,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    response.getEntity().getContent()
                )
        ) {
            return parser.map();
        }
    }

    private static class ExtensionsRunnerForTests extends ExtensionsRunner {

        protected ExtensionsRunnerForTests(Extension extension) throws IOException {
            super(extension);
        }

    }
}
