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

// @anomaly-detection.create-detector Commented this code until we have support of Common Utils for extensibility
//import static org.opensearch.commons.ConfigConstants.OPENSEARCH_SECURITY_SSL_HTTP_ENABLED;
//import static org.opensearch.commons.ConfigConstants.OPENSEARCH_SECURITY_SSL_HTTP_KEYSTORE_FILEPATH;
//import static org.opensearch.commons.ConfigConstants.OPENSEARCH_SECURITY_SSL_HTTP_KEYSTORE_KEYPASSWORD;
//import static org.opensearch.commons.ConfigConstants.OPENSEARCH_SECURITY_SSL_HTTP_KEYSTORE_PASSWORD;
//import static org.opensearch.commons.ConfigConstants.OPENSEARCH_SECURITY_SSL_HTTP_PEMCERT_FILEPATH;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.After;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.RestStatus;

/**
 * ODFE integration test base class to support both security disabled and enabled ODFE cluster.
 */
public abstract class ODFERestTestCase extends ADExtensionIntegTestCase {

    protected boolean isHttps() {
        boolean isHttps = Optional.ofNullable(System.getProperty("https")).map("true"::equalsIgnoreCase).orElse(false);
        if (isHttps) {
            // currently only external cluster is supported for security enabled testing
            if (!Optional.ofNullable(System.getProperty("tests.rest.cluster")).isPresent()) {
                throw new RuntimeException("cluster url should be provided for security enabled testing");
            }
        }

        return isHttps;
    }

    // Utility fn for deleting indices. Should only be used when not allowed in a regular context
    // (e.g., deleting system indices)
    protected static void deleteIndexWithAdminClient(String name) throws IOException {
        Request request = new Request("DELETE", "/" + name);
        sdkAdminClient().performRequest(request);
    }

    // Utility fn for checking if an index exists. Should only be used when not allowed in a regular context
    // (e.g., checking existence of system indices)
    protected static boolean indexExistsWithAdminClient(String indexName) throws IOException {
        Request request = new Request("HEAD", "/" + indexName);
        Response response = sdkAdminClient().performRequest(request);
        return RestStatus.OK.getStatus() == response.getStatusLine().getStatusCode();
    }

    @SuppressWarnings("unchecked")
    @After
    protected void wipeAllODFEIndices() throws IOException {
        Response response = sdkAdminClient().performRequest(new Request("GET", "/_cat/indices?format=json&expand_wildcards=all"));
        XContentType xContentType = XContentType.fromMediaType(response.getEntity().getContentType());
        try (
            XContentParser parser = xContentType
                .xContent()
                .createParser(
                    NamedXContentRegistry.EMPTY,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    response.getEntity().getContent()
                )
        ) {
            XContentParser.Token token = parser.nextToken();
            List<Map<String, Object>> parserList = null;
            if (token == XContentParser.Token.START_ARRAY) {
                parserList = parser.listOrderedMap().stream().map(obj -> (Map<String, Object>) obj).collect(Collectors.toList());
            } else {
                parserList = Collections.singletonList(parser.mapOrdered());
            }

            for (Map<String, Object> index : parserList) {
                String indexName = (String) index.get("index");
                if (indexName != null && !".opendistro_security".equals(indexName)) {
                    sdkAdminClient().performRequest(new Request("DELETE", "/" + indexName));
                }
            }
        }
    }
}
