/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.ad.bwc;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.util.RestHandlerUtils;
import org.opensearch.client.Response;
import org.opensearch.common.settings.Settings;
import org.opensearch.rest.RestStatus;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import com.google.common.collect.ImmutableMap;

public class ADBackwardsCompatibilityIT extends OpenSearchRestTestCase {

    private static final ClusterType CLUSTER_TYPE = ClusterType.parse(System.getProperty("tests.rest.bwcsuite"));
    private static final String CLUSTER_NAME = System.getProperty("tests.clustername");

    @Override
    protected final boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Override
    protected final boolean preserveReposUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveTemplatesUponCompletion() {
        return true;
    }

    @Override
    protected final Settings restClientSettings() {
        return Settings
            .builder()
            .put(super.restClientSettings())
            // increase the timeout here to 90 seconds to handle long waits for a green
            // cluster health. the waits for green need to be longer than a minute to
            // account for delayed shards
            .put(OpenSearchRestTestCase.CLIENT_SOCKET_TIMEOUT, "90s")
            .build();
    }

    @SuppressWarnings("unchecked")
    public void testPluginUpgradeInAMixedCluster() throws Exception {
        Map<String, Map<String, Object>> responseMap = (Map<String, Map<String, Object>>) getAsMap("_nodes/" + CLUSTER_NAME + "-0/plugins")
            .get("nodes");
        for (Map<String, Object> response : responseMap.values()) {
            List<Map<String, Object>> plugins = (List<Map<String, Object>>) response.get("plugins");
            Set<Object> pluginNames = plugins.stream().map(map -> map.get("name")).collect(Collectors.toSet());
            switch (CLUSTER_TYPE) {
                case OLD:
                    Assert.assertTrue(pluginNames.contains("opendistro-anomaly-detection"));
                    Assert.assertTrue(pluginNames.contains("opendistro-job-scheduler"));
                    createBasicAnomalyDetector();
                    break;
                case MIXED:
                    Assert.assertTrue(pluginNames.contains("opensearch-anomaly-detection"));
                    Assert.assertTrue(pluginNames.contains("opensearch-job-scheduler"));
                    verifyAnomalyDetector(TestHelpers.LEGACY_OPENDISTRO_AD_BASE_DETECTORS_URI);
                    break;
            }
            break;
        }
    }

    private enum ClusterType {
        OLD,
        MIXED;

        public static ClusterType parse(String value) {
            switch (value) {
                case "old_cluster":
                    return OLD;
                case "mixed_cluster":
                    return MIXED;
                default:
                    throw new AssertionError("unknown cluster type: " + value);
            }
        }
    }

    private void createBasicAnomalyDetector() throws Exception {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null);
        String indexName = detector.getIndices().get(0);
        TestHelpers.createIndex(client(), indexName, TestHelpers.toHttpEntity("{\"name\": \"test\"}"));

        Response response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.LEGACY_OPENDISTRO_AD_BASE_DETECTORS_URI,
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(detector),
                null
            );
        // verify that the detector is created
        assertEquals("Create anomaly detector failed", RestStatus.CREATED, TestHelpers.restStatus(response));
        Map<String, Object> responseMap = entityAsMap(response);
        String id = (String) responseMap.get("_id");
        int version = (int) responseMap.get("_version");
        assertNotEquals("response is missing Id", AnomalyDetector.NO_ID, id);
        assertTrue("incorrect version", version > 0);
    }

    private void verifyAnomalyDetector(String uri) throws Exception {
        Response response = TestHelpers.makeRequest(client(), "GET", uri + "/" + RestHandlerUtils.COUNT, null, "", null);
        Map<String, Object> responseMap = entityAsMap(response);
        Integer count = (Integer) responseMap.get("count");
        assertEquals(1, (long) count);
    }

}
