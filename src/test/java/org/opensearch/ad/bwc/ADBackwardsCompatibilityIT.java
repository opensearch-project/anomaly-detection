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
import org.opensearch.ad.AnomalyDetectorRestTestCase;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.rest.OpenSearchRestTestCase;

public class ADBackwardsCompatibilityIT extends AnomalyDetectorRestTestCase {

    private static final ClusterType CLUSTER_TYPE = ClusterType.parse(System.getProperty("tests.rest.suite"));
    private static final String CLUSTER_NAME = System.getProperty("tests.cluster_name");

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
                    break;
                case MIXED:
                    Assert.assertTrue(pluginNames.contains("opensearch-anomaly-detection"));
                    Assert.assertTrue(pluginNames.contains("opensearch-job-scheduler"));
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

}
