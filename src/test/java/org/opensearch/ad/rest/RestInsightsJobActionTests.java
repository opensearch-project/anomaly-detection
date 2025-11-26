/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ad.rest;

import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import org.junit.Before;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.transport.client.node.NodeClient;

public class RestInsightsJobActionTests extends OpenSearchTestCase {

    private TestThreadPool threadPool;
    private ClusterService clusterService;

    @Before
    public void setUpThreadPool() {
        threadPool = new TestThreadPool(getClass().getSimpleName());
    }

    @Override
    public void tearDown() throws Exception {
        try {
            if (clusterService != null) {
                clusterService.close();
                clusterService = null;
            }
            if (threadPool != null) {
                ThreadPool.terminate(threadPool, 30, java.util.concurrent.TimeUnit.SECONDS);
                threadPool = null;
            }
        } finally {
            super.tearDown();
        }
    }

    public void testPrepareRequestThrowsWhenInsightsDisabled() throws IOException {
        Settings settings = Settings
            .builder()
            .put(AnomalyDetectorSettings.AD_REQUEST_TIMEOUT.getKey(), TimeValue.timeValueSeconds(10))
            .build();

        Set<Setting<?>> clusterSettingSet = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        clusterSettingSet.add(AnomalyDetectorSettings.AD_REQUEST_TIMEOUT);
        clusterSettingSet.add(AnomalyDetectorSettings.INSIGHTS_ENABLED);
        ClusterSettings clusterSettings = new ClusterSettings(settings, clusterSettingSet);
        clusterService = org.opensearch.timeseries.TestHelpers.createClusterService(threadPool, clusterSettings);

        RestInsightsJobAction action = new RestInsightsJobAction(settings, clusterService);

        FakeRestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
            .withMethod(RestRequest.Method.POST)
            .withPath(String.format(Locale.ROOT, "%s/insights/_start", TimeSeriesAnalyticsPlugin.AD_BASE_URI))
            .build();

        expectThrows(IllegalStateException.class, () -> action.prepareRequest(request, mock(NodeClient.class)));
    }

    public void testPrepareRequestStartPathWhenEnabled() throws IOException {
        Settings settings = Settings
            .builder()
            .put(AnomalyDetectorSettings.AD_REQUEST_TIMEOUT.getKey(), TimeValue.timeValueSeconds(10))
            .put(AnomalyDetectorSettings.INSIGHTS_ENABLED.getKey(), true)
            .build();

        Set<Setting<?>> clusterSettingSet = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        clusterSettingSet.add(AnomalyDetectorSettings.AD_REQUEST_TIMEOUT);
        clusterSettingSet.add(AnomalyDetectorSettings.INSIGHTS_ENABLED);
        ClusterSettings clusterSettings = new ClusterSettings(settings, clusterSettingSet);
        clusterService = org.opensearch.timeseries.TestHelpers.createClusterService(threadPool, clusterSettings);

        RestInsightsJobAction action = new RestInsightsJobAction(settings, clusterService);

        FakeRestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
            .withMethod(RestRequest.Method.POST)
            .withPath(String.format(Locale.ROOT, "%s/insights/_start", TimeSeriesAnalyticsPlugin.AD_BASE_URI))
            .withContent(new BytesArray("{\"frequency\":\"12h\"}"), org.opensearch.common.xcontent.XContentType.JSON)
            .build();

        // Should not throw when flag enabled
        assertNotNull(action.prepareRequest(request, mock(NodeClient.class)));
    }

    public void testPrepareRequestResultsPathWhenEnabled() throws IOException {
        Settings settings = Settings
            .builder()
            .put(AnomalyDetectorSettings.AD_REQUEST_TIMEOUT.getKey(), TimeValue.timeValueSeconds(10))
            .put(AnomalyDetectorSettings.INSIGHTS_ENABLED.getKey(), true)
            .build();

        Set<Setting<?>> clusterSettingSet = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        clusterSettingSet.add(AnomalyDetectorSettings.AD_REQUEST_TIMEOUT);
        clusterSettingSet.add(AnomalyDetectorSettings.INSIGHTS_ENABLED);
        ClusterSettings clusterSettings = new ClusterSettings(settings, clusterSettingSet);
        clusterService = org.opensearch.timeseries.TestHelpers.createClusterService(threadPool, clusterSettings);

        RestInsightsJobAction action = new RestInsightsJobAction(settings, clusterService);

        FakeRestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
            .withMethod(RestRequest.Method.GET)
            .withPath(String.format(Locale.ROOT, "%s/insights/_results", TimeSeriesAnalyticsPlugin.AD_BASE_URI))
            .withParams(java.util.Map.of("from", "0", "size", "10"))
            .build();

        assertNotNull(action.prepareRequest(request, mock(NodeClient.class)));
    }

    public void testPrepareRequestStopPathWhenEnabled() throws IOException {
        Settings settings = Settings
            .builder()
            .put(AnomalyDetectorSettings.AD_REQUEST_TIMEOUT.getKey(), TimeValue.timeValueSeconds(10))
            .put(AnomalyDetectorSettings.INSIGHTS_ENABLED.getKey(), true)
            .build();

        Set<Setting<?>> clusterSettingSet = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        clusterSettingSet.add(AnomalyDetectorSettings.AD_REQUEST_TIMEOUT);
        clusterSettingSet.add(AnomalyDetectorSettings.INSIGHTS_ENABLED);
        ClusterSettings clusterSettings = new ClusterSettings(settings, clusterSettingSet);
        clusterService = org.opensearch.timeseries.TestHelpers.createClusterService(threadPool, clusterSettings);

        RestInsightsJobAction action = new RestInsightsJobAction(settings, clusterService);

        FakeRestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
            .withMethod(RestRequest.Method.POST)
            .withPath(String.format(Locale.ROOT, "%s/insights/_stop", TimeSeriesAnalyticsPlugin.AD_BASE_URI))
            .build();

        assertNotNull(action.prepareRequest(request, mock(NodeClient.class)));
    }
}
