/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ad.indices;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;
import org.opensearch.transport.client.AdminClient;
import org.opensearch.transport.client.Client;

/**
 * Unit tests for {@link ADIndexManagement#validateInsightsResultIndexMapping} to satisfy jacoco per-class thresholds.
 * We override concrete-index resolution and mapping validation to avoid depending on cluster state.
 */
public class ADIndexManagementInsightsMappingTests extends OpenSearchTestCase {

    private Client client;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private DiscoveryNodeFilterer nodeFilter;

    private static class TestADIndexManagement extends ADIndexManagement {
        private String concreteIndex;
        private boolean validateReturn;

        TestADIndexManagement(
            Client client,
            ClusterService clusterService,
            ThreadPool threadPool,
            Settings settings,
            DiscoveryNodeFilterer nodeFilter,
            int maxUpdateRunningTimes,
            NamedXContentRegistry xContentRegistry
        )
            throws IOException {
            super(client, clusterService, threadPool, settings, nodeFilter, maxUpdateRunningTimes, xContentRegistry);
        }

        @Override
        protected void getConcreteIndex(String indexOrAliasName, ActionListener<String> thenDo) {
            thenDo.onResponse(concreteIndex);
        }

        @Override
        protected void validateIndexMapping(
            String concreteIndex,
            Map<String, Object> expectedFieldConfigs,
            String indexTypeNameForLog,
            ActionListener<Boolean> thenDo
        ) {
            thenDo.onResponse(validateReturn);
        }
    }

    @Before
    public void setUpMocks() {
        client = mock(Client.class);
        when(client.admin()).thenReturn(mock(AdminClient.class));
        clusterService = mock(ClusterService.class);
        // ADIndexManagement constructor registers update consumers on cluster settings, so provide a real ClusterSettings instance.
        Set<Setting<?>> settingSet = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settingSet.add(AnomalyDetectorSettings.AD_RESULT_HISTORY_MAX_DOCS_PER_SHARD);
        settingSet.add(AnomalyDetectorSettings.AD_RESULT_HISTORY_ROLLOVER_PERIOD);
        settingSet.add(AnomalyDetectorSettings.AD_RESULT_HISTORY_RETENTION_PERIOD);
        settingSet.add(AnomalyDetectorSettings.AD_MAX_PRIMARY_SHARDS);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, settingSet);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        doNothing().when(clusterService).addLocalNodeClusterManagerListener(any());
        threadPool = mock(ThreadPool.class);
        nodeFilter = mock(DiscoveryNodeFilterer.class);
    }

    @Test
    public void testValidateInsightsResultIndexMappingConcreteIndexNull() throws Exception {
        TestADIndexManagement m = new TestADIndexManagement(
            client,
            clusterService,
            threadPool,
            Settings.EMPTY,
            nodeFilter,
            TimeSeriesSettings.MAX_UPDATE_RETRY_TIMES,
            NamedXContentRegistry.EMPTY
        );
        m.concreteIndex = null;
        m.validateReturn = true;

        final boolean[] result = new boolean[] { true };
        m.validateInsightsResultIndexMapping("opensearch-ad-plugin-insights", ActionListener.wrap(valid -> result[0] = valid, e -> {
            throw new RuntimeException(e);
        }));
        assertFalse(result[0]);
    }

    @Test
    public void testValidateInsightsResultIndexMappingValidAndInvalid() throws Exception {
        TestADIndexManagement m = new TestADIndexManagement(
            client,
            clusterService,
            threadPool,
            Settings.EMPTY,
            nodeFilter,
            TimeSeriesSettings.MAX_UPDATE_RETRY_TIMES,
            NamedXContentRegistry.EMPTY
        );
        m.concreteIndex = "concrete-index";

        final boolean[] result = new boolean[] { false };
        m.validateReturn = true;
        m.validateInsightsResultIndexMapping("opensearch-ad-plugin-insights", ActionListener.wrap(valid -> result[0] = valid, e -> {
            throw new RuntimeException(e);
        }));
        assertTrue(result[0]);

        m.validateReturn = false;
        m.validateInsightsResultIndexMapping("opensearch-ad-plugin-insights", ActionListener.wrap(valid -> result[0] = valid, e -> {
            throw new RuntimeException(e);
        }));
        assertFalse(result[0]);
    }
}
