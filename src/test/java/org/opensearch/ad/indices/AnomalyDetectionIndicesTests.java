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

package org.opensearch.ad.indices;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import org.junit.Before;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.plugins.Plugin;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.indices.IndexManagementIntegTestCase;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;

public class AnomalyDetectionIndicesTests extends IndexManagementIntegTestCase<ADIndex, ADIndexManagement> {

    private ADIndexManagement indices;
    private Settings settings;
    private DiscoveryNodeFilterer nodeFilter;

    // help register setting using TimeSeriesAnalyticsPlugin.getSettings.
    // Otherwise, ADIndexManagement's constructor would fail due to
    // unregistered settings like AD_RESULT_HISTORY_MAX_DOCS_PER_SHARD.
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(TimeSeriesAnalyticsPlugin.class);
    }

    @Before
    public void setup() throws IOException {
        settings = Settings
            .builder()
            .put("plugins.anomaly_detection.ad_result_history_rollover_period", TimeValue.timeValueHours(12))
            .put("plugins.anomaly_detection.ad_result_history_retention_period", TimeValue.timeValueHours(24))
            .put("plugins.anomaly_detection.ad_result_history_max_docs", 10000L)
            .put("plugins.anomaly_detection.request_timeout", TimeValue.timeValueSeconds(10))
            .build();

        nodeFilter = new DiscoveryNodeFilterer(clusterService());

        indices = new ADIndexManagement(
            client(),
            clusterService(),
            client().threadPool(),
            settings,
            nodeFilter,
            TimeSeriesSettings.MAX_UPDATE_RETRY_TIMES,
            NamedXContentRegistry.EMPTY
        );
    }

    public void testAnomalyDetectorIndexNotExists() {
        boolean exists = indices.doesConfigIndexExist();
        assertFalse(exists);
    }

    public void testAnomalyDetectorIndexExists() throws IOException {
        indices.initConfigIndexIfAbsent(TestHelpers.createActionListener(response -> {
            boolean acknowledged = response.isAcknowledged();
            assertTrue(acknowledged);
        }, failure -> { throw new RuntimeException("should not recreate index"); }));
        TestHelpers.waitForIndexCreationToComplete(client(), CommonName.CONFIG_INDEX);
    }

    public void testAnomalyDetectorIndexExistsAndNotRecreate() throws IOException {
        indices.initConfigIndexIfAbsent(TestHelpers.createActionListener(response -> response.isAcknowledged(), failure -> {
            throw new RuntimeException("should not recreate index");
        }));
        TestHelpers.waitForIndexCreationToComplete(client(), CommonName.CONFIG_INDEX);
        if (client().admin().indices().prepareExists(CommonName.CONFIG_INDEX).get().isExists()) {
            indices.initConfigIndexIfAbsent(TestHelpers.createActionListener(response -> {
                throw new RuntimeException("should not recreate index " + CommonName.CONFIG_INDEX);
            }, failure -> { throw new RuntimeException("should not recreate index " + CommonName.CONFIG_INDEX); }));
        }
    }

    public void testAnomalyResultIndexNotExists() {
        boolean exists = indices.doesDefaultResultIndexExist();
        assertFalse(exists);
    }

    public void testAnomalyResultIndexExists() throws IOException {
        indices.initDefaultResultIndexIfAbsent(TestHelpers.createActionListener(response -> {
            boolean acknowledged = response.isAcknowledged();
            assertTrue(acknowledged);
        }, failure -> { throw new RuntimeException("should not recreate index"); }));
        TestHelpers.waitForIndexCreationToComplete(client(), ADCommonName.ANOMALY_RESULT_INDEX_ALIAS);
    }

    public void testAnomalyResultIndexExistsAndNotRecreate() throws IOException {
        indices
            .initDefaultResultIndexIfAbsent(
                TestHelpers.createActionListener(response -> logger.info("Acknowledged: " + response.isAcknowledged()), failure -> {
                    throw new RuntimeException("should not recreate index");
                })
            );
        TestHelpers.waitForIndexCreationToComplete(client(), ADCommonName.ANOMALY_RESULT_INDEX_ALIAS);
        if (client().admin().indices().prepareExists(ADCommonName.ANOMALY_RESULT_INDEX_ALIAS).get().isExists()) {
            indices.initDefaultResultIndexIfAbsent(TestHelpers.createActionListener(response -> {
                throw new RuntimeException("should not recreate index " + ADCommonName.ANOMALY_RESULT_INDEX_ALIAS);
            }, failure -> { throw new RuntimeException("should not recreate index " + ADCommonName.ANOMALY_RESULT_INDEX_ALIAS, failure); })
            );
        }
    }

    public void testGetDetectionStateIndexMapping() throws IOException {
        String detectorIndexMappings = ADIndexManagement.getConfigMappings();
        detectorIndexMappings = detectorIndexMappings
            .substring(detectorIndexMappings.indexOf("\"properties\""), detectorIndexMappings.lastIndexOf("}"));
        String detectionStateIndexMapping = ADIndexManagement.getStateMappings();
        assertTrue(detectionStateIndexMapping.contains(detectorIndexMappings));
    }

    public void testValidateCustomIndexForBackendJob() throws IOException, InterruptedException {
        String resultMapping = ADIndexManagement.getResultMappings();

        validateCustomIndexForBackendJob(indices, resultMapping);
    }

    public void testValidateCustomIndexForBackendJobInvalidMapping() {
        validateCustomIndexForBackendJobInvalidMapping(indices);
    }

    public void testValidateCustomIndexForBackendJobNoIndex() {
        validateCustomIndexForBackendJobNoIndex(indices);
    }
}
