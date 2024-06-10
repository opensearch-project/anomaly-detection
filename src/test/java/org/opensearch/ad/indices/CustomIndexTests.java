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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.opensearch.Version;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;

public class CustomIndexTests extends AbstractTimeSeriesTest {
    ADIndexManagement adIndices;
    Client client;
    ClusterService clusterService;
    DiscoveryNodeFilterer nodeFilter;
    ClusterState clusterState;
    String customIndexName;
    ClusterName clusterName;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        client = mock(Client.class);

        clusterService = mock(ClusterService.class);

        clusterName = new ClusterName("test");

        customIndexName = "opensearch-ad-plugin-result-a";
        // clusterState = ClusterState.builder(clusterName).metadata(Metadata.builder().build()).build();
        when(clusterService.state()).thenReturn(clusterState);

        Settings settings = Settings.EMPTY;
        ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            Collections
                .unmodifiableSet(
                    new HashSet<>(
                        Arrays
                            .asList(
                                AnomalyDetectorSettings.AD_RESULT_HISTORY_MAX_DOCS_PER_SHARD,
                                AnomalyDetectorSettings.AD_RESULT_HISTORY_ROLLOVER_PERIOD,
                                AnomalyDetectorSettings.AD_RESULT_HISTORY_RETENTION_PERIOD,
                                AnomalyDetectorSettings.AD_MAX_PRIMARY_SHARDS
                            )
                    )
                )
        );

        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        nodeFilter = mock(DiscoveryNodeFilterer.class);

        adIndices = new ADIndexManagement(
            client,
            clusterService,
            threadPool,
            settings,
            nodeFilter,
            TimeSeriesSettings.MAX_UPDATE_RETRY_TIMES,
            NamedXContentRegistry.EMPTY
        );
    }

    private Map<String, Object> createMapping() {
        Map<String, Object> mappings = new HashMap<>();

        Map<String, Object> grade_mapping = new HashMap<>();
        grade_mapping.put("type", "double");
        mappings.put(AnomalyResult.ANOMALY_GRADE_FIELD, grade_mapping);

        Map<String, Object> score_mapping = new HashMap<>();
        score_mapping.put("type", "double");
        mappings.put(AnomalyResult.ANOMALY_SCORE_FIELD, score_mapping);

        Map<String, Object> approx_mapping = new HashMap<>();
        approx_mapping.put("type", "date");
        approx_mapping.put("format", "strict_date_time||epoch_millis");
        mappings.put(AnomalyResult.APPROX_ANOMALY_START_FIELD, approx_mapping);

        Map<String, Object> confidence_mapping = new HashMap<>();
        confidence_mapping.put("type", "double");
        mappings.put(CommonName.CONFIDENCE_FIELD, confidence_mapping);

        Map<String, Object> data_end_time = new HashMap<>();
        data_end_time.put("type", "date");
        data_end_time.put("format", "strict_date_time||epoch_millis");
        mappings.put(CommonName.DATA_END_TIME_FIELD, data_end_time);

        Map<String, Object> data_start_time = new HashMap<>();
        data_start_time.put("type", "date");
        data_start_time.put("format", "strict_date_time||epoch_millis");
        mappings.put(CommonName.DATA_START_TIME_FIELD, data_start_time);

        Map<String, Object> exec_start_mapping = new HashMap<>();
        exec_start_mapping.put("type", "date");
        exec_start_mapping.put("format", "strict_date_time||epoch_millis");
        mappings.put(CommonName.EXECUTION_START_TIME_FIELD, exec_start_mapping);

        Map<String, Object> exec_end_mapping = new HashMap<>();
        exec_end_mapping.put("type", "date");
        exec_end_mapping.put("format", "strict_date_time||epoch_millis");
        mappings.put(CommonName.EXECUTION_END_TIME_FIELD, exec_end_mapping);

        Map<String, Object> detector_id_mapping = new HashMap<>();
        detector_id_mapping.put("type", "keyword");
        mappings.put(AnomalyResult.DETECTOR_ID_FIELD, detector_id_mapping);

        Map<String, Object> entity_mapping = new HashMap<>();
        entity_mapping.put("type", "nested");
        Map<String, Object> entity_nested_mapping = new HashMap<>();
        entity_nested_mapping.put("name", Collections.singletonMap("type", "keyword"));
        entity_nested_mapping.put("value", Collections.singletonMap("type", "keyword"));
        entity_mapping.put(CommonName.PROPERTIES, entity_nested_mapping);
        mappings.put(CommonName.ENTITY_KEY, entity_mapping);

        Map<String, Object> error_mapping = new HashMap<>();
        error_mapping.put("type", "text");
        mappings.put(CommonName.ERROR_FIELD, error_mapping);

        Map<String, Object> expected_mapping = new HashMap<>();
        expected_mapping.put("type", "nested");
        Map<String, Object> expected_nested_mapping = new HashMap<>();
        expected_mapping.put(CommonName.PROPERTIES, expected_nested_mapping);
        expected_nested_mapping.put("likelihood", Collections.singletonMap("type", "double"));
        Map<String, Object> value_list_mapping = new HashMap<>();
        expected_nested_mapping.put("value_list", value_list_mapping);
        value_list_mapping.put("type", "nested");
        Map<String, Object> value_list_nested_mapping = new HashMap<>();
        value_list_mapping.put(CommonName.PROPERTIES, value_list_nested_mapping);
        value_list_nested_mapping.put("data", Collections.singletonMap("type", "double"));
        value_list_nested_mapping.put("feature_id", Collections.singletonMap("type", "keyword"));
        mappings.put(AnomalyResult.EXPECTED_VALUES_FIELD, expected_mapping);

        Map<String, Object> feature_mapping = new HashMap<>();
        feature_mapping.put("type", "nested");
        Map<String, Object> feature_nested_mapping = new HashMap<>();
        feature_mapping.put(CommonName.PROPERTIES, feature_nested_mapping);
        feature_nested_mapping.put("data", Collections.singletonMap("type", "double"));
        feature_nested_mapping.put("feature_id", Collections.singletonMap("type", "keyword"));
        feature_nested_mapping.put("feature_name", Collections.singletonMap("type", "keyword"));
        mappings.put(CommonName.FEATURE_DATA_FIELD, feature_mapping);
        mappings.put(AnomalyResult.IS_ANOMALY_FIELD, Collections.singletonMap("type", "boolean"));
        mappings.put(CommonName.MODEL_ID_FIELD, Collections.singletonMap("type", "keyword"));

        Map<String, Object> past_mapping = new HashMap<>();
        past_mapping.put("type", "nested");
        Map<String, Object> past_nested_mapping = new HashMap<>();
        past_mapping.put(CommonName.PROPERTIES, past_nested_mapping);
        past_nested_mapping.put("data", Collections.singletonMap("type", "double"));
        past_nested_mapping.put("feature_id", Collections.singletonMap("type", "keyword"));
        mappings.put(AnomalyResult.PAST_VALUES_FIELD, past_mapping);

        Map<String, Object> attribution_mapping = new HashMap<>();
        attribution_mapping.put("type", "nested");
        Map<String, Object> attribution_nested_mapping = new HashMap<>();
        attribution_mapping.put(CommonName.PROPERTIES, attribution_nested_mapping);
        attribution_nested_mapping.put("data", Collections.singletonMap("type", "double"));
        attribution_nested_mapping.put("feature_id", Collections.singletonMap("type", "keyword"));
        mappings.put(AnomalyResult.RELEVANT_ATTRIBUTION_FIELD, attribution_mapping);

        mappings.put(org.opensearch.timeseries.constant.CommonName.SCHEMA_VERSION_FIELD, Collections.singletonMap("type", "integer"));

        mappings.put(CommonName.TASK_ID_FIELD, Collections.singletonMap("type", "keyword"));

        mappings.put(AnomalyResult.THRESHOLD_FIELD, Collections.singletonMap("type", "double"));

        Map<String, Object> user_mapping = new HashMap<>();
        user_mapping.put("type", "nested");
        Map<String, Object> user_nested_mapping = new HashMap<>();
        user_mapping.put(CommonName.PROPERTIES, user_nested_mapping);
        Map<String, Object> backend_role_mapping = new HashMap<>();
        backend_role_mapping.put("type", "text");
        backend_role_mapping.put("fields", Collections.singletonMap("keyword", Collections.singletonMap("type", "keyword")));
        user_nested_mapping.put("backend_roles", backend_role_mapping);
        Map<String, Object> custom_attribute_mapping = new HashMap<>();
        custom_attribute_mapping.put("type", "text");
        custom_attribute_mapping.put("fields", Collections.singletonMap("keyword", Collections.singletonMap("type", "keyword")));
        user_nested_mapping.put("custom_attribute_names", custom_attribute_mapping);
        Map<String, Object> name_mapping = new HashMap<>();
        name_mapping.put("type", "text");
        Map<String, Object> name_fields_mapping = new HashMap<>();
        name_fields_mapping.put("type", "keyword");
        name_fields_mapping.put("ignore_above", 256);
        name_mapping.put("fields", Collections.singletonMap("keyword", name_fields_mapping));
        user_nested_mapping.put("name", name_mapping);
        Map<String, Object> roles_mapping = new HashMap<>();
        roles_mapping.put("type", "text");
        roles_mapping.put("fields", Collections.singletonMap("keyword", Collections.singletonMap("type", "keyword")));
        user_nested_mapping.put("roles", roles_mapping);
        mappings.put(CommonName.USER_FIELD, user_mapping);
        return mappings;
    }

    public void testCorrectMapping() throws IOException, InterruptedException {
        Map<String, Object> mappings = createMapping();

        IndexMetadata indexMetadata1 = new IndexMetadata.Builder(customIndexName)
            .settings(
                Settings
                    .builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
            .putMapping(new MappingMetadata("type1", Collections.singletonMap(CommonName.PROPERTIES, mappings)))
            .build();
        when(clusterService.state())
            .thenReturn(ClusterState.builder(clusterName).metadata(Metadata.builder().put(indexMetadata1, true).build()).build());

        CountDownLatch countDown = new CountDownLatch(1);
        adIndices.validateResultIndexMapping(customIndexName, ActionListener.wrap(valid -> {
            assertTrue(valid);
            countDown.countDown();
        }, exception -> {
            assertTrue(exception.getMessage(), false);
            countDown.countDown();
        }));
        countDown.await(60, TimeUnit.SECONDS);
    }

    /**
     * Test that the mapping returned by get mapping request returns the same mapping
     * but with different order
     * @throws IOException when MappingMetadata constructor throws errors
     * @throws InterruptedException
     */
    public void testCorrectReordered() throws IOException, InterruptedException {
        Map<String, Object> mappings = createMapping();

        Map<String, Object> feature_mapping = new HashMap<>();
        feature_mapping.put("type", "nested");
        Map<String, Object> feature_nested_mapping = new HashMap<>();
        feature_mapping.put(CommonName.PROPERTIES, feature_nested_mapping);
        // feature_id comes before data compared with what createMapping returned
        feature_nested_mapping.put("feature_id", Collections.singletonMap("type", "keyword"));
        feature_nested_mapping.put("feature_name", Collections.singletonMap("type", "keyword"));
        feature_nested_mapping.put("data", Collections.singletonMap("type", "double"));
        mappings.put(CommonName.FEATURE_DATA_FIELD, feature_mapping);

        IndexMetadata indexMetadata1 = new IndexMetadata.Builder(customIndexName)
            .settings(
                Settings
                    .builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
            .putMapping(new MappingMetadata("type1", Collections.singletonMap(CommonName.PROPERTIES, mappings)))
            .build();
        when(clusterService.state())
            .thenReturn(ClusterState.builder(clusterName).metadata(Metadata.builder().put(indexMetadata1, true).build()).build());

        CountDownLatch countDown = new CountDownLatch(1);
        adIndices.validateResultIndexMapping(customIndexName, ActionListener.wrap(valid -> {
            assertTrue(valid);
            countDown.countDown();
        }, exception -> {
            assertTrue(exception.getMessage(), false);
            countDown.countDown();
        }));
        countDown.await(60, TimeUnit.SECONDS);
    }

    /**
     * Test that the mapping returned by get mapping request returns a super set
     * of result index mapping
     * @throws IOException when MappingMetadata constructor throws errors
     * @throws InterruptedException
     */
    public void testSuperset() throws IOException, InterruptedException {
        Map<String, Object> mappings = createMapping();

        Map<String, Object> feature_mapping = new HashMap<>();
        feature_mapping.put("type", "nested");
        Map<String, Object> feature_nested_mapping = new HashMap<>();
        feature_mapping.put(CommonName.PROPERTIES, feature_nested_mapping);
        feature_nested_mapping.put("feature_id", Collections.singletonMap("type", "keyword"));
        feature_nested_mapping.put("feature_name", Collections.singletonMap("type", "keyword"));
        feature_nested_mapping.put("data", Collections.singletonMap("type", "double"));
        mappings.put("a", feature_mapping);

        IndexMetadata indexMetadata1 = new IndexMetadata.Builder(customIndexName)
            .settings(
                Settings
                    .builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
            .putMapping(new MappingMetadata("type1", Collections.singletonMap(CommonName.PROPERTIES, mappings)))
            .build();
        when(clusterService.state())
            .thenReturn(ClusterState.builder(clusterName).metadata(Metadata.builder().put(indexMetadata1, true).build()).build());

        CountDownLatch countDown = new CountDownLatch(1);
        adIndices.validateResultIndexMapping(customIndexName, ActionListener.wrap(valid -> {
            assertTrue(valid);
            countDown.countDown();
        }, exception -> {
            assertTrue(exception.getMessage(), false);
            countDown.countDown();
        }));
        countDown.await(60, TimeUnit.SECONDS);
    }

    public void testInCorrectMapping() throws IOException, InterruptedException {
        Map<String, Object> mappings = new HashMap<>();

        Map<String, Object> past_mapping = new HashMap<>();
        past_mapping.put("type", "nested");
        Map<String, Object> past_nested_mapping = new HashMap<>();
        past_mapping.put(CommonName.PROPERTIES, past_nested_mapping);
        past_nested_mapping.put("data", Collections.singletonMap("type", "double"));
        past_nested_mapping.put("feature_id", Collections.singletonMap("type", "keyword"));
        mappings.put(AnomalyResult.PAST_VALUES_FIELD, past_mapping);

        Map<String, Object> attribution_mapping = new HashMap<>();
        past_mapping.put("type", "nested");
        Map<String, Object> attribution_nested_mapping = new HashMap<>();
        attribution_mapping.put(CommonName.PROPERTIES, attribution_nested_mapping);
        attribution_nested_mapping.put("data", Collections.singletonMap("type", "double"));
        attribution_nested_mapping.put("feature_id", Collections.singletonMap("type", "keyword"));
        mappings.put(AnomalyResult.RELEVANT_ATTRIBUTION_FIELD, attribution_mapping);

        IndexMetadata indexMetadata1 = new IndexMetadata.Builder(customIndexName)
            .settings(
                Settings
                    .builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
            .putMapping(new MappingMetadata("type1", Collections.singletonMap(CommonName.PROPERTIES, mappings)))
            .build();
        when(clusterService.state())
            .thenReturn(ClusterState.builder(clusterName).metadata(Metadata.builder().put(indexMetadata1, true).build()).build());

        CountDownLatch countDown = new CountDownLatch(1);
        adIndices.validateResultIndexMapping(customIndexName, ActionListener.wrap(valid -> {
            assertTrue("Should be invalid mapping", !valid);
            countDown.countDown();
        }, exception -> {
            assertTrue("should not reach here.", false);
            countDown.countDown();
        }));
        assertTrue(countDown.await(60, TimeUnit.SECONDS));
    }

}
