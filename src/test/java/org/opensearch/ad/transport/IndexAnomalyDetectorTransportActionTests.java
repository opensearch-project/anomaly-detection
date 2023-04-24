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

package org.opensearch.ad.transport;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.feature.SearchFeatureDao;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.rest.RestRequest;
import org.opensearch.sdk.Extension;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.SDKClient.SDKRestClient;
import org.opensearch.sdk.SDKClusterService;
import org.opensearch.sdk.SDKClusterService.SDKClusterSettings;
import org.opensearch.sdk.SDKNamedXContentRegistry;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;
import org.opensearch.test.OpenSearchIntegTestCase;

import com.google.common.collect.ImmutableMap;

public class IndexAnomalyDetectorTransportActionTests extends OpenSearchIntegTestCase {
    private IndexAnomalyDetectorTransportAction action;
    private Task task;
    private IndexAnomalyDetectorRequest request;
    private ActionListener<IndexAnomalyDetectorResponse> response;
    private SDKClusterService clusterService;
    private SDKClusterSettings clusterSettings;
    private ADTaskManager adTaskManager;
    private SDKRestClient client = mock(SDKRestClient.class);
    private SearchFeatureDao searchFeatureDao;
    private SDKNamedXContentRegistry mockSdkXContentRegistry;
    private ExtensionsRunner mockRunner;
    private Extension mockExtension;

    @SuppressWarnings("unchecked")
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        clusterService = mock(SDKClusterService.class);
        mockExtension = mock(Extension.class);

        mockRunner = mock(ExtensionsRunner.class);
        /*-
        clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES)))
        );
        */
        Settings settings = Settings.EMPTY;
        List<Setting<?>> settingsList = List.of(AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES, AnomalyDetectorSettings.PAGE_SIZE);
        Extension mockExtension = mock(Extension.class);
        when(mockRunner.getEnvironmentSettings()).thenReturn(settings);
        when(mockRunner.getExtension()).thenReturn(mockExtension);
        when(mockExtension.getSettings()).thenReturn(settingsList);
        SDKClusterService.SDKClusterSettings clusterSettings = new SDKClusterService(mockRunner).getClusterSettings();
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        ClusterName clusterName = new ClusterName("test");
        Settings indexSettings = Settings
            .builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();
        final Settings.Builder existingSettings = Settings.builder().put(indexSettings).put(IndexMetadata.SETTING_INDEX_UUID, "test2UUID");
        IndexMetadata indexMetaData = IndexMetadata.builder(AnomalyDetector.ANOMALY_DETECTORS_INDEX).settings(existingSettings).build();
        final Map<String, IndexMetadata> indices = new HashMap<>();
        indices.put(AnomalyDetector.ANOMALY_DETECTORS_INDEX, indexMetaData);
        ClusterState clusterState = ClusterState.builder(clusterName).metadata(Metadata.builder().indices(indices).build()).build();
        when(clusterService.state()).thenReturn(clusterState);

        adTaskManager = mock(ADTaskManager.class);
        searchFeatureDao = mock(SearchFeatureDao.class);

        this.mockSdkXContentRegistry = mock(SDKNamedXContentRegistry.class);
        when(mockSdkXContentRegistry.getRegistry()).thenReturn(xContentRegistry());

        action = new IndexAnomalyDetectorTransportAction(
            mockRunner,
            mock(TaskManager.class),
            mock(ActionFilters.class),
            client, // client(),
            clusterService,
            mock(AnomalyDetectionIndices.class),
            mockSdkXContentRegistry,
            adTaskManager,
            searchFeatureDao
        );
        task = mock(Task.class);
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableMap.of("testKey", "testValue"), Instant.now());
        GetResponse getDetectorResponse = TestHelpers
            .createGetResponse(detector, detector.getDetectorId(), AnomalyDetector.ANOMALY_DETECTORS_INDEX);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(
                String.format(Locale.ROOT, "The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)),
                args.length == 2
            );

            assertTrue(args[0] instanceof GetRequest);
            assertTrue(args[1] instanceof ActionListener);

            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) args[1];
            listener.onResponse(getDetectorResponse);
            return null;
        }).when(client).get(any(GetRequest.class), any());

        SearchHits hits = new SearchHits(new SearchHit[] {}, null, Float.NaN);
        SearchResponseSections searchSections = new SearchResponseSections(hits, null, null, false, false, null, 1);
        SearchResponse searchResponse = new SearchResponse(
            searchSections,
            null,
            1,
            1,
            0,
            30,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(
                String.format(Locale.ROOT, "The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)),
                args.length == 2
            );

            assertTrue(args[0] instanceof SearchRequest);
            assertTrue(args[1] instanceof ActionListener);

            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) args[1];
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(SearchRequest.class), any());

        request = new IndexAnomalyDetectorRequest(
            "1234",
            4567,
            7890,
            WriteRequest.RefreshPolicy.IMMEDIATE,
            detector,
            RestRequest.Method.PUT,
            TimeValue.timeValueSeconds(60),
            1000,
            10,
            5
        );
        response = new ActionListener<IndexAnomalyDetectorResponse>() {
            @Override
            public void onResponse(IndexAnomalyDetectorResponse indexResponse) {
                // onResponse will not be called as we do not have the AD index
                Assert.assertTrue(false);
            }

            @Override
            public void onFailure(Exception e) {
                Assert.assertTrue(true);
            }
        };
    }

    @Test
    public void testIndexTransportAction() {
        action.doExecute(task, request, response);
    }

    @Test
    public void testIndexTransportActionWithUserAndFilterOn() {
        Settings settings = Settings.builder().put(AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES.getKey(), true).build();
        List<Setting<?>> settingsList = List.of(AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES);
        when(mockRunner.getEnvironmentSettings()).thenReturn(settings);
        when(mockRunner.getExtension()).thenReturn(mockExtension);
        when(mockExtension.getSettings()).thenReturn(settingsList);
        SDKClusterSettings clusterSettings = new SDKClusterService(mockRunner).getClusterSettings();
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        IndexAnomalyDetectorTransportAction transportAction = new IndexAnomalyDetectorTransportAction(
            mockRunner,
            mock(TaskManager.class),
            mock(ActionFilters.class),
            client,
            clusterService,
            mock(AnomalyDetectionIndices.class),
            mockSdkXContentRegistry,
            adTaskManager,
            searchFeatureDao

        );
        transportAction.doExecute(task, request, response);
    }

    @Test
    public void testIndexTransportActionWithUserAndFilterOff() {
        Settings settings = Settings.builder().build();
        when(mockRunner.getEnvironmentSettings()).thenReturn(settings);
        when(mockRunner.getExtension()).thenReturn(mockExtension);
        SDKClusterSettings clusterSettings = new SDKClusterService(mockRunner).getClusterSettings();
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        IndexAnomalyDetectorTransportAction transportAction = new IndexAnomalyDetectorTransportAction(
            mockRunner,
            mock(TaskManager.class),
            mock(ActionFilters.class),
            client,
            clusterService,
            mock(AnomalyDetectionIndices.class),
            mockSdkXContentRegistry,
            adTaskManager,
            searchFeatureDao
        );
        transportAction.doExecute(task, request, response);
    }

    @Test
    public void testIndexDetectorAction() {
        Assert.assertNotNull(IndexAnomalyDetectorAction.INSTANCE.name());
        Assert.assertEquals(IndexAnomalyDetectorAction.INSTANCE.name(), IndexAnomalyDetectorAction.NAME);
    }
}
