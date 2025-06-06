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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.Version;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.ConfigConstants;
import org.opensearch.core.action.ActionListener;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.TransportService;

import com.google.common.collect.ImmutableMap;

public class IndexAnomalyDetectorTransportActionTests extends OpenSearchIntegTestCase {
    private IndexAnomalyDetectorTransportAction action;
    private Task task;
    private IndexAnomalyDetectorRequest request;
    private ActionListener<IndexAnomalyDetectorResponse> response;
    private ClusterService clusterService;
    private ClusterSettings clusterSettings;
    private ADTaskManager adTaskManager;
    private Client client = mock(Client.class);
    private SecurityClientUtil clientUtil;
    private SearchFeatureDao searchFeatureDao;

    @SuppressWarnings("unchecked")
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        clusterService = mock(ClusterService.class);
        clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(AnomalyDetectorSettings.AD_FILTER_BY_BACKEND_ROLES)))
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        ClusterName clusterName = new ClusterName("test");
        Settings indexSettings = Settings
            .builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();
        final Settings.Builder existingSettings = Settings.builder().put(indexSettings).put(IndexMetadata.SETTING_INDEX_UUID, "test2UUID");
        IndexMetadata indexMetaData = IndexMetadata.builder(ADCommonName.CONFIG_INDEX).settings(existingSettings).build();
        final Map<String, IndexMetadata> indices = new HashMap<>();
        indices.put(ADCommonName.CONFIG_INDEX, indexMetaData);
        ClusterState clusterState = ClusterState.builder(clusterName).metadata(Metadata.builder().indices(indices).build()).build();
        when(clusterService.state()).thenReturn(clusterState);

        adTaskManager = mock(ADTaskManager.class);
        searchFeatureDao = mock(SearchFeatureDao.class);
        NodeStateManager nodeStateManager = mock(NodeStateManager.class);
        clientUtil = new SecurityClientUtil(nodeStateManager, Settings.EMPTY);
        action = new IndexAnomalyDetectorTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            client(),
            clientUtil,
            clusterService,
            indexSettings(),
            mock(ADIndexManagement.class),
            xContentRegistry(),
            adTaskManager,
            searchFeatureDao
        );
        task = mock(Task.class);
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableMap.of("testKey", "testValue"), Instant.now());
        GetResponse getDetectorResponse = TestHelpers.createGetResponse(detector, detector.getId(), ADCommonName.CONFIG_INDEX);
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
            5,
            10
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
        Settings settings = Settings.builder().put(AnomalyDetectorSettings.AD_FILTER_BY_BACKEND_ROLES.getKey(), true).build();
        ThreadContext threadContext = new ThreadContext(settings);
        threadContext.putTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT, "alice|odfe,aes|engineering,operations");
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        org.opensearch.threadpool.ThreadPool mockThreadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(mockThreadPool);
        when(mockThreadPool.getThreadContext()).thenReturn(threadContext);

        IndexAnomalyDetectorTransportAction transportAction = new IndexAnomalyDetectorTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            client,
            clientUtil,
            clusterService,
            settings,
            mock(ADIndexManagement.class),
            xContentRegistry(),
            adTaskManager,
            searchFeatureDao

        );
        transportAction.doExecute(task, request, response);
    }

    @Test
    public void testIndexTransportActionWithUserAndFilterOff() {
        Settings settings = Settings.builder().build();
        ThreadContext threadContext = new ThreadContext(settings);
        threadContext.putTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT, "alice|odfe,aes|engineering,operations");
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        org.opensearch.threadpool.ThreadPool mockThreadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(mockThreadPool);
        when(mockThreadPool.getThreadContext()).thenReturn(threadContext);

        IndexAnomalyDetectorTransportAction transportAction = new IndexAnomalyDetectorTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            client,
            clientUtil,
            clusterService,
            settings,
            mock(ADIndexManagement.class),
            xContentRegistry(),
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
