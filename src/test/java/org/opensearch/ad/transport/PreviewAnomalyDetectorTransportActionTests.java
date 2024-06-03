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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.Version;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.ad.AnomalyDetectorRunner;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.ml.ADModelManager;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.commons.ConfigConstants;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.feature.FeatureManager;
import org.opensearch.timeseries.feature.Features;
import org.opensearch.timeseries.util.RestHandlerUtils;
import org.opensearch.transport.TransportService;

import com.google.common.collect.ImmutableMap;

public class PreviewAnomalyDetectorTransportActionTests extends OpenSearchSingleNodeTestCase {
    private ActionListener<PreviewAnomalyDetectorResponse> response;
    private PreviewAnomalyDetectorTransportAction action;
    private AnomalyDetectorRunner runner;
    private ClusterService clusterService;
    private FeatureManager featureManager;
    private ADModelManager modelManager;
    private Task task;
    private CircuitBreakerService circuitBreaker;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        task = mock(Task.class);
        clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Collections
                .unmodifiableSet(
                    new HashSet<>(
                        Arrays
                            .asList(
                                AnomalyDetectorSettings.MAX_ANOMALY_FEATURES,
                                AnomalyDetectorSettings.AD_FILTER_BY_BACKEND_ROLES,
                                AnomalyDetectorSettings.AD_PAGE_SIZE,
                                AnomalyDetectorSettings.MAX_CONCURRENT_PREVIEW
                            )
                    )
                )
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
        IndexMetadata indexMetaData = IndexMetadata.builder(CommonName.CONFIG_INDEX).settings(existingSettings).build();
        final Map<String, IndexMetadata> indices = new HashMap<>();
        indices.put(CommonName.CONFIG_INDEX, indexMetaData);
        ClusterState clusterState = ClusterState.builder(clusterName).metadata(Metadata.builder().indices(indices).build()).build();
        when(clusterService.state()).thenReturn(clusterState);

        featureManager = mock(FeatureManager.class);
        modelManager = mock(ADModelManager.class);
        runner = new AnomalyDetectorRunner(modelManager, featureManager, AnomalyDetectorSettings.MAX_PREVIEW_RESULTS);
        circuitBreaker = mock(CircuitBreakerService.class);
        when(circuitBreaker.isOpen()).thenReturn(false);
        action = new PreviewAnomalyDetectorTransportAction(
            Settings.EMPTY,
            mock(TransportService.class),
            clusterService,
            mock(ActionFilters.class),
            client(),
            runner,
            xContentRegistry(),
            circuitBreaker
        );
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testPreviewTransportAction() throws IOException, InterruptedException {
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableMap.of("testKey", "testValue"), Instant.now());
        PreviewAnomalyDetectorRequest request = new PreviewAnomalyDetectorRequest(detector, detector.getId(), Instant.now(), Instant.now());
        ActionListener<PreviewAnomalyDetectorResponse> previewResponse = new ActionListener<PreviewAnomalyDetectorResponse>() {
            @Override
            public void onResponse(PreviewAnomalyDetectorResponse response) {
                try {
                    XContentBuilder previewBuilder = response.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS);
                    Assert.assertNotNull(previewBuilder);
                    Map<String, Object> map = TestHelpers.XContentBuilderToMap(previewBuilder);
                    List<AnomalyResult> results = (List<AnomalyResult>) map.get("anomaly_result");
                    Assert.assertNotNull(results);
                    Assert.assertTrue(results.size() > 0);
                    inProgressLatch.countDown();
                } catch (IOException e) {
                    // Should not reach here
                    Assert.assertTrue(false);
                }
            }

            @Override
            public void onFailure(Exception e) {
                // onFailure should not be called
                Assert.assertTrue(false);
            }
        };

        doReturn(TestHelpers.randomThresholdingResults()).when(modelManager).getPreviewResults(any(), anyInt(), anyInt());

        doAnswer(responseMock -> {
            Long startTime = responseMock.getArgument(1);
            ActionListener<Features> listener = responseMock.getArgument(3);
            listener.onResponse(TestHelpers.randomFeatures());
            return null;
        }).when(featureManager).getPreviewFeatures(any(), anyLong(), anyLong(), any());
        action.doExecute(task, request, previewResponse);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    @Test
    public void testPreviewTransportActionWithNoFeature() throws IOException, InterruptedException {
        // Detector with no feature, Preview should fail
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(Collections.emptyList());
        PreviewAnomalyDetectorRequest request = new PreviewAnomalyDetectorRequest(detector, detector.getId(), Instant.now(), Instant.now());
        ActionListener<PreviewAnomalyDetectorResponse> previewResponse = new ActionListener<PreviewAnomalyDetectorResponse>() {
            @Override
            public void onResponse(PreviewAnomalyDetectorResponse response) {
                Assert.assertTrue(false);
            }

            @Override
            public void onFailure(Exception e) {
                Assert.assertTrue(e.getMessage().contains("Can't preview detector without feature"));
                inProgressLatch.countDown();
            }
        };
        action.doExecute(task, request, previewResponse);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    @Test
    public void testPreviewTransportActionWithNoDetector() throws IOException, InterruptedException {
        // When detectorId is null, preview should fail
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        PreviewAnomalyDetectorRequest request = new PreviewAnomalyDetectorRequest(null, "123", Instant.now(), Instant.now());
        ActionListener<PreviewAnomalyDetectorResponse> previewResponse = new ActionListener<PreviewAnomalyDetectorResponse>() {
            @Override
            public void onResponse(PreviewAnomalyDetectorResponse response) {
                Assert.assertTrue(false);
            }

            @Override
            public void onFailure(Exception e) {
                Assert.assertTrue(e.getMessage().contains("Could not execute get query to find detector"));
                inProgressLatch.countDown();
            }
        };
        action.doExecute(task, request, previewResponse);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    @Test
    public void testPreviewTransportActionWithDetectorID() throws IOException, InterruptedException {
        // When AD index does not exist, cannot query the detector
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        PreviewAnomalyDetectorRequest request = new PreviewAnomalyDetectorRequest(null, "1234", Instant.now(), Instant.now());
        ActionListener<PreviewAnomalyDetectorResponse> previewResponse = new ActionListener<PreviewAnomalyDetectorResponse>() {
            @Override
            public void onResponse(PreviewAnomalyDetectorResponse response) {
                Assert.assertTrue(false);
            }

            @Override
            public void onFailure(Exception e) {
                Assert.assertTrue(e.getMessage().contains("Could not execute get query to find detector"));
                inProgressLatch.countDown();
            }
        };
        action.doExecute(task, request, previewResponse);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    @Test
    public void testPreviewTransportActionWithIndex() throws IOException, InterruptedException {
        // When AD index exists, and detector does not exist
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        PreviewAnomalyDetectorRequest request = new PreviewAnomalyDetectorRequest(null, "1234", Instant.now(), Instant.now());
        Settings indexSettings = Settings.builder().put("index.number_of_shards", 5).put("index.number_of_replicas", 1).build();
        CreateIndexRequest indexRequest = new CreateIndexRequest(CommonName.CONFIG_INDEX, indexSettings);
        client().admin().indices().create(indexRequest).actionGet();
        ActionListener<PreviewAnomalyDetectorResponse> previewResponse = new ActionListener<PreviewAnomalyDetectorResponse>() {
            @Override
            public void onResponse(PreviewAnomalyDetectorResponse response) {
                Assert.assertTrue(false);
            }

            @Override
            public void onFailure(Exception e) {
                Assert.assertTrue(e.getMessage().contains("Can't find anomaly detector with id:1234"));
                inProgressLatch.countDown();
            }
        };
        action.doExecute(task, request, previewResponse);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testPreviewTransportActionNoContext() throws IOException, InterruptedException {
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        Settings settings = Settings.builder().put(AnomalyDetectorSettings.AD_FILTER_BY_BACKEND_ROLES.getKey(), true).build();
        Client client = mock(Client.class);
        ThreadContext threadContext = new ThreadContext(settings);
        threadContext.putTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT, "alice|odfe,aes|engineering,operations");
        org.opensearch.threadpool.ThreadPool mockThreadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(mockThreadPool);
        when(mockThreadPool.getThreadContext()).thenReturn(threadContext);
        PreviewAnomalyDetectorTransportAction previewAction = new PreviewAnomalyDetectorTransportAction(
            settings,
            mock(TransportService.class),
            clusterService,
            mock(ActionFilters.class),
            client,
            runner,
            xContentRegistry(),
            circuitBreaker
        );
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableMap.of("testKey", "testValue"), Instant.now());
        PreviewAnomalyDetectorRequest request = new PreviewAnomalyDetectorRequest(detector, detector.getId(), Instant.now(), Instant.now());

        GetResponse getDetectorResponse = TestHelpers.createGetResponse(detector, detector.getId(), CommonName.CONFIG_INDEX);
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

        ActionListener<PreviewAnomalyDetectorResponse> responseActionListener = new ActionListener<PreviewAnomalyDetectorResponse>() {
            @Override
            public void onResponse(PreviewAnomalyDetectorResponse response) {
                Assert.assertTrue(false);
            }

            @Override
            public void onFailure(Exception e) {
                Assert.assertEquals(OpenSearchStatusException.class, e.getClass());
                inProgressLatch.countDown();
            }
        };
        previewAction.doExecute(task, request, responseActionListener);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testPreviewTransportActionWithDetector() throws IOException, InterruptedException {
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        CreateIndexResponse createResponse = TestHelpers
            .createIndex(client().admin(), CommonName.CONFIG_INDEX, ADIndexManagement.getConfigMappings());
        Assert.assertNotNull(createResponse);

        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableMap.of("testKey", "testValue"), Instant.now());
        IndexRequest indexRequest = new IndexRequest(CommonName.CONFIG_INDEX)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .source(detector.toXContent(XContentFactory.jsonBuilder(), RestHandlerUtils.XCONTENT_WITH_TYPE));
        IndexResponse indexResponse = client().index(indexRequest).actionGet(5_000);
        assertEquals(RestStatus.CREATED, indexResponse.status());

        PreviewAnomalyDetectorRequest request = new PreviewAnomalyDetectorRequest(
            null,
            indexResponse.getId(),
            Instant.now(),
            Instant.now()
        );
        ActionListener<PreviewAnomalyDetectorResponse> previewResponse = new ActionListener<PreviewAnomalyDetectorResponse>() {
            @Override
            public void onResponse(PreviewAnomalyDetectorResponse response) {
                try {
                    XContentBuilder previewBuilder = response.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS);
                    Assert.assertNotNull(previewBuilder);
                    Map<String, Object> map = TestHelpers.XContentBuilderToMap(previewBuilder);
                    List<AnomalyResult> results = (List<AnomalyResult>) map.get("anomaly_result");
                    Assert.assertNotNull(results);
                    Assert.assertTrue(results.size() > 0);
                    inProgressLatch.countDown();
                } catch (IOException e) {
                    // Should not reach here
                    Assert.assertTrue(false);
                }
            }

            @Override
            public void onFailure(Exception e) {
                // onFailure should not be called
                Assert.assertTrue(false);
            }
        };
        doReturn(TestHelpers.randomThresholdingResults()).when(modelManager).getPreviewResults(any(), anyInt(), anyInt());

        doAnswer(responseMock -> {
            Long startTime = responseMock.getArgument(1);
            ActionListener<Features> listener = responseMock.getArgument(3);
            listener.onResponse(TestHelpers.randomFeatures());
            return null;
        }).when(featureManager).getPreviewFeatures(any(), anyLong(), anyLong(), any());
        action.doExecute(task, request, previewResponse);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    @Test
    public void testCircuitBreakerOpen() throws IOException, InterruptedException {
        // preview has no detector id
        AnomalyDetector detector = TestHelpers.randomAnomalyDetectorUsingCategoryFields(null, Arrays.asList("a"));
        PreviewAnomalyDetectorRequest request = new PreviewAnomalyDetectorRequest(detector, detector.getId(), Instant.now(), Instant.now());

        when(circuitBreaker.isOpen()).thenReturn(true);

        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        ActionListener<PreviewAnomalyDetectorResponse> previewResponse = new ActionListener<PreviewAnomalyDetectorResponse>() {
            @Override
            public void onResponse(PreviewAnomalyDetectorResponse response) {
                Assert.assertTrue(false);
            }

            @Override
            public void onFailure(Exception e) {
                Assert.assertTrue("actual class: " + e.getClass(), e instanceof OpenSearchStatusException);
                Assert.assertTrue(e.getMessage().contains(CommonMessages.MEMORY_CIRCUIT_BROKEN_ERR_MSG));
                inProgressLatch.countDown();
            }
        };
        action.doExecute(task, request, previewResponse);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }
}
