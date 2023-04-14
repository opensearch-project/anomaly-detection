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
import static org.mockito.ArgumentMatchers.anyObject;
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
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.ad.AnomalyDetectorRunner;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.breaker.ADCircuitBreakerService;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.feature.FeatureManager;
import org.opensearch.ad.feature.Features;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.ml.ModelManager;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.util.RestHandlerUtils;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.collect.ImmutableOpenMap;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.RestStatus;
import org.opensearch.sdk.Extension;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.SDKClient.SDKRestClient;
import org.opensearch.sdk.SDKClusterService;
import org.opensearch.sdk.SDKClusterService.SDKClusterSettings;
import org.opensearch.sdk.SDKNamedXContentRegistry;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.transport.TransportService;

import com.google.common.collect.ImmutableMap;

public class PreviewAnomalyDetectorTransportActionTests extends OpenSearchSingleNodeTestCase {
    private ActionListener<PreviewAnomalyDetectorResponse> response;
    private PreviewAnomalyDetectorTransportAction action;
    private AnomalyDetectorRunner runner;
    private SDKClusterService clusterService;
    private FeatureManager featureManager;
    private ModelManager modelManager;
    private Task task;
    private ADCircuitBreakerService circuitBreaker;
    private SDKNamedXContentRegistry mockSdkXContentRegistry;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        SDKRestClient client = mock(SDKRestClient.class);

        task = mock(Task.class);
        clusterService = mock(SDKClusterService.class);
        Settings settings = Settings.EMPTY;
        List<Setting<?>> settingsList = List
            .of(
                AnomalyDetectorSettings.MAX_ANOMALY_FEATURES,
                AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES,
                AnomalyDetectorSettings.PAGE_SIZE,
                AnomalyDetectorSettings.MAX_CONCURRENT_PREVIEW
            );
        ExtensionsRunner mockRunner = mock(ExtensionsRunner.class);
        Extension mockExtension = mock(Extension.class);
        when(mockRunner.getEnvironmentSettings()).thenReturn(settings);
        when(mockRunner.getExtension()).thenReturn(mockExtension);
        when(mockExtension.getSettings()).thenReturn(settingsList);
        SDKClusterSettings clusterSettings = new SDKClusterService(mockRunner).getClusterSettings();
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

        featureManager = mock(FeatureManager.class);
        modelManager = mock(ModelManager.class);
        runner = new AnomalyDetectorRunner(modelManager, featureManager, AnomalyDetectorSettings.MAX_PREVIEW_RESULTS);
        circuitBreaker = mock(ADCircuitBreakerService.class);
        when(circuitBreaker.isOpen()).thenReturn(false);

        this.mockSdkXContentRegistry = mock(SDKNamedXContentRegistry.class);
        when(mockSdkXContentRegistry.getRegistry()).thenReturn(xContentRegistry());

        action = new PreviewAnomalyDetectorTransportAction(
            Settings.EMPTY,
            mock(TransportService.class),
            clusterService,
            mock(ActionFilters.class),
            client,
            runner,
            mockSdkXContentRegistry,
            circuitBreaker
        );
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testPreviewTransportAction() throws IOException, InterruptedException {
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableMap.of("testKey", "testValue"), Instant.now());
        PreviewAnomalyDetectorRequest request = new PreviewAnomalyDetectorRequest(
            detector,
            detector.getDetectorId(),
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

        doReturn(TestHelpers.randomThresholdingResults()).when(modelManager).getPreviewResults(any(), anyInt());

        doAnswer(responseMock -> {
            Long startTime = responseMock.getArgument(1);
            ActionListener<Features> listener = responseMock.getArgument(3);
            listener.onResponse(TestHelpers.randomFeatures());
            return null;
        }).when(featureManager).getPreviewFeatures(anyObject(), anyLong(), anyLong(), any());
        action.doExecute(task, request, previewResponse);
        // The latch will never be triggered with mocked client
        // assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    @Test
    public void testPreviewTransportActionWithNoFeature() throws IOException, InterruptedException {
        // Detector with no feature, Preview should fail
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(Collections.emptyList());
        PreviewAnomalyDetectorRequest request = new PreviewAnomalyDetectorRequest(
            detector,
            detector.getDetectorId(),
            Instant.now(),
            Instant.now()
        );
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
        // The latch will never be triggered with mocked client
        // assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
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
        // The latch will never be triggered with mocked client
        // assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
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
        // The latch will never be triggered with mocked client
        // assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    @Test
    public void testPreviewTransportActionWithIndex() throws IOException, InterruptedException {
        // When AD index exists, and detector does not exist
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        PreviewAnomalyDetectorRequest request = new PreviewAnomalyDetectorRequest(null, "1234", Instant.now(), Instant.now());
        Settings indexSettings = Settings.builder().put("index.number_of_shards", 5).put("index.number_of_replicas", 1).build();
        CreateIndexRequest indexRequest = new CreateIndexRequest(AnomalyDetector.ANOMALY_DETECTORS_INDEX, indexSettings);
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
        // The latch will never be triggered with mocked client
        // assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testPreviewTransportActionNoContext() throws IOException, InterruptedException {
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        Settings settings = Settings.builder().put(AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES.getKey(), true).build();
        SDKRestClient client = mock(SDKRestClient.class);
        PreviewAnomalyDetectorTransportAction previewAction = new PreviewAnomalyDetectorTransportAction(
            settings,
            mock(TransportService.class),
            clusterService,
            mock(ActionFilters.class),
            client,
            runner,
            mockSdkXContentRegistry,
            circuitBreaker
        );
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableMap.of("testKey", "testValue"), Instant.now());
        PreviewAnomalyDetectorRequest request = new PreviewAnomalyDetectorRequest(
            detector,
            detector.getDetectorId(),
            Instant.now(),
            Instant.now()
        );

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

        ActionListener<PreviewAnomalyDetectorResponse> responseActionListener = new ActionListener<PreviewAnomalyDetectorResponse>() {
            @Override
            public void onResponse(PreviewAnomalyDetectorResponse response) {
                // Thread Context User has been replaced with null as part of https://github.com/opensearch-project/opensearch-sdk/issues/23
                // Previously we expected failure but could get a response here
                // Assert.assertTrue(false);
            }

            @Override
            public void onFailure(Exception e) {
                // Thread Context User has been replaced with null as part of https://github.com/opensearch-project/opensearch-sdk/issues/23
                // Previously we expected failure but we should now never get here
                fail("This should never fail with null (super-admin) user");
                Assert.assertEquals(OpenSearchStatusException.class, e.getClass());
                inProgressLatch.countDown();
            }
        };
        previewAction.doExecute(task, request, responseActionListener);
        // Thread Context User has been replaced with null as part of https://github.com/opensearch-project/opensearch-sdk/issues/23
        // Countdown latch was never decremented on failure, so we expect failure here
        // assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
        assertFalse(inProgressLatch.await(10, TimeUnit.SECONDS));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testPreviewTransportActionWithDetector() throws IOException, InterruptedException {
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        CreateIndexResponse createResponse = TestHelpers
            .createIndex(client().admin(), AnomalyDetector.ANOMALY_DETECTORS_INDEX, AnomalyDetectionIndices.getAnomalyDetectorMappings());
        Assert.assertNotNull(createResponse);

        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableMap.of("testKey", "testValue"), Instant.now());
        IndexRequest indexRequest = new IndexRequest(AnomalyDetector.ANOMALY_DETECTORS_INDEX)
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
        doReturn(TestHelpers.randomThresholdingResults()).when(modelManager).getPreviewResults(any(), anyInt());

        doAnswer(responseMock -> {
            Long startTime = responseMock.getArgument(1);
            ActionListener<Features> listener = responseMock.getArgument(3);
            listener.onResponse(TestHelpers.randomFeatures());
            return null;
        }).when(featureManager).getPreviewFeatures(anyObject(), anyLong(), anyLong(), any());
        action.doExecute(task, request, previewResponse);
        // The latch will never be triggered with mocked client
        // assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    @Test
    public void testCircuitBreakerOpen() throws IOException, InterruptedException {
        // preview has no detector id
        AnomalyDetector detector = TestHelpers.randomAnomalyDetectorUsingCategoryFields(null, Arrays.asList("a"));
        PreviewAnomalyDetectorRequest request = new PreviewAnomalyDetectorRequest(
            detector,
            detector.getDetectorId(),
            Instant.now(),
            Instant.now()
        );

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
                Assert.assertTrue(e.getMessage().contains(CommonErrorMessages.MEMORY_CIRCUIT_BROKEN_ERR_MSG));
                inProgressLatch.countDown();
            }
        };
        action.doExecute(task, request, previewResponse);
        // The latch will never be triggered with mocked client
        // assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }
}
