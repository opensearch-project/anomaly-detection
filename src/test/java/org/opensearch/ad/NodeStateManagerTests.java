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

package org.opensearch.ad;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.BACKOFF_MINUTES;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.transport.AnomalyResultTests;
import org.opensearch.ad.util.ClientUtil;
import org.opensearch.ad.util.Throttler;
import org.opensearch.client.Client;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.search.SearchModule;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;

import com.google.common.collect.ImmutableMap;

public class NodeStateManagerTests extends AbstractADTest {
    private NodeStateManager stateManager;
    private Client client;
    private ClientUtil clientUtil;
    private Clock clock;
    private Duration duration;
    private Throttler throttler;
    private AnomalyDetector detectorToCheck;
    private Settings settings;
    private String adId = "123";
    private String nodeId = "123";

    private GetResponse checkpointResponse;
    private ClusterService clusterService;
    private ClusterSettings clusterSettings;
    // private AnomalyDetectorJob jobToCheck;

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @BeforeClass
    public static void setUpBeforeClass() {
        setUpThreadPool(AnomalyResultTests.class.getSimpleName());
    }

    @AfterClass
    public static void tearDownAfterClass() {
        tearDownThreadPool();
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        client = mock(Client.class);
        settings = Settings
            .builder()
            .put("plugins.anomaly_detection.max_retry_for_unresponsive_node", 3)
            .put("plugins.anomaly_detection.ad_mute_minutes", TimeValue.timeValueMinutes(10))
            .build();
        clock = mock(Clock.class);
        duration = Duration.ofHours(1);
        throttler = new Throttler(clock);

        clientUtil = new ClientUtil(Settings.EMPTY, client, throttler);
        Set<Setting<?>> nodestateSetting = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        nodestateSetting.add(MAX_RETRY_FOR_UNRESPONSIVE_NODE);
        nodestateSetting.add(BACKOFF_MINUTES);
        clusterSettings = new ClusterSettings(Settings.EMPTY, nodestateSetting);

        DiscoveryNode discoveryNode = new DiscoveryNode(
            "node1",
            OpenSearchTestCase.buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );

        clusterService = ClusterServiceUtils.createClusterService(threadPool, discoveryNode, clusterSettings);
        stateManager = new NodeStateManager(client, xContentRegistry(), settings, clientUtil, clock, duration, clusterService);

        checkpointResponse = mock(GetResponse.class);
        // jobToCheck = TestHelpers.randomAnomalyDetectorJob(true, Instant.ofEpochMilli(1602401500000L), null);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        stateManager = null;
        client = null;
        clientUtil = null;
        detectorToCheck = null;
    }

    @SuppressWarnings("unchecked")
    private String setupDetector() throws IOException {
        detectorToCheck = TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null, true);

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(
                String.format(Locale.ROOT, "The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)),
                args.length >= 2
            );

            GetRequest request = null;
            ActionListener<GetResponse> listener = null;
            if (args[0] instanceof GetRequest) {
                request = (GetRequest) args[0];
            }
            if (args[1] instanceof ActionListener) {
                listener = (ActionListener<GetResponse>) args[1];
            }

            assertTrue(request != null && listener != null);
            listener
                .onResponse(
                    TestHelpers.createGetResponse(detectorToCheck, detectorToCheck.getDetectorId(), AnomalyDetector.ANOMALY_DETECTORS_INDEX)
                );

            return null;
        }).when(client).get(any(), any(ActionListener.class));
        return detectorToCheck.getDetectorId();
    }

    @SuppressWarnings("unchecked")
    private void setupCheckpoint(boolean responseExists) throws IOException {
        when(checkpointResponse.isExists()).thenReturn(responseExists);

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(
                String.format(Locale.ROOT, "The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)),
                args.length >= 2
            );

            GetRequest request = null;
            ActionListener<GetResponse> listener = null;
            if (args[0] instanceof GetRequest) {
                request = (GetRequest) args[0];
            }
            if (args[1] instanceof ActionListener) {
                listener = (ActionListener<GetResponse>) args[1];
            }

            assertTrue(request != null && listener != null);
            listener.onResponse(checkpointResponse);

            return null;
        }).when(client).get(any(), any(ActionListener.class));
    }

    public void testGetLastError() throws IOException, InterruptedException {
        String error = "blah";
        assertEquals(NodeStateManager.NO_ERROR, stateManager.getLastDetectionError(adId));
        stateManager.setLastDetectionError(adId, error);
        assertEquals(error, stateManager.getLastDetectionError(adId));
    }

    public void testShouldMute() {
        assertTrue(!stateManager.isMuted(nodeId, adId));

        when(clock.millis()).thenReturn(10000L);
        IntStream.range(0, 4).forEach(j -> stateManager.addPressure(nodeId, adId));

        when(clock.millis()).thenReturn(20000L);
        assertTrue(stateManager.isMuted(nodeId, adId));

        // > 15 minutes have passed, we should not mute anymore
        when(clock.millis()).thenReturn(1000001L);
        assertTrue(!stateManager.isMuted(nodeId, adId));

        // the backpressure counter should be reset
        when(clock.millis()).thenReturn(100001L);
        stateManager.resetBackpressureCounter(nodeId, adId);
        assertTrue(!stateManager.isMuted(nodeId, adId));
    }

    public void testMaintenanceDoNothing() {
        stateManager.maintenance();

        verifyZeroInteractions(clock);
    }

    public void testHasRunningQuery() throws IOException {
        stateManager = new NodeStateManager(
            client,
            xContentRegistry(),
            settings,
            new ClientUtil(settings, client, throttler),
            clock,
            duration,
            clusterService
        );

        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableMap.of(), null);
        SearchRequest dummySearchRequest = new SearchRequest();
        assertFalse(stateManager.hasRunningQuery(detector));
        throttler.insertFilteredQuery(detector.getDetectorId(), dummySearchRequest);
        assertTrue(stateManager.hasRunningQuery(detector));
    }

    public void testGetAnomalyDetector() throws IOException, InterruptedException {
        String detectorId = setupDetector();
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        stateManager.getAnomalyDetector(detectorId, ActionListener.wrap(asDetector -> {
            assertEquals(detectorToCheck, asDetector.get());
            inProgressLatch.countDown();
        }, exception -> {
            assertTrue(false);
            inProgressLatch.countDown();
        }));
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    /**
     * Test that we caches anomaly detector definition after the first call
     * @throws IOException if client throws exception
     * @throws InterruptedException  if the current thread is interrupted while waiting
     */
    @SuppressWarnings("unchecked")
    public void testRepeatedGetAnomalyDetector() throws IOException, InterruptedException {
        String detectorId = setupDetector();
        final CountDownLatch inProgressLatch = new CountDownLatch(2);

        stateManager.getAnomalyDetector(detectorId, ActionListener.wrap(asDetector -> {
            assertEquals(detectorToCheck, asDetector.get());
            inProgressLatch.countDown();
        }, exception -> {
            assertTrue(false);
            inProgressLatch.countDown();
        }));

        stateManager.getAnomalyDetector(detectorId, ActionListener.wrap(asDetector -> {
            assertEquals(detectorToCheck, asDetector.get());
            inProgressLatch.countDown();
        }, exception -> {
            assertTrue(false);
            inProgressLatch.countDown();
        }));

        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));

        verify(client, times(1)).get(any(), any(ActionListener.class));
    }

    public void getCheckpointTestTemplate(boolean exists) throws IOException {
        setupCheckpoint(exists);
        when(clock.instant()).thenReturn(Instant.MIN);
        stateManager
            .getDetectorCheckpoint(adId, ActionListener.wrap(checkpointExists -> { assertEquals(exists, checkpointExists); }, exception -> {
                for (StackTraceElement ste : exception.getStackTrace()) {
                    logger.info(ste);
                }
                assertTrue(false);
            }));
    }

    public void testCheckpointExists() throws IOException {
        getCheckpointTestTemplate(true);
    }

    public void testCheckpointNotExists() throws IOException {
        getCheckpointTestTemplate(false);
    }

    public void testMaintenanceNotRemove() throws IOException {
        setupCheckpoint(true);
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(1));
        stateManager
            .getDetectorCheckpoint(
                adId,
                ActionListener.wrap(gotCheckpoint -> { assertTrue(gotCheckpoint); }, exception -> assertTrue(false))
            );
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(1));
        stateManager.maintenance();
        stateManager
            .getDetectorCheckpoint(adId, ActionListener.wrap(gotCheckpoint -> assertTrue(gotCheckpoint), exception -> assertTrue(false)));
        verify(client, times(1)).get(any(), any());
    }

    public void testMaintenanceRemove() throws IOException {
        setupCheckpoint(true);
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(1));
        stateManager
            .getDetectorCheckpoint(
                adId,
                ActionListener.wrap(gotCheckpoint -> { assertTrue(gotCheckpoint); }, exception -> assertTrue(false))
            );
        when(clock.instant()).thenReturn(Instant.ofEpochSecond(7200L));
        stateManager.maintenance();
        stateManager
            .getDetectorCheckpoint(
                adId,
                ActionListener.wrap(gotCheckpoint -> { assertTrue(gotCheckpoint); }, exception -> assertTrue(false))
            );
        verify(client, times(2)).get(any(), any());
    }

    public void testColdStartRunning() {
        assertTrue(!stateManager.isColdStartRunning(adId));
        stateManager.markColdStartRunning(adId);
        assertTrue(stateManager.isColdStartRunning(adId));
    }

    public void testSettingUpdateMaxRetry() {
        when(clock.millis()).thenReturn(System.currentTimeMillis());
        stateManager.addPressure(nodeId, adId);
        // In setUp method, we mute after 3 tries
        assertTrue(!stateManager.isMuted(nodeId, adId));

        Settings newSettings = Settings.builder().put(AnomalyDetectorSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE.getKey(), "1").build();
        Settings.Builder target = Settings.builder();
        clusterSettings.updateDynamicSettings(newSettings, target, Settings.builder(), "test");
        clusterSettings.applySettings(target.build());
        stateManager.addPressure(nodeId, adId);
        // since we have one violation and the max is 1, this is flagged as muted
        assertTrue(stateManager.isMuted(nodeId, adId));
    }

    public void testSettingUpdateBackOffMin() {
        when(clock.millis()).thenReturn(1000L);
        // In setUp method, we mute after 3 tries
        for (int i = 0; i < 4; i++) {
            stateManager.addPressure(nodeId, adId);
        }

        assertTrue(stateManager.isMuted(nodeId, adId));

        Settings newSettings = Settings.builder().put(AnomalyDetectorSettings.BACKOFF_MINUTES.getKey(), "1m").build();
        Settings.Builder target = Settings.builder();
        clusterSettings.updateDynamicSettings(newSettings, target, Settings.builder(), "test");
        clusterSettings.applySettings(target.build());
        stateManager.addPressure(nodeId, adId);
        // move the clobk by 1000 milliseconds
        // when evaluating isMuted, 62000 - 1000 (last mute time) > 60000, which
        // make isMuted true
        when(clock.millis()).thenReturn(62000L);
        assertTrue(!stateManager.isMuted(nodeId, adId));
    }

    // @SuppressWarnings("unchecked")
    /*private String setupJob() throws IOException {
        String detectorId = jobToCheck.getName();
    
        doAnswer(invocation -> {
            GetRequest request = invocation.getArgument(0);
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            if (request.index().equals(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX)) {
                listener.onResponse(TestHelpers.createGetResponse(jobToCheck, detectorId, AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX));
            }
            return null;
        }).when(client).get(any(), any(ActionListener.class));
    
        return detectorId;
    }*/

    // public void testGetAnomalyJob() throws IOException, InterruptedException {
    // String detectorId = setupJob();
    // final CountDownLatch inProgressLatch = new CountDownLatch(1);
    // stateManager.getAnomalyDetectorJob(detectorId, ActionListener.wrap(asDetector -> {
    // assertEquals(jobToCheck, asDetector.get());
    // inProgressLatch.countDown();
    // }, exception -> {
    // assertTrue(false);
    // inProgressLatch.countDown();
    // }));
    // assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    // }

    /**
     * Test that we caches anomaly detector job definition after the first call
     * @throws IOException if client throws exception
     * @throws InterruptedException  if the current thread is interrupted while waiting
     */
    // @SuppressWarnings("unchecked")
    // public void testRepeatedGetAnomalyJob() throws IOException, InterruptedException {
    // String detectorId = setupJob();
    // final CountDownLatch inProgressLatch = new CountDownLatch(2);
    //
    // stateManager.getAnomalyDetectorJob(detectorId, ActionListener.wrap(asDetector -> {
    // assertEquals(jobToCheck, asDetector.get());
    // inProgressLatch.countDown();
    // }, exception -> {
    // assertTrue(false);
    // inProgressLatch.countDown();
    // }));
    //
    // stateManager.getAnomalyDetectorJob(detectorId, ActionListener.wrap(asDetector -> {
    // assertEquals(jobToCheck, asDetector.get());
    // inProgressLatch.countDown();
    // }, exception -> {
    // assertTrue(false);
    // inProgressLatch.countDown();
    // }));
    //
    // assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    //
    // verify(client, times(1)).get(any(), any(ActionListener.class));
    // }
}
