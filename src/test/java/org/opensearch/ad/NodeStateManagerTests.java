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

/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.ad;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Before;
import org.opensearch.action.ActionListener;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.ad.ml.ModelPartitioner;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.util.ClientUtil;
import org.opensearch.ad.util.Throttler;
import org.opensearch.client.Client;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.search.SearchModule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import com.google.common.collect.ImmutableMap;

public class NodeStateManagerTests extends OpenSearchTestCase {
    private NodeStateManager stateManager;
    private ModelPartitioner modelPartitioner;
    private Client client;
    private ClientUtil clientUtil;
    private Clock clock;
    private Duration duration;
    private Throttler throttler;
    private ThreadPool context;
    private AnomalyDetector detectorToCheck;
    private Settings settings;
    private String adId = "123";

    private GetResponse checkpointResponse;

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        modelPartitioner = mock(ModelPartitioner.class);
        when(modelPartitioner.getPartitionedForestSizes(any(AnomalyDetector.class))).thenReturn(new SimpleImmutableEntry<>(2, 20));
        client = mock(Client.class);
        settings = Settings
            .builder()
            .put("plugins.anomaly_detection.max_retry_for_unresponsive_node", 3)
            .put("plugins.anomaly_detection.ad_mute_minutes", TimeValue.timeValueMinutes(10))
            .build();
        clock = mock(Clock.class);
        duration = Duration.ofHours(1);
        context = TestHelpers.createThreadPool();
        throttler = new Throttler(clock);

        clientUtil = new ClientUtil(Settings.EMPTY, client, throttler, mock(ThreadPool.class));
        stateManager = new NodeStateManager(client, xContentRegistry(), settings, clientUtil, clock, duration, modelPartitioner);

        checkpointResponse = mock(GetResponse.class);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        stateManager = null;
        modelPartitioner = null;
        client = null;
        clientUtil = null;
        detectorToCheck = null;
    }

    @SuppressWarnings("unchecked")
    private String setupDetector() throws IOException {
        detectorToCheck = TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null, true);

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(String.format("The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)), args.length >= 2);

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
            assertTrue(String.format("The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)), args.length >= 2);

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

    public void testGetPartitionNumber() throws IOException, InterruptedException {
        String detectorId = setupDetector();
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null);
        for (int i = 0; i < 2; i++) {
            // call two times should return the same result
            int partitionNumber = stateManager.getPartitionNumber(detectorId, detector);
            assertEquals(2, partitionNumber);
        }

        // the 2nd call should directly fetch cached result
        verify(modelPartitioner, times(1)).getPartitionedForestSizes(any());
    }

    public void testGetLastError() throws IOException, InterruptedException {
        String error = "blah";
        assertEquals(NodeStateManager.NO_ERROR, stateManager.getLastDetectionError(adId));
        stateManager.setLastDetectionError(adId, error);
        assertEquals(error, stateManager.getLastDetectionError(adId));
    }

    public void testShouldMute() {
        String nodeId = "123";
        assertTrue(!stateManager.isMuted(nodeId));

        when(clock.millis()).thenReturn(10000L);
        IntStream.range(0, 4).forEach(j -> stateManager.addPressure(nodeId));

        when(clock.millis()).thenReturn(20000L);
        assertTrue(stateManager.isMuted(nodeId));

        // > 15 minutes have passed, we should not mute anymore
        when(clock.millis()).thenReturn(1000001L);
        assertTrue(!stateManager.isMuted(nodeId));

        // the backpressure counter should be reset
        when(clock.millis()).thenReturn(100001L);
        stateManager.resetBackpressureCounter(nodeId);
        assertTrue(!stateManager.isMuted(nodeId));
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
            new ClientUtil(settings, client, throttler, context),
            clock,
            duration,
            modelPartitioner
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
}
