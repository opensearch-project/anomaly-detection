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

package org.opensearch.ad.ml;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.BACKOFF_MINUTES;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.tuple.Triple;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.ad.AbstractADTest;
import org.opensearch.ad.AnomalyDetectorPlugin;
import org.opensearch.ad.MemoryTracker;
import org.opensearch.ad.NodeStateManager;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.common.exception.AnomalyDetectionException;
import org.opensearch.ad.dataprocessor.IntegerSensitiveSingleFeatureLinearUniformInterpolator;
import org.opensearch.ad.dataprocessor.Interpolator;
import org.opensearch.ad.dataprocessor.LinearUniformInterpolator;
import org.opensearch.ad.dataprocessor.SingleFeatureLinearUniformInterpolator;
import org.opensearch.ad.feature.FeatureManager;
import org.opensearch.ad.feature.SearchFeatureDao;
import org.opensearch.ad.ml.ModelManager.ModelType;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.Entity;
import org.opensearch.ad.model.IntervalTimeConfiguration;
import org.opensearch.ad.ratelimit.CheckpointWriteWorker;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.util.ClientUtil;
import org.opensearch.client.Client;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchRejectedExecutionException;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import test.org.opensearch.ad.util.MLUtil;

import com.amazon.randomcutforest.RandomCutForest;
import com.amazon.randomcutforest.config.Precision;

public class EntityColdStarterTests extends AbstractADTest {
    int numMinSamples;
    String modelId;
    String entityName;
    String detectorId;
    ModelState<EntityModel> modelState;
    Clock clock;
    float priority;
    EntityColdStarter entityColdStarter;
    NodeStateManager stateManager;
    SearchFeatureDao searchFeatureDao;
    Interpolator interpolator;
    CheckpointDao checkpoint;
    FeatureManager featureManager;
    Settings settings;
    ThreadPool threadPool;
    AtomicBoolean released;
    Runnable releaseSemaphore;
    ActionListener<Void> listener;
    CountDownLatch inProgressLatch;
    CheckpointWriteWorker checkpointWriteQueue;
    Entity entity;
    AnomalyDetector detector;
    long rcfSeed;

    @SuppressWarnings("unchecked")
    @Override
    public void setUp() throws Exception {
        super.setUp();
        numMinSamples = AnomalyDetectorSettings.NUM_MIN_SAMPLES;

        clock = mock(Clock.class);
        when(clock.instant()).thenReturn(Instant.now());

        threadPool = mock(ThreadPool.class);
        setUpADThreadPool(threadPool);

        settings = Settings.EMPTY;

        Client client = mock(Client.class);
        ClientUtil clientUtil = mock(ClientUtil.class);

        detector = TestHelpers.randomAnomalyDetectorWithInterval(new IntervalTimeConfiguration(1, ChronoUnit.MINUTES), true, true);
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(2);
            listener.onResponse(TestHelpers.createGetResponse(detector, detectorId, AnomalyDetector.ANOMALY_DETECTORS_INDEX));
            return null;
        }).when(clientUtil).asyncRequest(any(GetRequest.class), any(), any(ActionListener.class));

        ModelPartitioner modelPartitioner = mock(ModelPartitioner.class);

        Set<Setting<?>> nodestateSetting = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        nodestateSetting.add(MAX_RETRY_FOR_UNRESPONSIVE_NODE);
        nodestateSetting.add(BACKOFF_MINUTES);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, nodestateSetting);

        DiscoveryNode discoveryNode = new DiscoveryNode(
            "node1",
            OpenSearchTestCase.buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );

        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool, discoveryNode, clusterSettings);

        stateManager = new NodeStateManager(
            client,
            xContentRegistry(),
            settings,
            clientUtil,
            clock,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            modelPartitioner,
            clusterService
        );

        SingleFeatureLinearUniformInterpolator singleFeatureLinearUniformInterpolator =
            new IntegerSensitiveSingleFeatureLinearUniformInterpolator();
        interpolator = new LinearUniformInterpolator(singleFeatureLinearUniformInterpolator);

        searchFeatureDao = mock(SearchFeatureDao.class);
        checkpoint = mock(CheckpointDao.class);

        featureManager = new FeatureManager(
            searchFeatureDao,
            interpolator,
            clock,
            AnomalyDetectorSettings.MAX_TRAIN_SAMPLE,
            AnomalyDetectorSettings.MAX_SAMPLE_STRIDE,
            AnomalyDetectorSettings.TRAIN_SAMPLE_TIME_RANGE_IN_HOURS,
            AnomalyDetectorSettings.MIN_TRAIN_SAMPLES,
            AnomalyDetectorSettings.MAX_SHINGLE_PROPORTION_MISSING,
            AnomalyDetectorSettings.MAX_IMPUTATION_NEIGHBOR_DISTANCE,
            AnomalyDetectorSettings.PREVIEW_SAMPLE_RATE,
            AnomalyDetectorSettings.MAX_PREVIEW_SAMPLES,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            threadPool,
            AnomalyDetectorPlugin.AD_THREAD_POOL_NAME
        );

        checkpointWriteQueue = mock(CheckpointWriteWorker.class);

        rcfSeed = 2051L;
        entityColdStarter = new EntityColdStarter(
            clock,
            threadPool,
            stateManager,
            AnomalyDetectorSettings.NUM_SAMPLES_PER_TREE,
            AnomalyDetectorSettings.NUM_TREES,
            AnomalyDetectorSettings.TIME_DECAY,
            numMinSamples,
            AnomalyDetectorSettings.MAX_SAMPLE_STRIDE,
            AnomalyDetectorSettings.MAX_TRAIN_SAMPLE,
            interpolator,
            searchFeatureDao,
            AnomalyDetectorSettings.THRESHOLD_MIN_PVALUE,
            AnomalyDetectorSettings.THRESHOLD_MAX_RANK_ERROR,
            AnomalyDetectorSettings.THRESHOLD_MAX_SCORE,
            AnomalyDetectorSettings.THRESHOLD_NUM_LOGNORMAL_QUANTILES,
            AnomalyDetectorSettings.THRESHOLD_DOWNSAMPLES,
            AnomalyDetectorSettings.THRESHOLD_MAX_SAMPLES,
            featureManager,
            settings,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            checkpointWriteQueue,
            rcfSeed
        );

        detectorId = "123";
        modelId = "123_entity_abc";
        entityName = "abc";
        priority = 0.3f;
        entity = Entity.createSingleAttributeEntity("field", entityName);

        released = new AtomicBoolean();

        inProgressLatch = new CountDownLatch(1);
        releaseSemaphore = () -> {
            released.set(true);
            inProgressLatch.countDown();
        };
        listener = ActionListener.wrap(releaseSemaphore);
    }

    private void checkSemaphoreRelease() throws InterruptedException {
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
        assertTrue(released.get());
    }

    // train using samples directly
    public void testTrainUsingSamples() throws InterruptedException {
        Queue<double[]> samples = MLUtil.createQueueSamples(numMinSamples);
        EntityModel model = new EntityModel(entity, samples, null, null);
        modelState = new ModelState<>(model, modelId, detectorId, ModelType.ENTITY.getName(), clock, priority);
        entityColdStarter.trainModel(entity, detectorId, modelState, listener);
        RandomCutForest forest = model.getRcf();
        assertTrue(forest != null);
        assertEquals(numMinSamples, forest.getTotalUpdates());
        assertTrue(model.getThreshold() != null);

        checkSemaphoreRelease();
    }

    public void testColdStart() throws InterruptedException, IOException {
        Queue<double[]> samples = MLUtil.createQueueSamples(1);
        EntityModel model = new EntityModel(entity, samples, null, null);
        modelState = new ModelState<>(model, modelId, detectorId, ModelType.ENTITY.getName(), clock, priority);

        doAnswer(invocation -> {
            ActionListener<Entry<Optional<Long>, Optional<Long>>> listener = invocation.getArgument(2);
            listener.onResponse(new SimpleImmutableEntry<>(Optional.of(1602269260000L), Optional.of(1602401500000L)));
            return null;
        }).when(searchFeatureDao).getEntityMinMaxDataTime(any(), any(), any());

        List<Optional<double[]>> coldStartSamples = new ArrayList<>();
        coldStartSamples.add(Optional.of(new double[] { 57.0 }));
        coldStartSamples.add(Optional.of(new double[] { 1.0 }));
        coldStartSamples.add(Optional.of(new double[] { -19.0 }));
        doAnswer(invocation -> {
            ActionListener<List<Optional<double[]>>> listener = invocation.getArgument(4);
            listener.onResponse(coldStartSamples);
            return null;
        }).when(searchFeatureDao).getColdStartSamplesForPeriods(any(), any(), any(), anyBoolean(), any());

        entityColdStarter.trainModel(entity, detectorId, modelState, listener);

        waitForColdStartFinish();
        RandomCutForest forest = model.getRcf();
        assertTrue(forest != null);
        // maxSampleStride * (continuousSampledArray.length - 1) + 1 = 64 * 2 + 1 = 129
        assertEquals(129, forest.getTotalUpdates());
        assertTrue(model.getThreshold() != null);

        // sleep 1 secs to give time for the last timestamp record to expire when superShortLastColdStartTimeState = true
        Thread.sleep(1000L);
        checkSemaphoreRelease();

        released.set(false);
        // too frequent cold start of the same detector will fail
        samples = MLUtil.createQueueSamples(1);
        model = new EntityModel(entity, samples, null, null);
        entityColdStarter.trainModel(entity, detectorId, modelState, listener);
        waitForColdStartFinish();

        forest = model.getRcf();

        assertTrue(forest == null);
        assertTrue(model.getThreshold() == null);
        checkSemaphoreRelease();
    }

    private void waitForColdStartFinish() throws InterruptedException {
        int maxWaitTimes = 20;
        int i = 0;
        while (stateManager.isColdStartRunning(detectorId) && i < maxWaitTimes) {
            // wait for 500 milliseconds
            Thread.sleep(500L);
            i++;
        }
    }

    // min max: miss one
    public void testMissMin() throws IOException, InterruptedException {
        Queue<double[]> samples = MLUtil.createQueueSamples(1);
        EntityModel model = new EntityModel(entity, samples, null, null);
        modelState = new ModelState<>(model, modelId, detectorId, ModelType.ENTITY.getName(), clock, priority);

        doAnswer(invocation -> {
            ActionListener<Entry<Optional<Long>, Optional<Long>>> listener = invocation.getArgument(2);
            listener.onResponse(new SimpleImmutableEntry<>(Optional.empty(), Optional.of(1602401500000L)));
            return null;
        }).when(searchFeatureDao).getEntityMinMaxDataTime(any(), any(), any());

        entityColdStarter.trainModel(entity, detectorId, modelState, listener);

        verify(searchFeatureDao, never()).getColdStartSamplesForPeriods(any(), any(), any(), anyBoolean(), any());

        RandomCutForest forest = model.getRcf();
        assertTrue(forest == null);
        assertTrue(model.getThreshold() == null);
        checkSemaphoreRelease();
    }

    // two segments of samples, one segment has 3 samples, while another one has only 1
    public void testTwoSegmentsWithSingleSample() throws InterruptedException, IOException {
        Queue<double[]> samples = MLUtil.createQueueSamples(1);
        EntityModel model = new EntityModel(entity, samples, null, null);
        modelState = new ModelState<>(model, modelId, detectorId, ModelType.ENTITY.getName(), clock, priority);

        doAnswer(invocation -> {
            ActionListener<Entry<Optional<Long>, Optional<Long>>> listener = invocation.getArgument(2);
            listener.onResponse(new SimpleImmutableEntry<>(Optional.of(1602269260000L), Optional.of(1602401500000L)));
            return null;
        }).when(searchFeatureDao).getEntityMinMaxDataTime(any(), any(), any());

        List<Optional<double[]>> coldStartSamples = new ArrayList<>();
        coldStartSamples.add(Optional.of(new double[] { 57.0 }));
        coldStartSamples.add(Optional.of(new double[] { 1.0 }));
        coldStartSamples.add(Optional.of(new double[] { -19.0 }));
        coldStartSamples.add(Optional.empty());
        coldStartSamples.add(Optional.of(new double[] { -17.0 }));
        doAnswer(invocation -> {
            ActionListener<List<Optional<double[]>>> listener = invocation.getArgument(4);
            listener.onResponse(coldStartSamples);
            return null;
        }).when(searchFeatureDao).getColdStartSamplesForPeriods(any(), any(), any(), anyBoolean(), any());

        entityColdStarter.trainModel(entity, detectorId, modelState, listener);

        int maxWaitTimes = 20;
        int i = 0;
        while (stateManager.isColdStartRunning(detectorId) && i < maxWaitTimes) {
            // wait for 1 second
            Thread.sleep(500L);
            i++;
        }
        RandomCutForest forest = model.getRcf();
        assertTrue(forest != null);
        // 1st segment: maxSampleStride * (continuousSampledArray.length - 1) + 1 = 64 * 2 + 1 = 129
        // 2nd segment: 1
        assertEquals(130, forest.getTotalUpdates());
        assertTrue(model.getThreshold() != null);
        checkSemaphoreRelease();
    }

    // two segments of samples, one segment has 3 samples, while another one 2 samples
    public void testTwoSegments() throws InterruptedException, IOException {
        Queue<double[]> samples = MLUtil.createQueueSamples(1);
        EntityModel model = new EntityModel(entity, samples, null, null);
        modelState = new ModelState<>(model, modelId, detectorId, ModelType.ENTITY.getName(), clock, priority);

        doAnswer(invocation -> {
            ActionListener<Entry<Optional<Long>, Optional<Long>>> listener = invocation.getArgument(2);
            listener.onResponse(new SimpleImmutableEntry<>(Optional.of(1602269260000L), Optional.of(1602401500000L)));
            return null;
        }).when(searchFeatureDao).getEntityMinMaxDataTime(any(), any(), any());

        List<Optional<double[]>> coldStartSamples = new ArrayList<>();
        coldStartSamples.add(Optional.of(new double[] { 57.0 }));
        coldStartSamples.add(Optional.of(new double[] { 1.0 }));
        coldStartSamples.add(Optional.of(new double[] { -19.0 }));
        coldStartSamples.add(Optional.empty());
        coldStartSamples.add(Optional.of(new double[] { -17.0 }));
        coldStartSamples.add(Optional.of(new double[] { -38.0 }));
        doAnswer(invocation -> {
            ActionListener<List<Optional<double[]>>> listener = invocation.getArgument(4);
            listener.onResponse(coldStartSamples);
            return null;
        }).when(searchFeatureDao).getColdStartSamplesForPeriods(any(), any(), any(), anyBoolean(), any());

        entityColdStarter.trainModel(entity, detectorId, modelState, listener);

        int maxWaitTimes = 20;
        int i = 0;
        while (stateManager.isColdStartRunning(detectorId) && i < maxWaitTimes) {
            // wait for 1 second
            Thread.sleep(500L);
            i++;
        }
        RandomCutForest forest = model.getRcf();
        assertTrue(forest != null);
        // 1st segment: maxSampleStride * (continuousSampledArray.length - 1) + 1 = 64 * 2 + 1 = 129
        // 2nd segment: maxSampleStride * (continuousSampledArray.length - 1) + 1 = 64 * 1 + 1 = 65
        assertEquals(194, forest.getTotalUpdates());
        assertTrue(model.getThreshold() != null);
        checkSemaphoreRelease();
    }

    public void testThrottledColdStart() throws InterruptedException {
        Queue<double[]> samples = MLUtil.createQueueSamples(1);
        EntityModel model = new EntityModel(entity, samples, null, null);
        modelState = new ModelState<>(model, modelId, detectorId, ModelType.ENTITY.getName(), clock, priority);

        doAnswer(invocation -> {
            ActionListener<Entry<Optional<Long>, Optional<Long>>> listener = invocation.getArgument(2);
            listener.onFailure(new OpenSearchRejectedExecutionException(""));
            return null;
        }).when(searchFeatureDao).getEntityMinMaxDataTime(any(), any(), any());

        entityColdStarter.trainModel(entity, detectorId, modelState, listener);

        entityColdStarter.trainModel(entity, "456", modelState, listener);

        // only the first one makes the call
        verify(searchFeatureDao, times(1)).getEntityMinMaxDataTime(any(), any(), any());
        checkSemaphoreRelease();
    }

    public void testColdStartException() throws InterruptedException {
        Queue<double[]> samples = MLUtil.createQueueSamples(1);
        EntityModel model = new EntityModel(entity, samples, null, null);
        modelState = new ModelState<>(model, modelId, detectorId, ModelType.ENTITY.getName(), clock, priority);

        doAnswer(invocation -> {
            ActionListener<Entry<Optional<Long>, Optional<Long>>> listener = invocation.getArgument(2);
            listener.onFailure(new AnomalyDetectionException(detectorId, ""));
            return null;
        }).when(searchFeatureDao).getEntityMinMaxDataTime(any(), any(), any());

        entityColdStarter.trainModel(entity, detectorId, modelState, listener);

        assertTrue(stateManager.getLastDetectionError(detectorId) != null);
        checkSemaphoreRelease();
    }

    public void testNotEnoughSamples() throws InterruptedException, IOException {
        Queue<double[]> samples = MLUtil.createQueueSamples(1);
        EntityModel model = new EntityModel(entity, samples, null, null);
        modelState = new ModelState<>(model, modelId, detectorId, ModelType.ENTITY.getName(), clock, priority);

        doAnswer(invocation -> {
            ActionListener<Entry<Optional<Long>, Optional<Long>>> listener = invocation.getArgument(2);
            listener.onResponse(new SimpleImmutableEntry<>(Optional.of(1602269260000L), Optional.of(1602401500000L)));
            return null;
        }).when(searchFeatureDao).getEntityMinMaxDataTime(any(), any(), any());

        List<Optional<double[]>> coldStartSamples = new ArrayList<>();
        coldStartSamples.add(Optional.of(new double[] { 57.0 }));
        coldStartSamples.add(Optional.of(new double[] { 1.0 }));
        doAnswer(invocation -> {
            ActionListener<List<Optional<double[]>>> listener = invocation.getArgument(4);
            listener.onResponse(coldStartSamples);
            return null;
        }).when(searchFeatureDao).getColdStartSamplesForPeriods(any(), any(), any(), anyBoolean(), any());

        entityColdStarter.trainModel(entity, detectorId, modelState, listener);

        int maxWaitTimes = 20;
        int i = 0;
        while (stateManager.isColdStartRunning(detectorId) && i < maxWaitTimes) {
            // wait for 1 second
            Thread.sleep(500L);
            i++;
        }
        assertTrue(model.getRcf() == null);
        assertTrue(model.getThreshold() == null);
        // 1st segment: maxSampleStride * (continuousSampledArray.length - 1) + 1 = 64 * 1 + 1 = 65
        // 65 + origin 1 data points
        assertEquals("real sample size is " + model.getSamples().size(), 66, model.getSamples().size());
    }

    public void testEmptyDataRange() throws InterruptedException {
        Queue<double[]> samples = MLUtil.createQueueSamples(1);
        EntityModel model = new EntityModel(entity, samples, null, null);
        modelState = new ModelState<>(model, modelId, detectorId, ModelType.ENTITY.getName(), clock, priority);

        doAnswer(invocation -> {
            ActionListener<Entry<Optional<Long>, Optional<Long>>> listener = invocation.getArgument(2);
            listener.onResponse(new SimpleImmutableEntry<>(Optional.of(894056973000L), Optional.of(894057860000L)));
            return null;
        }).when(searchFeatureDao).getEntityMinMaxDataTime(any(), any(), any());

        entityColdStarter.trainModel(entity, detectorId, modelState, listener);
        waitForColdStartFinish();

        assertTrue(model.getRcf() == null);
        assertTrue(model.getThreshold() == null);
        // the min-max range is too small and thus no data range can be found
        assertEquals("real sample size is " + model.getSamples().size(), 1, model.getSamples().size());
    }

    public void testTrainModelFromExistingSamplesEnoughSamples() {
        int inputDimension = 2;
        int dimensions = inputDimension * detector.getShingleSize();

        RandomCutForest.Builder<?> rcfConfig = RandomCutForest
            .builder()
            .compact(true)
            .dimensions(dimensions)
            .precision(Precision.FLOAT_32)
            .randomSeed(rcfSeed)
            .numberOfTrees(AnomalyDetectorSettings.NUM_TREES)
            .shingleSize(detector.getShingleSize())
            .boundingBoxCacheFraction(AnomalyDetectorSettings.REAL_TIME_BOUNDING_BOX_CACHE_RATIO)
            .timeDecay(AnomalyDetectorSettings.TIME_DECAY)
            .outputAfter(numMinSamples)
            .parallelExecutionEnabled(false)
            .sampleSize(AnomalyDetectorSettings.NUM_SAMPLES_PER_TREE)
            .internalShinglingEnabled(true);
        Triple<Queue<double[]>, RandomCutForest, ThresholdingModel> models = MLUtil.prepareModel(inputDimension, rcfConfig);
        Queue<double[]> samples = models.getLeft();
        RandomCutForest rcf = models.getMiddle();
        ThresholdingModel threshold = models.getRight();

        EntityModel model = new EntityModel(entity, samples, null, null);
        modelState = new ModelState<>(model, modelId, detectorId, ModelType.ENTITY.getName(), clock, priority);

        ModelManager modelManager = new ModelManager(
            mock(CheckpointDao.class),
            mock(Clock.class),
            AnomalyDetectorSettings.NUM_TREES,
            AnomalyDetectorSettings.NUM_SAMPLES_PER_TREE,
            AnomalyDetectorSettings.TIME_DECAY,
            AnomalyDetectorSettings.NUM_MIN_SAMPLES,
            AnomalyDetectorSettings.THRESHOLD_MIN_PVALUE,
            AnomalyDetectorSettings.THRESHOLD_MAX_RANK_ERROR,
            AnomalyDetectorSettings.THRESHOLD_MAX_SCORE,
            AnomalyDetectorSettings.THRESHOLD_NUM_LOGNORMAL_QUANTILES,
            AnomalyDetectorSettings.THRESHOLD_DOWNSAMPLES,
            AnomalyDetectorSettings.THRESHOLD_MAX_SAMPLES,
            AnomalyDetectorSettings.MIN_PREVIEW_SIZE,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            entityColdStarter,
            mock(ModelPartitioner.class),
            mock(FeatureManager.class),
            mock(MemoryTracker.class)
        );

        Random r = new Random();

        // make sure we trained the expected models
        for (int i = 0; i < 100; i++) {
            double[] point = r.ints(inputDimension, 0, 50).asDoubleStream().toArray();
            double score = rcf.getAnomalyScore(point);
            ThresholdingResult result = modelManager
                .getAnomalyResultForEntity(point, modelState, modelId, entity, detector.getShingleSize());
            assertEquals(score, result.getRcfScore(), 1e-10);
            assertEquals(threshold.grade(score), result.getGrade(), 1e-10);
            assertEquals(modelManager.computeRcfConfidence(rcf) * threshold.confidence(), result.getConfidence(), 1e-10);
            rcf.update(point);
            threshold.update(score);
        }
    }

    public void testTrainModelFromExistingSamplesNotEnoughSamples() {
        Queue<double[]> samples = new ArrayDeque<>();
        EntityModel model = new EntityModel(entity, samples, null, null);
        modelState = new ModelState<>(model, modelId, detectorId, ModelType.ENTITY.getName(), clock, priority);
        entityColdStarter.trainModelFromExistingSamples(modelState, detector.getShingleSize());
        RandomCutForest adRCF = modelState.getModel().getRcf();
        ThresholdingModel adThreshold = modelState.getModel().getThreshold();
        assertTrue(adRCF == null);
        assertTrue(adThreshold == null);
    }
}
