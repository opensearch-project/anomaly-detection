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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
import java.util.stream.Collectors;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
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
import org.opensearch.ad.settings.EnabledSetting;
import org.opensearch.ad.util.ClientUtil;
import org.opensearch.client.Client;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchRejectedExecutionException;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import test.org.opensearch.ad.util.LabelledAnomalyGenerator;
import test.org.opensearch.ad.util.MLUtil;
import test.org.opensearch.ad.util.MultiDimDataWithTime;

import com.amazon.randomcutforest.config.Precision;
import com.amazon.randomcutforest.parkservices.AnomalyDescriptor;
import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;
import com.google.common.collect.ImmutableList;

@Ignore
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
    ModelManager modelManager;
    ClientUtil clientUtil;

    @BeforeClass
    public static void initOnce() {
        ClusterService clusterService = mock(ClusterService.class);

        Set<Setting<?>> settingSet = EnabledSetting.settings.values().stream().collect(Collectors.toSet());

        when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(Settings.EMPTY, settingSet));

        EnabledSetting.getInstance().init(clusterService);
    }

    @AfterClass
    public static void clearOnce() {
        // restore to default value
        EnabledSetting.getInstance().setSettingValue(EnabledSetting.INTERPOLATION_IN_HCAD_COLD_START_ENABLED, false);
    }

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
        clientUtil = mock(ClientUtil.class);

        detector = TestHelpers.AnomalyDetectorBuilder
            .newInstance()
            .setDetectionInterval(new IntervalTimeConfiguration(1, ChronoUnit.MINUTES))
            .setCategoryFields(ImmutableList.of(randomAlphaOfLength(5)))
            .build();
        when(clock.millis()).thenReturn(1602401500000L);
        doAnswer(invocation -> {
            GetRequest request = invocation.getArgument(0);
            ActionListener<GetResponse> listener = invocation.getArgument(2);

            listener.onResponse(TestHelpers.createGetResponse(detector, detectorId, AnomalyDetector.ANOMALY_DETECTORS_INDEX));

            return null;
        }).when(clientUtil).asyncRequest(any(GetRequest.class), any(), any(ActionListener.class));

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
            featureManager,
            settings,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            checkpointWriteQueue,
            rcfSeed,
            AnomalyDetectorSettings.MAX_COLD_START_ROUNDS
        );
        EnabledSetting.getInstance().setSettingValue(EnabledSetting.INTERPOLATION_IN_HCAD_COLD_START_ENABLED, Boolean.TRUE);

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

        modelManager = new ModelManager(
            mock(CheckpointDao.class),
            mock(Clock.class),
            AnomalyDetectorSettings.NUM_TREES,
            AnomalyDetectorSettings.NUM_SAMPLES_PER_TREE,
            AnomalyDetectorSettings.TIME_DECAY,
            AnomalyDetectorSettings.NUM_MIN_SAMPLES,
            AnomalyDetectorSettings.THRESHOLD_MIN_PVALUE,
            AnomalyDetectorSettings.MIN_PREVIEW_SIZE,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            entityColdStarter,
            mock(FeatureManager.class),
            mock(MemoryTracker.class)
        );
    }

    @Override
    public void tearDown() throws Exception {
        EnabledSetting.getInstance().setSettingValue(EnabledSetting.INTERPOLATION_IN_HCAD_COLD_START_ENABLED, Boolean.FALSE);
        super.tearDown();
    }

    private void checkSemaphoreRelease() throws InterruptedException {
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
        assertTrue(released.get());
    }

    // train using samples directly
    public void testTrainUsingSamples() throws InterruptedException {
        Queue<double[]> samples = MLUtil.createQueueSamples(numMinSamples);
        EntityModel model = new EntityModel(entity, samples, null);
        modelState = new ModelState<>(model, modelId, detectorId, ModelType.ENTITY.getName(), clock, priority);
        entityColdStarter.trainModel(entity, detectorId, modelState, listener);
        assertTrue(model.getTrcf().isPresent());
        ThresholdedRandomCutForest ercf = model.getTrcf().get();
        assertEquals(numMinSamples, ercf.getForest().getTotalUpdates());

        checkSemaphoreRelease();
    }

    /*public void testColdStart() throws InterruptedException, IOException {
        Queue<double[]> samples = MLUtil.createQueueSamples(1);
        double[] savedSample = samples.peek();
        EntityModel model = new EntityModel(entity, samples, null);
        modelState = new ModelState<>(model, modelId, detectorId, ModelType.ENTITY.getName(), clock, priority);
    
        doAnswer(invocation -> {
            ActionListener<Optional<Long>> listener = invocation.getArgument(2);
            listener.onResponse(Optional.of(1602269260000L));
            return null;
        }).when(searchFeatureDao).getEntityMinDataTime(any(), any(), any());
    
        List<Optional<double[]>> coldStartSamples = new ArrayList<>();
    
        double[] sample1 = new double[] { 57.0 };
        double[] sample2 = new double[] { 1.0 };
        double[] sample3 = new double[] { -19.0 };
    
        coldStartSamples.add(Optional.of(sample1));
        coldStartSamples.add(Optional.of(sample2));
        coldStartSamples.add(Optional.of(sample3));
        doAnswer(invocation -> {
            ActionListener<List<Optional<double[]>>> listener = invocation.getArgument(4);
            listener.onResponse(coldStartSamples);
            return null;
        }).when(searchFeatureDao).getColdStartSamplesForPeriods(any(), any(), any(), anyBoolean(), any());
    
        entityColdStarter.trainModel(entity, detectorId, modelState, listener);
        checkSemaphoreRelease();
    
        assertTrue(model.getTrcf().isPresent());
        ThresholdedRandomCutForest ercf = model.getTrcf().get();
        // 1 round: stride * (samples - 1) + 1 = 60 * 2 + 1 = 121
        // plus 1 existing sample
        assertEquals(121, ercf.getForest().getTotalUpdates());
        assertTrue("size: " + model.getSamples().size(), model.getSamples().isEmpty());
    
        checkSemaphoreRelease();
    
        released.set(false);
        // too frequent cold start of the same detector will fail
        samples = MLUtil.createQueueSamples(1);
        model = new EntityModel(entity, samples, null);
        entityColdStarter.trainModel(entity, detectorId, modelState, listener);
    
        assertFalse(model.getTrcf().isPresent());
        // the samples is not touched since cold start does not happen
        assertEquals("size: " + model.getSamples().size(), 1, model.getSamples().size());
        checkSemaphoreRelease();
    
        List<double[]> expectedColdStartData = new ArrayList<>();
    
        // for function interpolate:
        // 1st parameter is a matrix of size numFeatures * numSamples
        // 2nd parameter is the number of interpolants including two samples
        double[][] interval1 = interpolator.interpolate(new double[][] { new double[] { sample1[0], sample2[0] } }, 61);
        expectedColdStartData.addAll(convertToFeatures(interval1, 60));
        double[][] interval2 = interpolator.interpolate(new double[][] { new double[] { sample2[0], sample3[0] } }, 61);
        expectedColdStartData.addAll(convertToFeatures(interval2, 61));
        assertEquals(121, expectedColdStartData.size());
    
        diffTesting(modelState, expectedColdStartData);
    }*/

    // min max: miss one
    public void testMissMin() throws IOException, InterruptedException {
        Queue<double[]> samples = MLUtil.createQueueSamples(1);
        EntityModel model = new EntityModel(entity, samples, null);
        modelState = new ModelState<>(model, modelId, detectorId, ModelType.ENTITY.getName(), clock, priority);

        doAnswer(invocation -> {
            ActionListener<Optional<Long>> listener = invocation.getArgument(2);
            listener.onResponse(Optional.empty());
            return null;
        }).when(searchFeatureDao).getEntityMinDataTime(any(), any(), any());

        entityColdStarter.trainModel(entity, detectorId, modelState, listener);

        verify(searchFeatureDao, never()).getColdStartSamplesForPeriods(any(), any(), any(), anyBoolean(), any());

        assertTrue(!model.getTrcf().isPresent());
        checkSemaphoreRelease();
    }

    /**
     * Performan differential testing using trcf model with input cold start data and the modelState
     * @param modelState an initialized model state
     * @param coldStartData cold start data that initialized the modelState
     */
    private void diffTesting(ModelState<EntityModel> modelState, List<double[]> coldStartData) {
        int inputDimension = detector.getEnabledFeatureIds().size();

        ThresholdedRandomCutForest refTRcf = ThresholdedRandomCutForest
            .builder()
            .compact(true)
            .dimensions(inputDimension * detector.getShingleSize())
            .precision(Precision.FLOAT_32)
            .randomSeed(rcfSeed)
            .numberOfTrees(AnomalyDetectorSettings.NUM_TREES)
            .shingleSize(detector.getShingleSize())
            .boundingBoxCacheFraction(AnomalyDetectorSettings.REAL_TIME_BOUNDING_BOX_CACHE_RATIO)
            .timeDecay(AnomalyDetectorSettings.TIME_DECAY)
            .outputAfter(numMinSamples)
            .initialAcceptFraction(0.125d)
            .parallelExecutionEnabled(false)
            .sampleSize(AnomalyDetectorSettings.NUM_SAMPLES_PER_TREE)
            .internalShinglingEnabled(true)
            .anomalyRate(1 - AnomalyDetectorSettings.THRESHOLD_MIN_PVALUE)
            .build();

        for (int i = 0; i < coldStartData.size(); i++) {
            refTRcf.process(coldStartData.get(i), 0);
        }
        assertEquals(
            "Expect " + coldStartData.size() + " but got " + refTRcf.getForest().getTotalUpdates(),
            coldStartData.size(),
            refTRcf.getForest().getTotalUpdates()
        );

        Random r = new Random();

        // make sure we trained the expected models
        for (int i = 0; i < 100; i++) {
            double[] point = r.ints(inputDimension, 0, 50).asDoubleStream().toArray();
            AnomalyDescriptor descriptor = refTRcf.process(point, 0);
            ThresholdingResult result = modelManager
                .getAnomalyResultForEntity(point, modelState, modelId, entity, detector.getShingleSize());
            assertEquals(descriptor.getRCFScore(), result.getRcfScore(), 1e-10);
            assertEquals(descriptor.getAnomalyGrade(), result.getGrade(), 1e-10);
        }
    }

    /**
     * Convert a double array of size numFeatures * numSamples to a double array of
     * size numSamples * numFeatures
     * @param interval input array
     * @param numValsToKeep number of samples to keep in the input array.  Used to
     *  keep the last sample in the input array out in case of repeated inclusion
     * @return converted value
     */
    private List<double[]> convertToFeatures(double[][] interval, int numValsToKeep) {
        List<double[]> ret = new ArrayList<>();
        for (int j = 0; j < numValsToKeep; j++) {
            ret.add(new double[] { interval[0][j] });
        }
        return ret;
    }

    // two segments of samples, one segment has 3 samples, while another one has only 1
    public void testTwoSegmentsWithSingleSample() throws InterruptedException, IOException {
        Queue<double[]> samples = MLUtil.createQueueSamples(1);
        double[] savedSample = samples.peek();
        EntityModel model = new EntityModel(entity, samples, null);
        modelState = new ModelState<>(model, modelId, detectorId, ModelType.ENTITY.getName(), clock, priority);

        doAnswer(invocation -> {
            ActionListener<Optional<Long>> listener = invocation.getArgument(2);
            listener.onResponse(Optional.of(1602269260000L));
            return null;
        }).when(searchFeatureDao).getEntityMinDataTime(any(), any(), any());

        List<Optional<double[]>> coldStartSamples = new ArrayList<>();
        double[] sample1 = new double[] { 57.0 };
        double[] sample2 = new double[] { 1.0 };
        double[] sample3 = new double[] { -19.0 };
        double[] sample5 = new double[] { -17.0 };
        coldStartSamples.add(Optional.of(sample1));
        coldStartSamples.add(Optional.of(sample2));
        coldStartSamples.add(Optional.of(sample3));
        coldStartSamples.add(Optional.empty());
        coldStartSamples.add(Optional.of(sample5));
        doAnswer(invocation -> {
            ActionListener<List<Optional<double[]>>> listener = invocation.getArgument(4);
            listener.onResponse(coldStartSamples);
            return null;
        }).when(searchFeatureDao).getColdStartSamplesForPeriods(any(), any(), any(), anyBoolean(), any());

        entityColdStarter.trainModel(entity, detectorId, modelState, listener);
        checkSemaphoreRelease();
        assertTrue(model.getTrcf().isPresent());

        // 1 round: stride * (samples - 1) + 1 = 60 * 4 + 1 = 241
        // if 241 < shingle size + numMinSamples, then another round is performed
        assertEquals(241, modelState.getModel().getTrcf().get().getForest().getTotalUpdates());
        checkSemaphoreRelease();

        List<double[]> expectedColdStartData = new ArrayList<>();

        // for function interpolate:
        // 1st parameter is a matrix of size numFeatures * numSamples
        // 2nd parameter is the number of interpolants including two samples
        double[][] interval1 = interpolator.interpolate(new double[][] { new double[] { sample1[0], sample2[0] } }, 61);
        expectedColdStartData.addAll(convertToFeatures(interval1, 60));
        double[][] interval2 = interpolator.interpolate(new double[][] { new double[] { sample2[0], sample3[0] } }, 61);
        expectedColdStartData.addAll(convertToFeatures(interval2, 60));
        double[][] interval3 = interpolator.interpolate(new double[][] { new double[] { sample3[0], sample5[0] } }, 121);
        expectedColdStartData.addAll(convertToFeatures(interval3, 121));
        assertTrue("size: " + model.getSamples().size(), model.getSamples().isEmpty());
        assertEquals(241, expectedColdStartData.size());
        diffTesting(modelState, expectedColdStartData);
    }

    // two segments of samples, one segment has 3 samples, while another one 2 samples
    /*public void testTwoSegments() throws InterruptedException, IOException {
        Queue<double[]> samples = MLUtil.createQueueSamples(1);
        double[] savedSample = samples.peek();
        EntityModel model = new EntityModel(entity, samples, null);
        modelState = new ModelState<>(model, modelId, detectorId, ModelType.ENTITY.getName(), clock, priority);
    
        doAnswer(invocation -> {
            ActionListener<Optional<Long>> listener = invocation.getArgument(2);
            listener.onResponse(Optional.of(1602269260000L));
            return null;
        }).when(searchFeatureDao).getEntityMinDataTime(any(), any(), any());
    
        List<Optional<double[]>> coldStartSamples = new ArrayList<>();
        double[] sample1 = new double[] { 57.0 };
        double[] sample2 = new double[] { 1.0 };
        double[] sample3 = new double[] { -19.0 };
        double[] sample5 = new double[] { -17.0 };
        double[] sample6 = new double[] { -38.0 };
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
        checkSemaphoreRelease();
    
        assertTrue(model.getTrcf().isPresent());
        ThresholdedRandomCutForest ercf = model.getTrcf().get();
        // 1 rounds: stride * (samples - 1) + 1 = 60 * 5 + 1 = 301
        assertEquals(301, ercf.getForest().getTotalUpdates());
        checkSemaphoreRelease();
    
        List<double[]> expectedColdStartData = new ArrayList<>();
    
        // for function interpolate:
        // 1st parameter is a matrix of size numFeatures * numSamples
        // 2nd parameter is the number of interpolants including two samples
        double[][] interval1 = interpolator.interpolate(new double[][] { new double[] { sample1[0], sample2[0] } }, 61);
        expectedColdStartData.addAll(convertToFeatures(interval1, 60));
        double[][] interval2 = interpolator.interpolate(new double[][] { new double[] { sample2[0], sample3[0] } }, 61);
        expectedColdStartData.addAll(convertToFeatures(interval2, 60));
        double[][] interval3 = interpolator.interpolate(new double[][] { new double[] { sample3[0], sample5[0] } }, 121);
        expectedColdStartData.addAll(convertToFeatures(interval3, 120));
        double[][] interval4 = interpolator.interpolate(new double[][] { new double[] { sample5[0], sample6[0] } }, 61);
        expectedColdStartData.addAll(convertToFeatures(interval4, 61));
        assertEquals(301, expectedColdStartData.size());
        assertTrue("size: " + model.getSamples().size(), model.getSamples().isEmpty());
        diffTesting(modelState, expectedColdStartData);
    }*/

    public void testThrottledColdStart() throws InterruptedException {
        Queue<double[]> samples = MLUtil.createQueueSamples(1);
        EntityModel model = new EntityModel(entity, samples, null);
        modelState = new ModelState<>(model, modelId, detectorId, ModelType.ENTITY.getName(), clock, priority);

        doAnswer(invocation -> {
            ActionListener<Optional<Long>> listener = invocation.getArgument(2);
            listener.onFailure(new OpenSearchRejectedExecutionException(""));
            return null;
        }).when(searchFeatureDao).getEntityMinDataTime(any(), any(), any());

        entityColdStarter.trainModel(entity, detectorId, modelState, listener);

        entityColdStarter.trainModel(entity, "456", modelState, listener);

        // only the first one makes the call
        verify(searchFeatureDao, times(1)).getEntityMinDataTime(any(), any(), any());
        checkSemaphoreRelease();
    }

    public void testColdStartException() throws InterruptedException {
        Queue<double[]> samples = MLUtil.createQueueSamples(1);
        EntityModel model = new EntityModel(entity, samples, null);
        modelState = new ModelState<>(model, modelId, detectorId, ModelType.ENTITY.getName(), clock, priority);

        doAnswer(invocation -> {
            ActionListener<Optional<Long>> listener = invocation.getArgument(2);
            listener.onFailure(new AnomalyDetectionException(detectorId, ""));
            return null;
        }).when(searchFeatureDao).getEntityMinDataTime(any(), any(), any());

        entityColdStarter.trainModel(entity, detectorId, modelState, listener);

        assertTrue(stateManager.getLastDetectionError(detectorId) != null);
        checkSemaphoreRelease();
    }

    @SuppressWarnings("unchecked")
    public void testNotEnoughSamples() throws InterruptedException, IOException {
        Queue<double[]> samples = MLUtil.createQueueSamples(1);
        EntityModel model = new EntityModel(entity, samples, null);
        modelState = new ModelState<>(model, modelId, detectorId, ModelType.ENTITY.getName(), clock, priority);

        detector = TestHelpers.AnomalyDetectorBuilder
            .newInstance()
            .setDetectionInterval(new IntervalTimeConfiguration(13, ChronoUnit.MINUTES))
            .setCategoryFields(ImmutableList.of(randomAlphaOfLength(5)))
            .build();
        doAnswer(invocation -> {
            GetRequest request = invocation.getArgument(0);
            ActionListener<GetResponse> listener = invocation.getArgument(2);

            listener.onResponse(TestHelpers.createGetResponse(detector, detectorId, AnomalyDetector.ANOMALY_DETECTORS_INDEX));
            return null;
        }).when(clientUtil).asyncRequest(any(GetRequest.class), any(), any(ActionListener.class));

        doAnswer(invocation -> {
            ActionListener<Optional<Long>> listener = invocation.getArgument(2);
            listener.onResponse(Optional.of(1602269260000L));
            return null;
        }).when(searchFeatureDao).getEntityMinDataTime(any(), any(), any());

        List<Optional<double[]>> coldStartSamples = new ArrayList<>();
        coldStartSamples.add(Optional.of(new double[] { 57.0 }));
        coldStartSamples.add(Optional.of(new double[] { 1.0 }));
        doAnswer(invocation -> {
            ActionListener<List<Optional<double[]>>> listener = invocation.getArgument(4);
            listener.onResponse(coldStartSamples);
            return null;
        }).when(searchFeatureDao).getColdStartSamplesForPeriods(any(), any(), any(), anyBoolean(), any());

        entityColdStarter.trainModel(entity, detectorId, modelState, listener);
        checkSemaphoreRelease();

        assertTrue(!model.getTrcf().isPresent());
        // 1st round we add 57 and 1.
        // 2nd round we add 57 and 1.
        Queue<double[]> currentSamples = model.getSamples();
        assertEquals("real sample size is " + currentSamples.size(), 4, currentSamples.size());
        int j = 0;
        while (!currentSamples.isEmpty()) {
            double[] element = currentSamples.poll();
            assertEquals(1, element.length);
            if (j == 0 || j == 2) {
                assertEquals(57, element[0], 1e-10);
            } else {
                assertEquals(1, element[0], 1e-10);
            }
            j++;
        }
    }

    @SuppressWarnings("unchecked")
    public void testEmptyDataRange() throws InterruptedException {
        Queue<double[]> samples = MLUtil.createQueueSamples(1);
        EntityModel model = new EntityModel(entity, samples, null);
        modelState = new ModelState<>(model, modelId, detectorId, ModelType.ENTITY.getName(), clock, priority);

        // the min-max range 894056973000L~894057860000L is too small and thus no data range can be found
        when(clock.millis()).thenReturn(894057860000L);

        doAnswer(invocation -> {
            GetRequest request = invocation.getArgument(0);
            ActionListener<GetResponse> listener = invocation.getArgument(2);

            listener.onResponse(TestHelpers.createGetResponse(detector, detector.getDetectorId(), AnomalyDetector.ANOMALY_DETECTORS_INDEX));
            return null;
        }).when(clientUtil).asyncRequest(any(GetRequest.class), any(), any(ActionListener.class));

        doAnswer(invocation -> {
            ActionListener<Optional<Long>> listener = invocation.getArgument(2);
            listener.onResponse(Optional.of(894056973000L));
            return null;
        }).when(searchFeatureDao).getEntityMinDataTime(any(), any(), any());

        entityColdStarter.trainModel(entity, detectorId, modelState, listener);
        checkSemaphoreRelease();

        assertTrue(!model.getTrcf().isPresent());
        // the min-max range is too small and thus no data range can be found
        assertEquals("real sample size is " + model.getSamples().size(), 1, model.getSamples().size());
    }

    public void testTrainModelFromExistingSamplesEnoughSamples() {
        int inputDimension = 2;
        int dimensions = inputDimension * detector.getShingleSize();

        ThresholdedRandomCutForest.Builder<?> rcfConfig = ThresholdedRandomCutForest
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
            .initialAcceptFraction(0.125d)
            .parallelExecutionEnabled(false)
            .sampleSize(AnomalyDetectorSettings.NUM_SAMPLES_PER_TREE)
            .internalShinglingEnabled(true)
            .anomalyRate(1 - AnomalyDetectorSettings.THRESHOLD_MIN_PVALUE);
        Tuple<Queue<double[]>, ThresholdedRandomCutForest> models = MLUtil.prepareModel(inputDimension, rcfConfig);
        Queue<double[]> samples = models.v1();
        ThresholdedRandomCutForest rcf = models.v2();

        EntityModel model = new EntityModel(entity, samples, null);
        modelState = new ModelState<>(model, modelId, detectorId, ModelType.ENTITY.getName(), clock, priority);

        Random r = new Random();

        // make sure we trained the expected models
        for (int i = 0; i < 100; i++) {
            double[] point = r.ints(inputDimension, 0, 50).asDoubleStream().toArray();
            AnomalyDescriptor descriptor = rcf.process(point, 0);
            ThresholdingResult result = modelManager
                .getAnomalyResultForEntity(point, modelState, modelId, entity, detector.getShingleSize());
            assertEquals(descriptor.getRCFScore(), result.getRcfScore(), 1e-10);
            assertEquals(descriptor.getAnomalyGrade(), result.getGrade(), 1e-10);
        }
    }

    public void testTrainModelFromExistingSamplesNotEnoughSamples() {
        Queue<double[]> samples = new ArrayDeque<>();
        EntityModel model = new EntityModel(entity, samples, null);
        modelState = new ModelState<>(model, modelId, detectorId, ModelType.ENTITY.getName(), clock, priority);
        entityColdStarter.trainModelFromExistingSamples(modelState, detector.getShingleSize());
        assertTrue(!modelState.getModel().getTrcf().isPresent());
    }

    @SuppressWarnings("unchecked")
    private void accuracyTemplate(int detectorIntervalMins, float precisionThreshold, float recallThreshold) throws Exception {
        int baseDimension = 2;
        int dataSize = 20 * AnomalyDetectorSettings.NUM_SAMPLES_PER_TREE;
        int trainTestSplit = 300;
        // detector interval
        int interval = detectorIntervalMins;
        int delta = 60000 * interval;

        int numberOfTrials = 20;
        double prec = 0;
        double recall = 0;
        for (int z = 0; z < numberOfTrials; z++) {
            // set up detector
            detector = TestHelpers.AnomalyDetectorBuilder
                .newInstance()
                .setDetectionInterval(new IntervalTimeConfiguration(interval, ChronoUnit.MINUTES))
                .setCategoryFields(ImmutableList.of(randomAlphaOfLength(5)))
                .setShingleSize(AnomalyDetectorSettings.DEFAULT_SHINGLE_SIZE)
                .build();

            long seed = new Random().nextLong();
            LOG.info("seed = " + seed);
            // create labelled data
            MultiDimDataWithTime dataWithKeys = LabelledAnomalyGenerator
                .getMultiDimData(dataSize + detector.getShingleSize() - 1, 50, 100, 5, seed, baseDimension, false, trainTestSplit, delta);
            long[] timestamps = dataWithKeys.timestampsMs;
            double[][] data = dataWithKeys.data;
            when(clock.millis()).thenReturn(timestamps[trainTestSplit - 1]);

            // training data ranges from timestamps[0] ~ timestamps[trainTestSplit-1]
            doAnswer(invocation -> {
                GetRequest request = invocation.getArgument(0);
                ActionListener<GetResponse> listener = invocation.getArgument(2);

                listener
                    .onResponse(TestHelpers.createGetResponse(detector, detector.getDetectorId(), AnomalyDetector.ANOMALY_DETECTORS_INDEX));
                return null;
            }).when(clientUtil).asyncRequest(any(GetRequest.class), any(), any(ActionListener.class));

            doAnswer(invocation -> {
                ActionListener<Optional<Long>> listener = invocation.getArgument(2);
                listener.onResponse(Optional.of(timestamps[0]));
                return null;
            }).when(searchFeatureDao).getEntityMinDataTime(any(), any(), any());

            doAnswer(invocation -> {
                List<Entry<Long, Long>> ranges = invocation.getArgument(1);
                List<Optional<double[]>> coldStartSamples = new ArrayList<>();

                Collections.sort(ranges, new Comparator<Entry<Long, Long>>() {
                    @Override
                    public int compare(Entry<Long, Long> p1, Entry<Long, Long> p2) {
                        return Long.compare(p1.getKey(), p2.getKey());
                    }
                });
                for (int j = 0; j < ranges.size(); j++) {
                    Entry<Long, Long> range = ranges.get(j);
                    Long start = range.getKey();
                    int valueIndex = searchInsert(timestamps, start);
                    coldStartSamples.add(Optional.of(data[valueIndex]));
                }

                ActionListener<List<Optional<double[]>>> listener = invocation.getArgument(4);
                listener.onResponse(coldStartSamples);
                return null;
            }).when(searchFeatureDao).getColdStartSamplesForPeriods(any(), any(), any(), anyBoolean(), any());

            EntityModel model = new EntityModel(entity, new ArrayDeque<>(), null);
            modelState = new ModelState<>(model, modelId, detector.getDetectorId(), ModelType.ENTITY.getName(), clock, priority);

            released = new AtomicBoolean();

            inProgressLatch = new CountDownLatch(1);
            listener = ActionListener.wrap(() -> {
                released.set(true);
                inProgressLatch.countDown();
            });

            entityColdStarter.trainModel(entity, detector.getDetectorId(), modelState, listener);

            checkSemaphoreRelease();
            assertTrue(model.getTrcf().isPresent());

            int tp = 0;
            int fp = 0;
            int fn = 0;
            long[] changeTimestamps = dataWithKeys.changeTimeStampsMs;

            for (int j = trainTestSplit; j < data.length; j++) {
                ThresholdingResult result = modelManager
                    .getAnomalyResultForEntity(data[j], modelState, modelId, entity, detector.getShingleSize());
                if (result.getGrade() > 0) {
                    if (changeTimestamps[j] == 0) {
                        fp++;
                    } else {
                        tp++;
                    }
                } else {
                    if (changeTimestamps[j] != 0) {
                        fn++;
                    }
                    // else ok
                }
            }

            if (tp + fp == 0) {
                prec = 1;
            } else {
                prec = tp * 1.0 / (tp + fp);
            }

            if (tp + fn == 0) {
                recall = 1;
            } else {
                recall = tp * 1.0 / (tp + fn);
            }

            // there are randomness involved; keep trying for a limited times
            if (prec >= precisionThreshold && recall >= recallThreshold) {
                break;
            }
        }

        assertTrue("precision is " + prec, prec >= precisionThreshold);
        assertTrue("recall is " + recall, recall >= recallThreshold);
        LOG.info("Interval {}, Precision: {}, recall: {}", detectorIntervalMins, prec, recall);
    }

    public int searchInsert(long[] timestamps, long target) {
        int pivot, left = 0, right = timestamps.length - 1;
        while (left <= right) {
            pivot = left + (right - left) / 2;
            if (timestamps[pivot] == target)
                return pivot;
            if (target < timestamps[pivot])
                right = pivot - 1;
            else
                left = pivot + 1;
        }
        return left;
    }

    public void testAccuracyTenMinuteInterval() throws Exception {
        accuracyTemplate(10, 0.5f, 0.5f);
    }

    public void testAccuracyThirteenMinuteInterval() throws Exception {
        accuracyTemplate(13, 0.5f, 0.5f);
    }

    /*public void testAccuracyOneMinuteIntervalNoInterpolation() throws Exception {
        EnabledSetting.getInstance().setSettingValue(EnabledSetting.INTERPOLATION_IN_HCAD_COLD_START_ENABLED, false);
        // for one minute interval, we need to disable interpolation to achieve good results
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
            featureManager,
            settings,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            checkpointWriteQueue,
            rcfSeed,
            AnomalyDetectorSettings.MAX_COLD_START_ROUNDS
        );
    
        modelManager = new ModelManager(
            mock(CheckpointDao.class),
            mock(Clock.class),
            AnomalyDetectorSettings.NUM_TREES,
            AnomalyDetectorSettings.NUM_SAMPLES_PER_TREE,
            AnomalyDetectorSettings.TIME_DECAY,
            AnomalyDetectorSettings.NUM_MIN_SAMPLES,
            AnomalyDetectorSettings.THRESHOLD_MIN_PVALUE,
            AnomalyDetectorSettings.MIN_PREVIEW_SIZE,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            entityColdStarter,
            mock(FeatureManager.class),
            mock(MemoryTracker.class)
        );
    
        accuracyTemplate(1, 0.6f, 0.6f);
    }*/

    private ModelState<EntityModel> createStateForCacheRelease() {
        inProgressLatch = new CountDownLatch(1);
        releaseSemaphore = () -> {
            released.set(true);
            inProgressLatch.countDown();
        };
        listener = ActionListener.wrap(releaseSemaphore);
        Queue<double[]> samples = MLUtil.createQueueSamples(1);
        EntityModel model = new EntityModel(entity, samples, null);
        return new ModelState<>(model, modelId, detectorId, ModelType.ENTITY.getName(), clock, priority);
    }

    /*public void testCacheReleaseAfterMaintenance() throws IOException, InterruptedException {
        ModelState<EntityModel> modelState = createStateForCacheRelease();
        doAnswer(invocation -> {
            ActionListener<Optional<Long>> listener = invocation.getArgument(2);
            listener.onResponse(Optional.of(1602269260000L));
            return null;
        }).when(searchFeatureDao).getEntityMinDataTime(any(), any(), any());
    
        List<Optional<double[]>> coldStartSamples = new ArrayList<>();
    
        double[] sample1 = new double[] { 57.0 };
        double[] sample2 = new double[] { 1.0 };
        double[] sample3 = new double[] { -19.0 };
    
        coldStartSamples.add(Optional.of(sample1));
        coldStartSamples.add(Optional.of(sample2));
        coldStartSamples.add(Optional.of(sample3));
        doAnswer(invocation -> {
            ActionListener<List<Optional<double[]>>> listener = invocation.getArgument(4);
            listener.onResponse(coldStartSamples);
            return null;
        }).when(searchFeatureDao).getColdStartSamplesForPeriods(any(), any(), any(), anyBoolean(), any());
    
        entityColdStarter.trainModel(entity, detectorId, modelState, listener);
        checkSemaphoreRelease();
        assertTrue(modelState.getModel().getTrcf().isPresent());
    
        modelState = createStateForCacheRelease();
        entityColdStarter.trainModel(entity, detectorId, modelState, listener);
        checkSemaphoreRelease();
        // model is not trained as the door keeper remembers it and won't retry training
        assertTrue(!modelState.getModel().getTrcf().isPresent());
    
        // make sure when the next maintenance coming, current door keeper gets reset
        // note our detector interval is 1 minute and the door keeper will expire in 60 intervals, which are 60 minutes
        when(clock.instant()).thenReturn(Instant.now().plus(AnomalyDetectorSettings.DOOR_KEEPER_MAINTENANCE_FREQ + 1, ChronoUnit.MINUTES));
        entityColdStarter.maintenance();
    
        modelState = createStateForCacheRelease();
        entityColdStarter.trainModel(entity, detectorId, modelState, listener);
        checkSemaphoreRelease();
        // model is trained as the door keeper gets reset
        assertTrue(modelState.getModel().getTrcf().isPresent());
    }*/

    public void testCacheReleaseAfterClear() throws IOException, InterruptedException {
        ModelState<EntityModel> modelState = createStateForCacheRelease();
        doAnswer(invocation -> {
            ActionListener<Optional<Long>> listener = invocation.getArgument(2);
            listener.onResponse(Optional.of(1602269260000L));
            return null;
        }).when(searchFeatureDao).getEntityMinDataTime(any(), any(), any());

        List<Optional<double[]>> coldStartSamples = new ArrayList<>();

        double[] sample1 = new double[] { 57.0 };
        double[] sample2 = new double[] { 1.0 };
        double[] sample3 = new double[] { -19.0 };

        coldStartSamples.add(Optional.of(sample1));
        coldStartSamples.add(Optional.of(sample2));
        coldStartSamples.add(Optional.of(sample3));
        doAnswer(invocation -> {
            ActionListener<List<Optional<double[]>>> listener = invocation.getArgument(4);
            listener.onResponse(coldStartSamples);
            return null;
        }).when(searchFeatureDao).getColdStartSamplesForPeriods(any(), any(), any(), anyBoolean(), any());

        entityColdStarter.trainModel(entity, detectorId, modelState, listener);
        checkSemaphoreRelease();
        assertTrue(modelState.getModel().getTrcf().isPresent());

        entityColdStarter.clear(detectorId);

        modelState = createStateForCacheRelease();
        entityColdStarter.trainModel(entity, detectorId, modelState, listener);
        checkSemaphoreRelease();
        // model is trained as the door keeper is regenerated after clearance
        assertTrue(modelState.getModel().getTrcf().isPresent());
    }
}
