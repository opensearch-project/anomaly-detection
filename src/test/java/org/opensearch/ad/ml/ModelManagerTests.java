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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.ActionListener;
import org.opensearch.ad.AnomalyDetectorPlugin;
import org.opensearch.ad.MemoryTracker;
import org.opensearch.ad.NodeStateManager;
import org.opensearch.ad.breaker.ADCircuitBreakerService;
import org.opensearch.ad.caching.EntityCache;
import org.opensearch.ad.common.exception.LimitExceededException;
import org.opensearch.ad.common.exception.ResourceNotFoundException;
import org.opensearch.ad.dataprocessor.IntegerSensitiveSingleFeatureLinearUniformInterpolator;
import org.opensearch.ad.dataprocessor.LinearUniformInterpolator;
import org.opensearch.ad.dataprocessor.SingleFeatureLinearUniformInterpolator;
import org.opensearch.ad.feature.FeatureManager;
import org.opensearch.ad.feature.SearchFeatureDao;
import org.opensearch.ad.ml.ModelManager.ModelType;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.Entity;
import org.opensearch.ad.ratelimit.CheckpointWriteWorker;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.util.DiscoveryNodeFilterer;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.collect.ImmutableOpenMap;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.monitor.jvm.JvmService;
import org.opensearch.threadpool.ThreadPool;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;

import test.org.opensearch.ad.util.MLUtil;
import test.org.opensearch.ad.util.RandomModelStateConfig;

import com.amazon.randomcutforest.RandomCutForest;
import com.amazon.randomcutforest.parkservices.AnomalyDescriptor;
import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;
import com.amazon.randomcutforest.returntypes.DiVector;
import com.google.common.collect.Sets;

@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(JUnitParamsRunner.class)
@SuppressWarnings("unchecked")
public class ModelManagerTests {

    private ModelManager modelManager;

    @Mock
    private AnomalyDetector anomalyDetector;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private DiscoveryNodeFilterer nodeFilter;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private JvmService jvmService;

    @Mock
    private CheckpointDao checkpointDao;

    @Mock
    private Clock clock;

    @Mock
    private FeatureManager featureManager;

    @Mock
    private EntityColdStarter entityColdStarter;

    @Mock
    private EntityCache cache;

    @Mock
    private ModelState<EntityModel> modelState;

    @Mock
    private EntityModel entityModel;

    @Mock
    private ThresholdedRandomCutForest trcf;

    private double modelDesiredSizePercentage;
    private double modelMaxSizePercentage;
    private int numTrees;
    private int numSamples;
    private int numFeatures;
    private double rcfTimeDecay;
    private int numMinSamples;
    private double thresholdMinPvalue;
    private int minPreviewSize;
    private Duration modelTtl;
    private Duration checkpointInterval;
    private ThresholdedRandomCutForest rcf;

    @Mock
    private HybridThresholdingModel hybridThresholdingModel;

    @Mock
    private ThreadPool threadPool;

    private String detectorId;
    private String rcfModelId;
    private String thresholdModelId;
    private int shingleSize;
    private Settings settings;
    private ClusterService clusterService;
    private double[] attribution;
    private double[] point;
    private DiVector attributionVec;

    @Mock
    private ActionListener<RcfResult> rcfResultListener;

    @Mock
    private ActionListener<ThresholdingResult> thresholdResultListener;
    private MemoryTracker memoryTracker;
    private Instant now;

    @Mock
    private ADCircuitBreakerService adCircuitBreakerService;

    private String modelId = "modelId";

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        modelDesiredSizePercentage = 0.001;
        modelMaxSizePercentage = 0.1;
        numTrees = 100;
        numSamples = 10;
        numFeatures = 1;
        rcfTimeDecay = 1.0 / 1024;
        numMinSamples = 1;
        thresholdMinPvalue = 0.95;
        minPreviewSize = 500;
        modelTtl = Duration.ofHours(1);
        checkpointInterval = Duration.ofHours(1);
        shingleSize = 1;
        attribution = new double[] { 1, 1 };
        attributionVec = new DiVector(attribution.length);
        for (int i = 0; i < attribution.length; i++) {
            attributionVec.high[i] = attribution[i];
            attributionVec.low[i] = attribution[i] - 1;
        }
        point = new double[] { 2 };

        rcf = spy(ThresholdedRandomCutForest.builder().dimensions(numFeatures).sampleSize(numSamples).numberOfTrees(numTrees).build());
        double score = 11.;

        double confidence = 0.091353632;
        double grade = 0.1;
        AnomalyDescriptor descriptor = new AnomalyDescriptor();
        descriptor.setRcfScore(score);
        descriptor.setForestSize(numTrees);
        descriptor.setDataConfidence(confidence);
        descriptor.setAnomalyGrade(grade);
        descriptor.setAttribution(attributionVec);
        descriptor.setTotalUpdates(numSamples);
        when(rcf.process(any(), anyLong())).thenReturn(descriptor);

        ExecutorService executorService = mock(ExecutorService.class);
        when(threadPool.executor(AnomalyDetectorPlugin.AD_THREAD_POOL_NAME)).thenReturn(executorService);
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(executorService).execute(any(Runnable.class));

        now = Instant.now();
        when(clock.instant()).thenReturn(now);

        memoryTracker = mock(MemoryTracker.class);
        when(memoryTracker.isHostingAllowed(anyString(), any())).thenReturn(true);

        modelManager = spy(
            new ModelManager(
                checkpointDao,
                clock,
                numTrees,
                numSamples,
                rcfTimeDecay,
                numMinSamples,
                thresholdMinPvalue,
                minPreviewSize,
                modelTtl,
                checkpointInterval,
                entityColdStarter,
                featureManager,
                memoryTracker
            )
        );

        detectorId = "detectorId";
        rcfModelId = "detectorId_model_rcf_1";
        thresholdModelId = "detectorId_model_threshold";

        when(this.modelState.getModel()).thenReturn(this.entityModel);
        when(this.entityModel.getTrcf()).thenReturn(Optional.of(this.trcf));
        settings = Settings.builder().put("plugins.anomaly_detection.model_max_size_percent", modelMaxSizePercentage).build();

        when(anomalyDetector.getShingleSize()).thenReturn(shingleSize);
    }

    private Object[] getDetectorIdForModelIdData() {
        return new Object[] {
            new Object[] { "testId_model_threshold", "testId" },
            new Object[] { "test_id_model_threshold", "test_id" },
            new Object[] { "test_model_id_model_threshold", "test_model_id" },
            new Object[] { "testId_model_rcf_1", "testId" },
            new Object[] { "test_Id_model_rcf_1", "test_Id" },
            new Object[] { "test_model_rcf_Id_model_rcf_1", "test_model_rcf_Id" }, };
    };

    @Test
    @Parameters(method = "getDetectorIdForModelIdData")
    public void getDetectorIdForModelId_returnExpectedId(String modelId, String expectedDetectorId) {
        assertEquals(expectedDetectorId, SingleStreamModelIdMapper.getDetectorIdForModelId(modelId));
    }

    private Object[] getDetectorIdForModelIdIllegalArgument() {
        return new Object[] { new Object[] { "testId" }, new Object[] { "testid_" }, new Object[] { "_testId" }, };
    }

    @Test(expected = IllegalArgumentException.class)
    @Parameters(method = "getDetectorIdForModelIdIllegalArgument")
    public void getDetectorIdForModelId_throwIllegalArgument_forInvalidId(String modelId) {
        SingleStreamModelIdMapper.getDetectorIdForModelId(modelId);
    }

    private ImmutableOpenMap<String, DiscoveryNode> createDataNodes(int numDataNodes) {
        ImmutableOpenMap.Builder<String, DiscoveryNode> dataNodes = ImmutableOpenMap.builder();
        for (int i = 0; i < numDataNodes; i++) {
            dataNodes.put("foo" + i, mock(DiscoveryNode.class));
        }
        return dataNodes.build();
    }

    private Object[] getPartitionedForestSizesData() {
        ThresholdedRandomCutForest rcf = ThresholdedRandomCutForest.builder().dimensions(1).sampleSize(10).numberOfTrees(100).build();
        return new Object[] {
            // one partition given sufficient large nodes
            new Object[] { rcf, 100L, 100_000L, createDataNodes(10), pair(1, 100) },
            // two paritions given sufficient medium nodes
            new Object[] { rcf, 100L, 50_000L, createDataNodes(10), pair(2, 50) },
            // ten partitions given sufficent small nodes
            new Object[] { rcf, 100L, 10_000L, createDataNodes(10), pair(10, 10) },
            // five double-sized paritions given fewer small nodes
            new Object[] { rcf, 100L, 10_000L, createDataNodes(5), pair(5, 20) },
            // one large-sized partition given one small node
            new Object[] { rcf, 100L, 1_000L, createDataNodes(1), pair(1, 100) } };
    }

    private Object[] estimateModelSizeData() {
        return new Object[] {
            new Object[] { ThresholdedRandomCutForest.builder().dimensions(1).sampleSize(256).numberOfTrees(100).build(), 819200L },
            new Object[] { ThresholdedRandomCutForest.builder().dimensions(5).sampleSize(256).numberOfTrees(100).build(), 4096000L } };
    }

    @Parameters(method = "estimateModelSizeData")
    public void estimateModelSize_returnExpected(ThresholdedRandomCutForest rcf, long expectedSize) {
        assertEquals(expectedSize, memoryTracker.estimateTRCFModelSize(rcf));
    }

    @Test
    public void getRcfResult_returnExpectedToListener() {
        double[] point = new double[0];
        ThresholdedRandomCutForest rForest = mock(ThresholdedRandomCutForest.class);
        RandomCutForest rcf = mock(RandomCutForest.class);
        when(rForest.getForest()).thenReturn(rcf);
        when(rcf.getDimensions()).thenReturn(8);
        double score = 11.;

        doAnswer(invocation -> {
            ActionListener<Optional<ThresholdedRandomCutForest>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(rForest));
            return null;
        }).when(checkpointDao).getTRCFModel(eq(rcfModelId), any(ActionListener.class));

        double confidence = 0.091353632;
        double grade = 0.1;
        AnomalyDescriptor descriptor = new AnomalyDescriptor();
        descriptor.setRcfScore(score);
        descriptor.setForestSize(numTrees);
        descriptor.setDataConfidence(confidence);
        descriptor.setAnomalyGrade(grade);
        descriptor.setAttribution(attributionVec);
        descriptor.setTotalUpdates(numSamples);
        when(rForest.process(any(), anyLong())).thenReturn(descriptor);

        ActionListener<RcfResult> listener = mock(ActionListener.class);
        modelManager.getRcfResult(detectorId, rcfModelId, point, listener);

        RcfResult expected = new RcfResult(score, confidence, numTrees, new double[] { 0.5, 0.5 }, numSamples, grade);
        verify(listener).onResponse(eq(expected));

        descriptor.setTotalUpdates(numSamples + 1L);
        listener = mock(ActionListener.class);
        modelManager.getRcfResult(detectorId, rcfModelId, point, listener);

        ArgumentCaptor<RcfResult> responseCaptor = ArgumentCaptor.forClass(RcfResult.class);
        verify(listener).onResponse(responseCaptor.capture());
        assertEquals(0.091353632, responseCaptor.getValue().getConfidence(), 1e-6);
    }

    @Test
    public void getRcfResult_throwToListener_whenNoCheckpoint() {
        doAnswer(invocation -> {
            ActionListener<Optional<RandomCutForest>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.empty());
            return null;
        }).when(checkpointDao).getTRCFModel(eq(rcfModelId), any(ActionListener.class));

        ActionListener<RcfResult> listener = mock(ActionListener.class);
        modelManager.getRcfResult(detectorId, rcfModelId, new double[0], listener);

        verify(listener).onFailure(any(ResourceNotFoundException.class));
    }

    @Test
    public void getRcfResult_throwToListener_whenHeapLimitExceed() {
        doAnswer(invocation -> {
            ActionListener<Optional<ThresholdedRandomCutForest>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(rcf));
            return null;
        }).when(checkpointDao).getTRCFModel(eq(rcfModelId), any(ActionListener.class));

        when(jvmService.info().getMem().getHeapMax().getBytes()).thenReturn(1_000L);
        final Set<Setting<?>> settingsSet = Stream
            .concat(
                ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(),
                Sets.newHashSet(AnomalyDetectorSettings.MODEL_MAX_SIZE_PERCENTAGE).stream()
            )
            .collect(Collectors.toSet());
        ClusterSettings clusterSettings = new ClusterSettings(settings, settingsSet);
        clusterService = new ClusterService(settings, clusterSettings, null);

        MemoryTracker memoryTracker = new MemoryTracker(
            jvmService,
            modelMaxSizePercentage,
            modelDesiredSizePercentage,
            clusterService,
            adCircuitBreakerService
        );

        ActionListener<RcfResult> listener = mock(ActionListener.class);

        // use new memoryTracker
        modelManager = spy(
            new ModelManager(
                checkpointDao,
                clock,
                numTrees,
                numSamples,
                rcfTimeDecay,
                numMinSamples,
                thresholdMinPvalue,
                minPreviewSize,
                modelTtl,
                checkpointInterval,
                entityColdStarter,
                featureManager,
                memoryTracker
            )
        );

        modelManager.getRcfResult(detectorId, rcfModelId, new double[0], listener);

        verify(listener).onFailure(any(LimitExceededException.class));
    }

    @Test
    public void getThresholdingResult_returnExpectedToListener() {
        double score = 1.;
        double grade = 0.;
        double confidence = 0.5;

        doAnswer(invocation -> {
            ActionListener<Optional<ThresholdingModel>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(hybridThresholdingModel));
            return null;
        }).when(checkpointDao).getThresholdModel(eq(thresholdModelId), any(ActionListener.class));
        when(hybridThresholdingModel.grade(score)).thenReturn(grade);
        when(hybridThresholdingModel.confidence()).thenReturn(confidence);

        ActionListener<ThresholdingResult> listener = mock(ActionListener.class);
        modelManager.getThresholdingResult(detectorId, thresholdModelId, score, listener);

        ThresholdingResult expected = new ThresholdingResult(grade, confidence, score);
        verify(listener).onResponse(eq(expected));

        listener = mock(ActionListener.class);
        modelManager.getThresholdingResult(detectorId, thresholdModelId, score, listener);
        verify(listener).onResponse(eq(expected));
    }

    @Test
    public void getThresholdingResult_throwToListener_withNoCheckpoint() {
        doAnswer(invocation -> {
            ActionListener<Optional<ThresholdingModel>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.empty());
            return null;
        }).when(checkpointDao).getThresholdModel(eq(thresholdModelId), any(ActionListener.class));

        ActionListener<ThresholdingResult> listener = mock(ActionListener.class);
        modelManager.getThresholdingResult(detectorId, thresholdModelId, 0, listener);

        verify(listener).onFailure(any(ResourceNotFoundException.class));
    }

    @Test
    public void getThresholdingResult_notUpdate_withZeroScore() {
        double score = 0.0;

        doAnswer(invocation -> {
            ActionListener<Optional<ThresholdingModel>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(hybridThresholdingModel));
            return null;
        }).when(checkpointDao).getThresholdModel(eq(thresholdModelId), any(ActionListener.class));

        ActionListener<ThresholdingResult> listener = mock(ActionListener.class);
        modelManager.getThresholdingResult(detectorId, thresholdModelId, score, listener);

        verify(hybridThresholdingModel, never()).update(score);
    }

    @Test
    public void getAllModelIds_returnAllIds_forRcfAndThreshold() {
        doAnswer(invocation -> {
            ActionListener<Optional<ThresholdingModel>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(hybridThresholdingModel));
            return null;
        }).when(checkpointDao).getThresholdModel(eq(thresholdModelId), any(ActionListener.class));
        modelManager.getThresholdingResult(detectorId, thresholdModelId, 0, thresholdResultListener);

        assertEquals(Stream.of(thresholdModelId).collect(Collectors.toSet()), modelManager.getAllModelIds());
    }

    @Test
    public void getAllModelIds_returnEmpty_forNoModels() {
        assertEquals(Collections.emptySet(), modelManager.getAllModelIds());
    }

    @Test
    public void stopModel_returnExpectedToListener_whenRcfStop() {
        ThresholdedRandomCutForest forest = mock(ThresholdedRandomCutForest.class);

        doAnswer(invocation -> {
            ActionListener<Optional<ThresholdedRandomCutForest>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(forest));
            return null;
        }).when(checkpointDao).getTRCFModel(eq(rcfModelId), any(ActionListener.class));

        modelManager.getRcfResult(detectorId, rcfModelId, new double[0], rcfResultListener);
        when(clock.instant()).thenReturn(Instant.EPOCH);
        doAnswer(invocation -> {
            ActionListener<Void> listener = invocation.getArgument(2);
            listener.onResponse(null);
            return null;
        }).when(checkpointDao).putTRCFCheckpoint(eq(rcfModelId), eq(forest), any(ActionListener.class));

        ActionListener<Void> listener = mock(ActionListener.class);
        modelManager.stopModel(detectorId, rcfModelId, listener);

        verify(listener).onResponse(eq(null));
    }

    @Test
    public void stopModel_returnExpectedToListener_whenThresholdStop() {
        doAnswer(invocation -> {
            ActionListener<Optional<ThresholdingModel>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(hybridThresholdingModel));
            return null;
        }).when(checkpointDao).getThresholdModel(eq(thresholdModelId), any(ActionListener.class));
        modelManager.getThresholdingResult(detectorId, thresholdModelId, 0, thresholdResultListener);
        when(clock.instant()).thenReturn(Instant.EPOCH);
        doAnswer(invocation -> {
            ActionListener<Void> listener = invocation.getArgument(2);
            listener.onResponse(null);
            return null;
        }).when(checkpointDao).putThresholdCheckpoint(eq(thresholdModelId), eq(hybridThresholdingModel), any(ActionListener.class));

        ActionListener<Void> listener = mock(ActionListener.class);
        modelManager.stopModel(detectorId, thresholdModelId, listener);

        verify(listener).onResponse(eq(null));
    }

    @Test
    public void stopModel_throwToListener_whenCheckpointFail() {
        ThresholdedRandomCutForest forest = mock(ThresholdedRandomCutForest.class);
        doAnswer(invocation -> {
            ActionListener<Optional<ThresholdedRandomCutForest>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(forest));
            return null;
        }).when(checkpointDao).getTRCFModel(eq(rcfModelId), any(ActionListener.class));
        modelManager.getRcfResult(detectorId, rcfModelId, new double[0], rcfResultListener);
        when(clock.instant()).thenReturn(Instant.EPOCH);
        doAnswer(invocation -> {
            ActionListener<Void> listener = invocation.getArgument(2);
            listener.onFailure(new RuntimeException());
            return null;
        }).when(checkpointDao).putTRCFCheckpoint(eq(rcfModelId), eq(forest), any(ActionListener.class));

        ActionListener<Void> listener = mock(ActionListener.class);
        modelManager.stopModel(detectorId, rcfModelId, listener);

        verify(listener).onFailure(any(Exception.class));
    }

    @Test
    public void clear_callListener_whenRcfDeleted() {
        String otherModelId = detectorId + rcfModelId;
        RandomCutForest forest = mock(RandomCutForest.class);
        doAnswer(invocation -> {
            ActionListener<Optional<RandomCutForest>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(forest));
            return null;
        }).when(checkpointDao).getTRCFModel(eq(rcfModelId), any(ActionListener.class));
        doAnswer(invocation -> {
            ActionListener<Optional<RandomCutForest>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(forest));
            return null;
        }).when(checkpointDao).getTRCFModel(eq(otherModelId), any(ActionListener.class));
        modelManager.getRcfResult(detectorId, rcfModelId, new double[0], rcfResultListener);
        modelManager.getRcfResult(otherModelId, otherModelId, new double[0], rcfResultListener);
        doAnswer(invocation -> {
            ActionListener<Void> listener = invocation.getArgument(1);
            listener.onResponse(null);
            return null;
        }).when(checkpointDao).deleteModelCheckpoint(eq(rcfModelId), any(ActionListener.class));

        ActionListener<Void> listener = mock(ActionListener.class);
        modelManager.clear(detectorId, listener);

        verify(listener).onResponse(null);
    }

    @Test
    public void clear_callListener_whenThresholdDeleted() {
        doAnswer(invocation -> {
            ActionListener<Optional<ThresholdingModel>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(hybridThresholdingModel));
            return null;
        }).when(checkpointDao).getThresholdModel(eq(thresholdModelId), any(ActionListener.class));

        modelManager.getThresholdingResult(detectorId, thresholdModelId, 0, thresholdResultListener);
        doAnswer(invocation -> {
            ActionListener<Void> listener = invocation.getArgument(1);
            listener.onResponse(null);
            return null;
        }).when(checkpointDao).deleteModelCheckpoint(eq(thresholdModelId), any(ActionListener.class));

        ActionListener<Void> listener = mock(ActionListener.class);
        modelManager.clear(detectorId, listener);

        verify(listener).onResponse(null);
    }

    @Test
    public void clear_throwToListener_whenDeleteFail() {
        doAnswer(invocation -> {
            ActionListener<Optional<ThresholdedRandomCutForest>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(rcf));
            return null;
        }).when(checkpointDao).getTRCFModel(eq(rcfModelId), any(ActionListener.class));
        modelManager.getRcfResult(detectorId, rcfModelId, new double[0], rcfResultListener);
        doAnswer(invocation -> {
            ActionListener<Void> listener = invocation.getArgument(1);
            listener.onFailure(new RuntimeException());
            return null;
        }).when(checkpointDao).deleteModelCheckpoint(eq(rcfModelId), any(ActionListener.class));

        ActionListener<Void> listener = mock(ActionListener.class);
        modelManager.clear(detectorId, listener);

        verify(listener).onFailure(any(Exception.class));
    }

    @Test
    public void trainModel_returnExpectedToListener_putCheckpoints() {
        double[][] trainData = new Random().doubles().limit(100).mapToObj(d -> new double[] { d }).toArray(double[][]::new);
        doAnswer(invocation -> {
            ActionListener<Void> listener = invocation.getArgument(2);
            listener.onResponse(null);
            return null;
        }).when(checkpointDao).putTRCFCheckpoint(any(), any(), any(ActionListener.class));

        ActionListener<Void> listener = mock(ActionListener.class);
        modelManager.trainModel(anomalyDetector, trainData, listener);

        verify(listener).onResponse(eq(null));
        verify(checkpointDao, times(1)).putTRCFCheckpoint(any(), any(), any());
    }

    private Object[] trainModelIllegalArgumentData() {
        return new Object[] { new Object[] { new double[][] {} }, new Object[] { new double[][] { {} } } };
    }

    @Test
    @Parameters(method = "trainModelIllegalArgumentData")
    public void trainModel_throwIllegalArgumentToListener_forInvalidTrainData(double[][] trainData) {
        ActionListener<Void> listener = mock(ActionListener.class);
        modelManager.trainModel(anomalyDetector, trainData, listener);

        verify(listener).onFailure(any(IllegalArgumentException.class));
    }

    @Test
    public void trainModel_throwLimitExceededToListener_whenLimitExceed() {
        doThrow(new LimitExceededException(null, null)).when(checkpointDao).putTRCFCheckpoint(any(), any(), any());

        ActionListener<Void> listener = mock(ActionListener.class);
        modelManager.trainModel(anomalyDetector, new double[][] { { 0 } }, listener);

        verify(listener).onFailure(any(LimitExceededException.class));
    }

    @Test
    public void getRcfModelId_returnNonEmptyString() {
        String rcfModelId = SingleStreamModelIdMapper.getRcfModelId(anomalyDetector.getDetectorId(), 0);

        assertFalse(rcfModelId.isEmpty());
    }

    @Test
    public void getThresholdModelId_returnNonEmptyString() {
        String thresholdModelId = SingleStreamModelIdMapper.getThresholdModelId(anomalyDetector.getDetectorId());

        assertFalse(thresholdModelId.isEmpty());
    }

    private Entry<Integer, Integer> pair(int size, int value) {
        return new SimpleImmutableEntry<>(size, value);
    }

    @Test
    public void maintenance_returnExpectedToListener_forRcfModel() {
        String successModelId = "testSuccessModelId";
        String failModelId = "testFailModelId";
        double[] point = new double[0];
        ThresholdedRandomCutForest forest = mock(ThresholdedRandomCutForest.class);
        ThresholdedRandomCutForest failForest = mock(ThresholdedRandomCutForest.class);

        doAnswer(invocation -> {
            ActionListener<Optional<ThresholdedRandomCutForest>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(forest));
            return null;
        }).when(checkpointDao).getTRCFModel(eq(successModelId), any(ActionListener.class));
        doAnswer(invocation -> {
            ActionListener<Optional<ThresholdedRandomCutForest>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(failForest));
            return null;
        }).when(checkpointDao).getTRCFModel(eq(failModelId), any(ActionListener.class));
        doAnswer(invocation -> {
            ActionListener<Optional<String>> listener = invocation.getArgument(2);
            listener.onResponse(null);
            return null;
        }).when(checkpointDao).putTRCFCheckpoint(eq(successModelId), eq(forest), any(ActionListener.class));
        doAnswer(invocation -> {
            ActionListener<Optional<String>> listener = invocation.getArgument(2);
            listener.onFailure(new RuntimeException());
            return null;
        }).when(checkpointDao).putTRCFCheckpoint(eq(failModelId), eq(failForest), any(ActionListener.class));
        when(clock.instant()).thenReturn(Instant.EPOCH);
        ActionListener<RcfResult> scoreListener = mock(ActionListener.class);
        modelManager.getRcfResult(detectorId, successModelId, point, scoreListener);
        modelManager.getRcfResult(detectorId, failModelId, point, scoreListener);

        ActionListener<Void> listener = mock(ActionListener.class);
        modelManager.maintenance(listener);

        verify(listener).onResponse(eq(null));
        verify(checkpointDao, times(1)).putTRCFCheckpoint(eq(successModelId), eq(forest), any(ActionListener.class));
        verify(checkpointDao, times(1)).putTRCFCheckpoint(eq(failModelId), eq(failForest), any(ActionListener.class));
    }

    @Test
    public void maintenance_returnExpectedToListener_forThresholdModel() {
        String successModelId = "testSuccessModelId";
        String failModelId = "testFailModelId";
        double score = 1.;
        HybridThresholdingModel failThresholdModel = mock(HybridThresholdingModel.class);
        doAnswer(invocation -> {
            ActionListener<Optional<ThresholdingModel>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(hybridThresholdingModel));
            return null;
        }).when(checkpointDao).getThresholdModel(eq(successModelId), any(ActionListener.class));
        doAnswer(invocation -> {
            ActionListener<Optional<ThresholdingModel>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(failThresholdModel));
            return null;
        }).when(checkpointDao).getThresholdModel(eq(failModelId), any(ActionListener.class));
        doAnswer(invocation -> {
            ActionListener<Optional<Void>> listener = invocation.getArgument(2);
            listener.onResponse(null);
            return null;
        }).when(checkpointDao).putThresholdCheckpoint(eq(successModelId), eq(hybridThresholdingModel), any(ActionListener.class));
        doAnswer(invocation -> {
            ActionListener<Optional<Void>> listener = invocation.getArgument(2);
            listener.onFailure(new RuntimeException());
            return null;
        }).when(checkpointDao).putThresholdCheckpoint(eq(failModelId), eq(failThresholdModel), any(ActionListener.class));
        when(clock.instant()).thenReturn(Instant.EPOCH);
        ActionListener<ThresholdingResult> scoreListener = mock(ActionListener.class);
        modelManager.getThresholdingResult(detectorId, successModelId, score, scoreListener);
        modelManager.getThresholdingResult(detectorId, failModelId, score, scoreListener);

        ActionListener<Void> listener = mock(ActionListener.class);
        modelManager.maintenance(listener);

        verify(listener).onResponse(eq(null));
        verify(checkpointDao, times(1)).putThresholdCheckpoint(eq(successModelId), eq(hybridThresholdingModel), any(ActionListener.class));
    }

    @Test
    public void maintenance_returnExpectedToListener_stopModel() {
        double[] point = new double[0];
        ThresholdedRandomCutForest forest = mock(ThresholdedRandomCutForest.class);

        doAnswer(invocation -> {
            ActionListener<Optional<ThresholdedRandomCutForest>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(forest));
            return null;
        }).when(checkpointDao).getTRCFModel(eq(rcfModelId), any(ActionListener.class));
        doAnswer(invocation -> {
            ActionListener<Void> listener = invocation.getArgument(2);
            listener.onResponse(null);
            return null;
        }).when(checkpointDao).putTRCFCheckpoint(eq(rcfModelId), eq(forest), any(ActionListener.class));
        when(clock.instant()).thenReturn(Instant.EPOCH, Instant.EPOCH, Instant.EPOCH.plus(modelTtl.plusSeconds(1)));
        ActionListener<RcfResult> scoreListener = mock(ActionListener.class);
        modelManager.getRcfResult(detectorId, rcfModelId, point, scoreListener);

        ActionListener<Void> listener = mock(ActionListener.class);
        modelManager.maintenance(listener);
        verify(listener).onResponse(eq(null));

        modelManager.getRcfResult(detectorId, rcfModelId, point, scoreListener);
        verify(checkpointDao, times(2)).getTRCFModel(eq(rcfModelId), any(ActionListener.class));
    }

    @Test
    public void maintenance_returnExpectedToListener_doNothing() {
        double[] point = new double[0];

        doAnswer(invocation -> {
            ActionListener<Optional<ThresholdedRandomCutForest>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(rcf));
            return null;
        }).when(checkpointDao).getTRCFModel(eq(rcfModelId), any(ActionListener.class));
        doAnswer(invocation -> {
            ActionListener<Void> listener = invocation.getArgument(2);
            listener.onResponse(null);
            return null;
        }).when(checkpointDao).putTRCFCheckpoint(eq(rcfModelId), eq(rcf), any(ActionListener.class));
        when(clock.instant()).thenReturn(Instant.MIN);
        ActionListener<RcfResult> scoreListener = mock(ActionListener.class);
        modelManager.getRcfResult(detectorId, rcfModelId, point, scoreListener);
        ActionListener<Void> listener = mock(ActionListener.class);
        modelManager.maintenance(listener);
        verify(listener).onResponse(eq(null));

        listener = mock(ActionListener.class);
        modelManager.maintenance(listener);
        verify(listener).onResponse(eq(null));

        modelManager.getRcfResult(detectorId, rcfModelId, point, scoreListener);
        verify(checkpointDao, times(1)).getTRCFModel(eq(rcfModelId), any(ActionListener.class));
    }

    @Test
    public void getPreviewResults_returnNoAnomalies_forNoAnomalies() {
        int numPoints = 1000;
        double[][] points = Stream.generate(() -> new double[] { 0 }).limit(numPoints).toArray(double[][]::new);

        List<ThresholdingResult> results = modelManager.getPreviewResults(points, shingleSize);

        assertEquals(numPoints, results.size());
        assertTrue(results.stream().noneMatch(r -> r.getGrade() > 0));
    }

    @Test
    public void getPreviewResults_returnAnomalies_forLastAnomaly() {
        int numPoints = 1000;
        double[][] points = Stream.generate(() -> new double[] { 0 }).limit(numPoints).toArray(double[][]::new);
        points[points.length - 1] = new double[] { 1. };

        List<ThresholdingResult> results = modelManager.getPreviewResults(points, shingleSize);

        assertEquals(numPoints, results.size());
        assertTrue(results.stream().limit(numPoints - 1).noneMatch(r -> r.getGrade() > 0));
        assertTrue(results.get(numPoints - 1).getGrade() > 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getPreviewResults_throwIllegalArgument_forInvalidInput() {
        modelManager.getPreviewResults(new double[0][0], shingleSize);
    }

    @Test
    public void processEmptyCheckpoint() {
        ModelState<EntityModel> modelState = modelManager.processEntityCheckpoint(Optional.empty(), null, "", "", shingleSize);
        assertEquals(Instant.MIN, modelState.getLastCheckpointTime());
    }

    @Test
    public void processNonEmptyCheckpoint() {
        String modelId = "abc";
        String detectorId = "123";
        EntityModel model = MLUtil.createNonEmptyModel(modelId);
        Instant checkpointTime = Instant.ofEpochMilli(1000);
        ModelState<EntityModel> modelState = modelManager
            .processEntityCheckpoint(
                Optional.of(new SimpleImmutableEntry<>(model, checkpointTime)),
                null,
                modelId,
                detectorId,
                shingleSize
            );
        assertEquals(checkpointTime, modelState.getLastCheckpointTime());
        assertEquals(model.getSamples().size(), modelState.getModel().getSamples().size());
        assertEquals(now, modelState.getLastUsedTime());
    }

    @Test
    public void getNullState() {
        assertEquals(new ThresholdingResult(0, 0, 0), modelManager.getAnomalyResultForEntity(new double[] {}, null, "", null, shingleSize));
    }

    @Test
    public void getEmptyStateFullSamples() {
        SearchFeatureDao searchFeatureDao = mock(SearchFeatureDao.class);

        SingleFeatureLinearUniformInterpolator singleFeatureLinearUniformInterpolator =
            new IntegerSensitiveSingleFeatureLinearUniformInterpolator();
        LinearUniformInterpolator interpolator = new LinearUniformInterpolator(singleFeatureLinearUniformInterpolator);

        NodeStateManager stateManager = mock(NodeStateManager.class);
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

        CheckpointWriteWorker checkpointWriteQueue = mock(CheckpointWriteWorker.class);

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
            checkpointWriteQueue
        );

        modelManager = spy(
            new ModelManager(
                checkpointDao,
                clock,
                numTrees,
                numSamples,
                rcfTimeDecay,
                numMinSamples,
                thresholdMinPvalue,
                minPreviewSize,
                modelTtl,
                checkpointInterval,
                entityColdStarter,
                featureManager,
                memoryTracker
            )
        );

        ModelState<EntityModel> state = MLUtil
            .randomModelState(new RandomModelStateConfig.Builder().fullModel(false).sampleSize(numMinSamples).build());
        EntityModel model = state.getModel();
        assertTrue(!model.getTrcf().isPresent());
        ThresholdingResult result = modelManager.getAnomalyResultForEntity(new double[] { -1 }, state, "", null, shingleSize);
        // model outputs scores
        assertTrue(result.getRcfScore() != 0);
        // added the sample to score since our model is empty
        assertEquals(0, model.getSamples().size());
    }

    @Test
    public void getAnomalyResultForEntityNoModel() {
        ModelState<EntityModel> modelState = new ModelState<>(null, modelId, detectorId, ModelType.ENTITY.getName(), clock, 0);
        ThresholdingResult result = modelManager
            .getAnomalyResultForEntity(
                new double[] { -1 },
                modelState,
                modelId,
                Entity.createSingleAttributeEntity("field", "val"),
                shingleSize
            );
        // model outputs scores
        assertEquals(new ThresholdingResult(0, 0, 0), result);
        // added the sample to score since our model is empty
        assertEquals(1, modelState.getModel().getSamples().size());
    }

    @Test
    public void getEmptyStateNotFullSamples() {
        ModelState<EntityModel> state = MLUtil
            .randomModelState(new RandomModelStateConfig.Builder().fullModel(false).sampleSize(numMinSamples - 1).build());
        assertEquals(
            new ThresholdingResult(0, 0, 0),
            modelManager.getAnomalyResultForEntity(new double[] { -1 }, state, "", null, shingleSize)
        );
        assertEquals(numMinSamples, state.getModel().getSamples().size());
    }

    @Test
    public void scoreSamples() {
        ModelState<EntityModel> state = MLUtil.randomModelState(new RandomModelStateConfig.Builder().fullModel(true).build());
        modelManager.getAnomalyResultForEntity(new double[] { -1 }, state, "", null, shingleSize);
        assertEquals(0, state.getModel().getSamples().size());
        assertEquals(now, state.getLastUsedTime());
    }

    public void getAnomalyResultForEntity_withTrcf() {
        AnomalyDescriptor anomalyDescriptor = new AnomalyDescriptor();
        anomalyDescriptor.setRcfScore(2);
        anomalyDescriptor.setDataConfidence(1);
        anomalyDescriptor.setAnomalyGrade(1);
        when(this.trcf.process(this.point, 0)).thenReturn(anomalyDescriptor);

        ThresholdingResult result = modelManager
            .getAnomalyResultForEntity(this.point, this.modelState, this.detectorId, null, this.shingleSize);
        assertEquals(
            new ThresholdingResult(
                anomalyDescriptor.getAnomalyGrade(),
                anomalyDescriptor.getDataConfidence(),
                anomalyDescriptor.getRcfScore()
            ),
            result
        );
    }

    @Test
    public void score_with_trcf() {
        AnomalyDescriptor anomalyDescriptor = new AnomalyDescriptor();
        anomalyDescriptor.setRcfScore(2);
        anomalyDescriptor.setAnomalyGrade(1);
        when(this.trcf.process(this.point, 0)).thenReturn(anomalyDescriptor);
        when(this.entityModel.getSamples()).thenReturn(new ArrayDeque<>(Arrays.asList(this.point)));

        ThresholdingResult result = modelManager.score(this.point, this.detectorId, this.modelState);
        assertEquals(
            new ThresholdingResult(
                anomalyDescriptor.getAnomalyGrade(),
                anomalyDescriptor.getDataConfidence(),
                anomalyDescriptor.getRcfScore()
            ),
            result
        );
    }
}
