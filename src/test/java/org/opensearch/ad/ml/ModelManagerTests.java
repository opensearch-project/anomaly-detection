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

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.AbstractMap.SimpleEntry;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Random;
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
import org.opensearch.ad.breaker.ADCircuitBreakerService;
import org.opensearch.ad.caching.EntityCache;
import org.opensearch.ad.common.exception.LimitExceededException;
import org.opensearch.ad.common.exception.ResourceNotFoundException;
import org.opensearch.ad.feature.FeatureManager;
import org.opensearch.ad.ml.rcf.CombinedRcfResult;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.util.DiscoveryNodeFilterer;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.collect.ImmutableOpenMap;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.monitor.jvm.JvmService;
import org.opensearch.threadpool.ThreadPool;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;

import com.amazon.randomcutforest.RandomCutForest;
import com.amazon.randomcutforest.returntypes.DiVector;
import com.amazon.randomcutforest.serialize.RandomCutForestSerDe;
import com.google.gson.Gson;

@PowerMockIgnore("javax.management.*")
@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(JUnitParamsRunner.class)
@PrepareForTest({ Gson.class, ClusterSettings.class })
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
    private RandomCutForestSerDe rcfSerde;

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

    private Gson gson;

    private double modelDesiredSizePercentage;
    private double modelMaxSizePercentage;
    private int numTrees;
    private int numSamples;
    private int numFeatures;
    private double rcfTimeDecay;
    private int numMinSamples;
    private double thresholdMinPvalue;
    private double thresholdMaxRankError;
    private double thresholdMaxScore;
    private int thresholdNumLogNormalQuantiles;
    private int thresholdDownsamples;
    private long thresholdMaxSamples;
    private Class<? extends ThresholdingModel> thresholdingModelClass;
    private int minPreviewSize;
    private Duration modelTtl;
    private Duration checkpointInterval;
    private RandomCutForest rcf;
    private ModelPartitioner modelPartitioner;

    @Mock
    private HybridThresholdingModel hybridThresholdingModel;

    @Mock
    private ThreadPool threadPool;

    private String detectorId;
    private String modelId;
    private String rcfModelId;
    private String thresholdModelId;
    private String checkpoint;
    private int shingleSize;
    private Settings settings;
    private ClusterService clusterService;
    private String successModelId;
    private String failModelId;
    private String successCheckpoint;
    private String failCheckpoint;
    private double[] attribution;
    private double[] point;
    private DiVector attributionVec;
    private String entityName;

    @Mock
    private ActionListener<RcfResult> rcfResultListener;

    @Mock
    private ActionListener<ThresholdingResult> thresholdResultListener;
    private MemoryTracker memoryTracker;
    private Instant now;

    @Mock
    private ADCircuitBreakerService adCircuitBreakerService;

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
        thresholdMaxRankError = 1e-4;
        thresholdMaxScore = 8.0;
        thresholdNumLogNormalQuantiles = 10000;
        thresholdDownsamples = 1_000_000;
        thresholdMaxSamples = 2_000_000;
        thresholdingModelClass = HybridThresholdingModel.class;
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

        rcf = spy(RandomCutForest.builder().dimensions(numFeatures).sampleSize(numSamples).numberOfTrees(numTrees).build());
        when(rcf.getAnomalyAttribution(point)).thenReturn(attributionVec);

        when(jvmService.info().getMem().getHeapMax().getBytes()).thenReturn(10_000_000_000L);

        when(anomalyDetector.getShingleSize()).thenReturn(shingleSize);

        gson = PowerMockito.mock(Gson.class);

        settings = Settings.builder().put("plugins.anomaly_detection.model_max_size_percent", modelMaxSizePercentage).build();
        ClusterSettings clusterSettings = PowerMockito.mock(ClusterSettings.class);
        clusterService = new ClusterService(settings, clusterSettings, null);
        MemoryTracker memoryTracker = new MemoryTracker(
            jvmService,
            modelMaxSizePercentage,
            modelDesiredSizePercentage,
            clusterService,
            numSamples,
            adCircuitBreakerService
        );

        ExecutorService executorService = mock(ExecutorService.class);
        when(threadPool.executor(AnomalyDetectorPlugin.AD_THREAD_POOL_NAME)).thenReturn(executorService);
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(executorService).execute(any(Runnable.class));

        modelPartitioner = spy(new ModelPartitioner(numSamples, numTrees, nodeFilter, memoryTracker));

        now = Instant.now();
        when(clock.instant()).thenReturn(now);

        modelManager = spy(
            new ModelManager(
                rcfSerde,
                checkpointDao,
                gson,
                clock,
                numTrees,
                numSamples,
                rcfTimeDecay,
                numMinSamples,
                thresholdMinPvalue,
                thresholdMaxRankError,
                thresholdMaxScore,
                thresholdNumLogNormalQuantiles,
                thresholdDownsamples,
                thresholdMaxSamples,
                thresholdingModelClass,
                minPreviewSize,
                modelTtl,
                checkpointInterval,
                entityColdStarter,
                modelPartitioner,
                featureManager,
                memoryTracker
            )
        );

        detectorId = "detectorId";
        modelId = "modelId";
        rcfModelId = "detectorId_model_rcf_1";
        thresholdModelId = "detectorId_model_threshold";
        checkpoint = "testcheckpoint";
        successModelId = "testSuccessModelId";
        failModelId = "testFailModelId";
        successCheckpoint = "testSuccessCheckpoint";
        failCheckpoint = "testFailCheckpoint";

        doAnswer(invocation -> {
            ActionListener<Optional<String>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(checkpoint));
            return null;
        }).when(checkpointDao).getModelCheckpoint(any(String.class), any(ActionListener.class));
        doAnswer(invocation -> {
            ActionListener<Optional<String>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(successCheckpoint));
            return null;
        }).when(checkpointDao).getModelCheckpoint(eq(successModelId), any(ActionListener.class));
        doAnswer(invocation -> {
            ActionListener<Optional<String>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(failCheckpoint));
            return null;
        }).when(checkpointDao).getModelCheckpoint(eq(failModelId), any(ActionListener.class));

        entityName = "1.0.2.3";
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
        assertEquals(expectedDetectorId, modelManager.getDetectorIdForModelId(modelId));
    }

    private Object[] getDetectorIdForModelIdIllegalArgument() {
        return new Object[] { new Object[] { "testId" }, new Object[] { "testid_" }, new Object[] { "_testId" }, };
    }

    @Test(expected = IllegalArgumentException.class)
    @Parameters(method = "getDetectorIdForModelIdIllegalArgument")
    public void getDetectorIdForModelId_throwIllegalArgument_forInvalidId(String modelId) {
        modelManager.getDetectorIdForModelId(modelId);
    }

    private Object[] combineRcfResultsData() {
        double[] attribution = new double[] { 1 };
        return new Object[] {
            new Object[] { asList(), 1, new CombinedRcfResult(0, 0, new double[0]) },
            new Object[] { asList(new RcfResult(0, 0, 0, new double[0])), 1, new CombinedRcfResult(0, 0, new double[0]) },
            new Object[] { asList(new RcfResult(1, 0, 50, attribution)), 1, new CombinedRcfResult(1, 0, attribution) },
            new Object[] {
                asList(new RcfResult(1, 0, 50, attribution), new RcfResult(2, 0, 50, attribution)),
                1,
                new CombinedRcfResult(1.5, 0, attribution) },
            new Object[] {
                asList(new RcfResult(1, 0, 40, attribution), new RcfResult(2, 0, 60, attribution), new RcfResult(3, 0, 100, attribution)),
                1,
                new CombinedRcfResult(2.3, 0, attribution) },
            new Object[] { asList(new RcfResult(0, 1, 100, attribution)), 1, new CombinedRcfResult(0, 1, attribution) },
            new Object[] { asList(new RcfResult(0, 1, 50, attribution)), 1, new CombinedRcfResult(0, 0.5, attribution) },
            new Object[] { asList(new RcfResult(0, 0.5, 1000, attribution)), 1, new CombinedRcfResult(0, 0.5, attribution) },
            new Object[] {
                asList(new RcfResult(0, 1, 50, attribution), new RcfResult(0, 0, 50, attribution)),
                1,
                new CombinedRcfResult(0, 0.5, attribution) },
            new Object[] {
                asList(new RcfResult(0, 0.5, 50, attribution), new RcfResult(0, 0.5, 50, attribution)),
                1,
                new CombinedRcfResult(0, 0.5, attribution) },
            new Object[] {
                asList(new RcfResult(0, 1, 20, attribution), new RcfResult(0, 1, 30, attribution), new RcfResult(0, 0.5, 50, attribution)),
                1,
                new CombinedRcfResult(0, 0.75, attribution) },
            new Object[] {
                asList(new RcfResult(1, 0, 20, new double[] { 0, 0, .5, .5 }), new RcfResult(1, 0, 80, new double[] { 0, .5, .25, .25 })),
                2,
                new CombinedRcfResult(1, 0, new double[] { .5, .5 }) },
            new Object[] {
                asList(new RcfResult(1, 0, 25, new double[] { 0, 0, 1, .0 }), new RcfResult(1, 0, 75, new double[] { 0, 0, 0, 1 })),
                2,
                new CombinedRcfResult(1, 0, new double[] { .25, .75 }) }, };
    }

    @Test
    @Parameters(method = "combineRcfResultsData")
    public void combineRcfResults_returnExpected(List<RcfResult> results, int numFeatures, CombinedRcfResult expected) {
        assertEquals(expected, modelManager.combineRcfResults(results, numFeatures));
    }

    private ImmutableOpenMap<String, DiscoveryNode> createDataNodes(int numDataNodes) {
        ImmutableOpenMap.Builder<String, DiscoveryNode> dataNodes = ImmutableOpenMap.builder();
        for (int i = 0; i < numDataNodes; i++) {
            dataNodes.put("foo" + i, mock(DiscoveryNode.class));
        }
        return dataNodes.build();
    }

    private Object[] getPartitionedForestSizesData() {
        RandomCutForest rcf = RandomCutForest.builder().dimensions(1).sampleSize(10).numberOfTrees(100).build();
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

    @Test
    @Parameters(method = "getPartitionedForestSizesData")
    public void getPartitionedForestSizes_returnExpected(
        RandomCutForest rcf,
        long totalModelSize,
        long heapSize,
        ImmutableOpenMap<String, DiscoveryNode> dataNodes,
        Entry<Integer, Integer> expected
    ) {
        when(jvmService.info().getMem().getHeapMax().getBytes()).thenReturn(heapSize);
        MemoryTracker memoryTracker = spy(
            new MemoryTracker(
                jvmService,
                modelMaxSizePercentage,
                modelDesiredSizePercentage,
                clusterService,
                numSamples,
                adCircuitBreakerService
            )
        );

        when(memoryTracker.estimateModelSize(rcf)).thenReturn(totalModelSize);

        modelPartitioner = spy(new ModelPartitioner(numSamples, numTrees, nodeFilter, memoryTracker));

        when(nodeFilter.getEligibleDataNodes()).thenReturn(dataNodes.values().toArray(DiscoveryNode.class));

        assertEquals(expected, modelPartitioner.getPartitionedForestSizes(rcf, "id"));
    }

    private Object[] getPartitionedForestSizesLimitExceededData() {
        RandomCutForest rcf = RandomCutForest.builder().dimensions(1).sampleSize(10).numberOfTrees(100).build();
        return new Object[] {
            new Object[] { rcf, 101L, 1_000L, createDataNodes(1) },
            new Object[] { rcf, 201L, 1_000L, createDataNodes(2) },
            new Object[] { rcf, 3001L, 10_000L, createDataNodes(3) } };
    }

    @Test(expected = LimitExceededException.class)
    @Parameters(method = "getPartitionedForestSizesLimitExceededData")
    public void getPartitionedForestSizes_throwLimitExceeded(
        RandomCutForest rcf,
        long totalModelSize,
        long heapSize,
        ImmutableOpenMap<String, DiscoveryNode> dataNodes
    ) {
        when(jvmService.info().getMem().getHeapMax().getBytes()).thenReturn(heapSize);
        MemoryTracker memoryTracker = spy(
            new MemoryTracker(
                jvmService,
                modelMaxSizePercentage,
                modelDesiredSizePercentage,
                clusterService,
                numSamples,
                adCircuitBreakerService
            )
        );
        when(memoryTracker.estimateModelSize(rcf)).thenReturn(totalModelSize);
        modelPartitioner = spy(new ModelPartitioner(numSamples, numTrees, nodeFilter, memoryTracker));

        when(nodeFilter.getEligibleDataNodes()).thenReturn(dataNodes.values().toArray(DiscoveryNode.class));

        modelPartitioner.getPartitionedForestSizes(rcf, "id");
    }

    private Object[] estimateModelSizeData() {
        return new Object[] {
            new Object[] { RandomCutForest.builder().dimensions(1).sampleSize(256).numberOfTrees(100).build(), 819200L },
            new Object[] { RandomCutForest.builder().dimensions(5).sampleSize(256).numberOfTrees(100).build(), 4096000L } };
    }

    @Parameters(method = "estimateModelSizeData")
    public void estimateModelSize_returnExpected(RandomCutForest rcf, long expectedSize) {
        assertEquals(expectedSize, memoryTracker.estimateModelSize(rcf));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void getRcfResult_returnExpectedToListener() {
        double[] point = new double[0];
        RandomCutForest forest = mock(RandomCutForest.class);
        double score = 11.;

        doAnswer(invocation -> {
            ActionListener<Optional<String>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(checkpoint));
            return null;
        }).when(checkpointDao).getModelCheckpoint(eq(rcfModelId), any(ActionListener.class));
        when(rcfSerde.fromJson(checkpoint)).thenReturn(forest);
        when(forest.getAnomalyScore(point)).thenReturn(score);
        when(forest.getNumberOfTrees()).thenReturn(numTrees);
        when(forest.getLambda()).thenReturn(rcfTimeDecay);
        when(forest.getSampleSize()).thenReturn(numSamples);
        when(forest.getTotalUpdates()).thenReturn((long) numSamples);
        when(forest.getAnomalyAttribution(point)).thenReturn(attributionVec);

        ActionListener<RcfResult> listener = mock(ActionListener.class);
        modelManager.getRcfResult(detectorId, rcfModelId, point, listener);

        RcfResult expected = new RcfResult(score, 0, numTrees, new double[] { 0.5, 0.5 });
        verify(listener).onResponse(eq(expected));

        when(forest.getTotalUpdates()).thenReturn(numSamples + 1L);
        listener = mock(ActionListener.class);
        modelManager.getRcfResult(detectorId, rcfModelId, point, listener);

        ArgumentCaptor<RcfResult> responseCaptor = ArgumentCaptor.forClass(RcfResult.class);
        verify(listener).onResponse(responseCaptor.capture());
        assertEquals(0.091353632, responseCaptor.getValue().getConfidence(), 1e-6);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void getRcfResult_throwToListener_whenNoCheckpoint() {
        doAnswer(invocation -> {
            ActionListener<Optional<String>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.empty());
            return null;
        }).when(checkpointDao).getModelCheckpoint(eq(rcfModelId), any(ActionListener.class));

        ActionListener<RcfResult> listener = mock(ActionListener.class);
        modelManager.getRcfResult(detectorId, rcfModelId, new double[0], listener);

        verify(listener).onFailure(any(ResourceNotFoundException.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void getRcfResult_throwToListener_whenHeapLimitExceed() {
        doAnswer(invocation -> {
            ActionListener<Optional<String>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(checkpoint));
            return null;
        }).when(checkpointDao).getModelCheckpoint(eq(rcfModelId), any(ActionListener.class));
        when(rcfSerde.fromJson(checkpoint)).thenReturn(rcf);

        when(jvmService.info().getMem().getHeapMax().getBytes()).thenReturn(1_000L);
        MemoryTracker memoryTracker = new MemoryTracker(
            jvmService,
            modelMaxSizePercentage,
            modelDesiredSizePercentage,
            clusterService,
            numSamples,
            adCircuitBreakerService
        );

        ActionListener<RcfResult> listener = mock(ActionListener.class);

        // use new memoryTracker
        modelManager = spy(
            new ModelManager(
                rcfSerde,
                checkpointDao,
                gson,
                clock,
                numTrees,
                numSamples,
                rcfTimeDecay,
                numMinSamples,
                thresholdMinPvalue,
                thresholdMaxRankError,
                thresholdMaxScore,
                thresholdNumLogNormalQuantiles,
                thresholdDownsamples,
                thresholdMaxSamples,
                thresholdingModelClass,
                minPreviewSize,
                modelTtl,
                checkpointInterval,
                entityColdStarter,
                modelPartitioner,
                featureManager,
                memoryTracker
            )
        );

        modelManager.getRcfResult(detectorId, rcfModelId, new double[0], listener);

        verify(listener).onFailure(any(LimitExceededException.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void getThresholdingResult_returnExpectedToListener() {
        double score = 1.;
        double grade = 0.;
        double confidence = 0.5;

        doAnswer(invocation -> {
            ActionListener<Optional<String>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(checkpoint));
            return null;
        }).when(checkpointDao).getModelCheckpoint(eq(thresholdModelId), any(ActionListener.class));
        PowerMockito.doReturn(hybridThresholdingModel).when(gson).fromJson(checkpoint, thresholdingModelClass);
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
    @SuppressWarnings("unchecked")
    public void getThresholdingResult_throwToListener_withNoCheckpoint() {
        doAnswer(invocation -> {
            ActionListener<Optional<String>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.empty());
            return null;
        }).when(checkpointDao).getModelCheckpoint(eq(thresholdModelId), any(ActionListener.class));

        ActionListener<ThresholdingResult> listener = mock(ActionListener.class);
        modelManager.getThresholdingResult(detectorId, thresholdModelId, 0, listener);

        verify(listener).onFailure(any(ResourceNotFoundException.class));
    }

    @Test
    public void getThresholdingResult_notUpdate_withZeroScore() {
        double score = 0.0;

        doAnswer(invocation -> {
            ActionListener<Optional<String>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(checkpoint));
            return null;
        }).when(checkpointDao).getModelCheckpoint(eq(thresholdModelId), any(ActionListener.class));
        PowerMockito.doReturn(hybridThresholdingModel).when(gson).fromJson(checkpoint, thresholdingModelClass);

        ActionListener<ThresholdingResult> listener = mock(ActionListener.class);
        modelManager.getThresholdingResult(detectorId, thresholdModelId, score, listener);

        verify(hybridThresholdingModel, never()).update(score);
    }

    @Test
    public void getAllModelIds_returnAllIds_forRcfAndThreshold() {

        when(checkpointDao.getModelCheckpoint(rcfModelId)).thenReturn(Optional.of(checkpoint));
        when(rcfSerde.fromJson(checkpoint)).thenReturn(mock(RandomCutForest.class));
        modelManager.getRcfResult(detectorId, rcfModelId, new double[0], rcfResultListener);
        when(checkpointDao.getModelCheckpoint(thresholdModelId)).thenReturn(Optional.of(checkpoint));
        PowerMockito.doReturn(hybridThresholdingModel).when(gson).fromJson(checkpoint, thresholdingModelClass);
        modelManager.getThresholdingResult(detectorId, thresholdModelId, 0, thresholdResultListener);

        assertEquals(Stream.of(rcfModelId, thresholdModelId).collect(Collectors.toSet()), modelManager.getAllModelIds());
    }

    @Test
    public void getAllModelIds_returnEmpty_forNoModels() {
        assertEquals(Collections.emptySet(), modelManager.getAllModelIds());
    }

    @Test
    public void stopModel_saveRcfCheckpoint() {

        RandomCutForest forest = mock(RandomCutForest.class);
        when(checkpointDao.getModelCheckpoint(rcfModelId)).thenReturn(Optional.of(checkpoint));
        when(rcfSerde.fromJson(checkpoint)).thenReturn(forest);
        when(rcfSerde.toJson(forest)).thenReturn(checkpoint);
        modelManager.getRcfResult(detectorId, rcfModelId, new double[0], rcfResultListener);
        when(clock.instant()).thenReturn(Instant.EPOCH);

        modelManager.stopModel(detectorId, rcfModelId);

        verify(checkpointDao).putModelCheckpoint(rcfModelId, checkpoint);
    }

    @Test
    public void stopModel_saveThresholdCheckpoint() {

        when(checkpointDao.getModelCheckpoint(thresholdModelId)).thenReturn(Optional.of(checkpoint));
        PowerMockito.doReturn(hybridThresholdingModel).when(gson).fromJson(checkpoint, thresholdingModelClass);
        PowerMockito.doReturn(checkpoint).when(gson).toJson(hybridThresholdingModel);
        modelManager.getThresholdingResult(detectorId, thresholdModelId, 0, thresholdResultListener);
        when(clock.instant()).thenReturn(Instant.EPOCH);

        modelManager.stopModel(detectorId, thresholdModelId);

        verify(checkpointDao).putModelCheckpoint(thresholdModelId, checkpoint);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void stopModel_returnExpectedToListener_whenRcfStop() {
        RandomCutForest forest = mock(RandomCutForest.class);
        when(checkpointDao.getModelCheckpoint(rcfModelId)).thenReturn(Optional.of(checkpoint));
        when(rcfSerde.fromJson(checkpoint)).thenReturn(forest);
        when(rcfSerde.toJson(forest)).thenReturn(checkpoint);
        modelManager.getRcfResult(detectorId, rcfModelId, new double[0], rcfResultListener);
        when(clock.instant()).thenReturn(Instant.EPOCH);
        doAnswer(invocation -> {
            ActionListener<Void> listener = invocation.getArgument(2);
            listener.onResponse(null);
            return null;
        }).when(checkpointDao).putModelCheckpoint(eq(rcfModelId), eq(checkpoint), any(ActionListener.class));

        ActionListener<Void> listener = mock(ActionListener.class);
        modelManager.stopModel(detectorId, rcfModelId, listener);

        verify(listener).onResponse(eq(null));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void stopModel_returnExpectedToListener_whenThresholdStop() {
        when(checkpointDao.getModelCheckpoint(thresholdModelId)).thenReturn(Optional.of(checkpoint));
        PowerMockito.doReturn(hybridThresholdingModel).when(gson).fromJson(checkpoint, thresholdingModelClass);
        PowerMockito.doReturn(checkpoint).when(gson).toJson(hybridThresholdingModel);
        modelManager.getThresholdingResult(detectorId, thresholdModelId, 0, thresholdResultListener);
        when(clock.instant()).thenReturn(Instant.EPOCH);
        doAnswer(invocation -> {
            ActionListener<Void> listener = invocation.getArgument(2);
            listener.onResponse(null);
            return null;
        }).when(checkpointDao).putModelCheckpoint(eq(thresholdModelId), eq(checkpoint), any(ActionListener.class));

        ActionListener<Void> listener = mock(ActionListener.class);
        modelManager.stopModel(detectorId, thresholdModelId, listener);

        verify(listener).onResponse(eq(null));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void stopModel_throwToListener_whenCheckpointFail() {
        RandomCutForest forest = mock(RandomCutForest.class);
        when(checkpointDao.getModelCheckpoint(rcfModelId)).thenReturn(Optional.of(checkpoint));
        when(rcfSerde.fromJson(checkpoint)).thenReturn(forest);
        when(rcfSerde.toJson(forest)).thenReturn(checkpoint);
        modelManager.getRcfResult(detectorId, rcfModelId, new double[0], rcfResultListener);
        when(clock.instant()).thenReturn(Instant.EPOCH);
        doAnswer(invocation -> {
            ActionListener<Void> listener = invocation.getArgument(2);
            listener.onFailure(new RuntimeException());
            return null;
        }).when(checkpointDao).putModelCheckpoint(eq(rcfModelId), eq(checkpoint), any(ActionListener.class));

        ActionListener<Void> listener = mock(ActionListener.class);
        modelManager.stopModel(detectorId, rcfModelId, listener);

        verify(listener).onFailure(any(Exception.class));
    }

    @Test
    public void clear_deleteRcfCheckpoint() {

        RandomCutForest forest = mock(RandomCutForest.class);
        when(checkpointDao.getModelCheckpoint(rcfModelId)).thenReturn(Optional.of(checkpoint));
        when(rcfSerde.fromJson(checkpoint)).thenReturn(forest);
        modelManager.getRcfResult(detectorId, rcfModelId, new double[0], rcfResultListener);

        modelManager.clear(detectorId);

        verify(checkpointDao).deleteModelCheckpoint(rcfModelId);
    }

    @Test
    public void clear_deleteThresholdCheckpoint() {

        when(checkpointDao.getModelCheckpoint(thresholdModelId)).thenReturn(Optional.of(checkpoint));
        PowerMockito.doReturn(hybridThresholdingModel).when(gson).fromJson(checkpoint, thresholdingModelClass);
        PowerMockito.doReturn(checkpoint).when(gson).toJson(hybridThresholdingModel);
        modelManager.getThresholdingResult(detectorId, thresholdModelId, 0, thresholdResultListener);

        modelManager.clear(detectorId);

        verify(checkpointDao).deleteModelCheckpoint(thresholdModelId);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void clear_callListener_whenRcfDeleted() {
        String otherModelId = detectorId + rcfModelId;
        RandomCutForest forest = mock(RandomCutForest.class);
        when(checkpointDao.getModelCheckpoint(rcfModelId)).thenReturn(Optional.of(checkpoint));
        when(checkpointDao.getModelCheckpoint(otherModelId)).thenReturn(Optional.of(checkpoint));
        when(rcfSerde.fromJson(checkpoint)).thenReturn(forest);
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
    @SuppressWarnings("unchecked")
    public void clear_callListener_whenThresholdDeleted() {
        when(checkpointDao.getModelCheckpoint(thresholdModelId)).thenReturn(Optional.of(checkpoint));
        PowerMockito.doReturn(hybridThresholdingModel).when(gson).fromJson(checkpoint, thresholdingModelClass);
        PowerMockito.doReturn(checkpoint).when(gson).toJson(hybridThresholdingModel);
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
    @SuppressWarnings("unchecked")
    public void clear_throwToListener_whenDeleteFail() {
        RandomCutForest forest = mock(RandomCutForest.class);
        when(checkpointDao.getModelCheckpoint(rcfModelId)).thenReturn(Optional.of(checkpoint));
        when(rcfSerde.fromJson(checkpoint)).thenReturn(forest);
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
    public void trainModel_putTrainedModels() {
        double[][] trainData = new Random().doubles().limit(100).mapToObj(d -> new double[] { d }).toArray(double[][]::new);
        doReturn(new SimpleEntry<>(1, 10)).when(modelPartitioner).getPartitionedForestSizes(anyObject(), anyObject());
        doReturn(asList("feature1")).when(anomalyDetector).getEnabledFeatureIds();
        modelManager.trainModel(anomalyDetector, trainData);

        verify(checkpointDao).putModelCheckpoint(eq(modelPartitioner.getRcfModelId(anomalyDetector.getDetectorId(), 0)), anyObject());
        verify(checkpointDao).putModelCheckpoint(eq(modelPartitioner.getThresholdModelId(anomalyDetector.getDetectorId())), anyObject());
    }

    private Object[] trainModelIllegalArgumentData() {
        return new Object[] { new Object[] { new double[][] {} }, new Object[] { new double[][] { {} } } };
    }

    @Test(expected = IllegalArgumentException.class)
    @Parameters(method = "trainModelIllegalArgumentData")
    public void trainModel_throwIllegalArgument_forInvalidInput(double[][] trainData) {
        modelManager.trainModel(anomalyDetector, trainData);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void trainModel_returnExpectedToListener_putCheckpoints() {
        double[][] trainData = new Random().doubles().limit(100).mapToObj(d -> new double[] { d }).toArray(double[][]::new);
        doReturn(new SimpleEntry<>(2, 10)).when(modelPartitioner).getPartitionedForestSizes(anyObject(), anyObject());
        doAnswer(invocation -> {
            ActionListener<Void> listener = invocation.getArgument(2);
            listener.onResponse(null);
            return null;
        }).when(checkpointDao).putModelCheckpoint(any(), any(), any(ActionListener.class));

        ActionListener<Void> listener = mock(ActionListener.class);
        modelManager.trainModel(anomalyDetector, trainData, listener);

        verify(listener).onResponse(eq(null));
        verify(checkpointDao, times(3)).putModelCheckpoint(any(), any(), any());
    }

    @Test
    @SuppressWarnings("unchecked")
    @Parameters(method = "trainModelIllegalArgumentData")
    public void trainModel_throwIllegalArgumentToListener_forInvalidTrainData(double[][] trainData) {
        ActionListener<Void> listener = mock(ActionListener.class);
        modelManager.trainModel(anomalyDetector, trainData, listener);

        verify(listener).onFailure(any(IllegalArgumentException.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void trainModel_throwLimitExceededToListener_whenLimitExceed() {
        doThrow(new LimitExceededException(null, null)).when(modelPartitioner).getPartitionedForestSizes(anyObject(), anyObject());

        ActionListener<Void> listener = mock(ActionListener.class);
        modelManager.trainModel(anomalyDetector, new double[][] { { 0 } }, listener);

        verify(listener).onFailure(any(LimitExceededException.class));
    }

    @Test
    public void getRcfModelId_returnNonEmptyString() {
        String rcfModelId = modelPartitioner.getRcfModelId(anomalyDetector.getDetectorId(), 0);

        assertFalse(rcfModelId.isEmpty());
    }

    @Test
    public void getThresholdModelId_returnNonEmptyString() {
        String thresholdModelId = modelPartitioner.getThresholdModelId(anomalyDetector.getDetectorId());

        assertFalse(thresholdModelId.isEmpty());
    }

    private Entry<Integer, Integer> pair(int size, int value) {
        return new SimpleImmutableEntry<>(size, value);
    }

    @Test
    public void maintenance_doNothing_givenNoModel() {
        modelManager.maintenance();

        verifyZeroInteractions(checkpointDao);
    }

    @Test
    public void maintenance_saveRcfCheckpoint_skippingFailure() {
        double[] point = new double[0];
        RandomCutForest forest = mock(RandomCutForest.class);
        RandomCutForest failForest = mock(RandomCutForest.class);

        when(checkpointDao.getModelCheckpoint(successModelId)).thenReturn(Optional.of(successCheckpoint));
        when(checkpointDao.getModelCheckpoint(failModelId)).thenReturn(Optional.of(failCheckpoint));
        when(rcfSerde.fromJson(successCheckpoint)).thenReturn(forest);
        when(rcfSerde.fromJson(failCheckpoint)).thenReturn(failForest);
        when(rcfSerde.toJson(forest)).thenReturn(successCheckpoint);
        when(rcfSerde.toJson(failForest)).thenThrow(new RuntimeException());
        when(clock.instant()).thenReturn(Instant.EPOCH);
        modelManager.getRcfResult(detectorId, successModelId, point, rcfResultListener);
        modelManager.getRcfResult(detectorId, failModelId, point, rcfResultListener);

        modelManager.maintenance();

        verify(checkpointDao).putModelCheckpoint(successModelId, successCheckpoint);
    }

    @Test
    public void maintenance_saveThresholdCheckpoint_skippingFailure() {
        double score = 1.;
        HybridThresholdingModel failThresholdModel = mock(HybridThresholdingModel.class);
        when(checkpointDao.getModelCheckpoint(successModelId)).thenReturn(Optional.of(successCheckpoint));
        when(checkpointDao.getModelCheckpoint(failModelId)).thenReturn(Optional.of(failCheckpoint));
        doReturn(hybridThresholdingModel).when(gson).fromJson(successCheckpoint, thresholdingModelClass);
        doReturn(failThresholdModel).when(gson).fromJson(failCheckpoint, thresholdingModelClass);
        doReturn(successCheckpoint).when(gson).toJson(hybridThresholdingModel);
        doThrow(new RuntimeException()).when(gson).toJson(failThresholdModel);
        when(clock.instant()).thenReturn(Instant.EPOCH);
        modelManager.getThresholdingResult(detectorId, successModelId, score, thresholdResultListener);
        modelManager.getThresholdingResult(detectorId, failModelId, score, thresholdResultListener);

        modelManager.maintenance();

        verify(checkpointDao).putModelCheckpoint(successModelId, successCheckpoint);
    }

    @Test
    public void maintenance_stopInactiveRcfModel() {
        String modelId = "testModelId";
        double[] point = new double[0];
        RandomCutForest forest = mock(RandomCutForest.class);
        when(checkpointDao.getModelCheckpoint(modelId)).thenReturn(Optional.of(checkpoint));
        when(rcfSerde.fromJson(checkpoint)).thenReturn(forest);
        when(rcfSerde.toJson(forest)).thenReturn(checkpoint);
        when(clock.instant()).thenReturn(Instant.MIN, Instant.EPOCH, Instant.EPOCH.plus(modelTtl).plus(Duration.ofSeconds(1)));
        modelManager.getRcfResult(detectorId, modelId, point, rcfResultListener);

        modelManager.maintenance();

        modelManager.getRcfResult(detectorId, modelId, point, rcfResultListener);
        verify(checkpointDao, times(2)).getModelCheckpoint(eq(modelId), any(ActionListener.class));
    }

    @Test
    public void maintenance_keepActiveRcfModel() {
        when(rcfSerde.fromJson(checkpoint)).thenReturn(rcf);
        when(rcfSerde.toJson(rcf)).thenReturn(checkpoint);
        when(clock.instant()).thenReturn(Instant.MIN, Instant.EPOCH, Instant.EPOCH);
        modelManager.getRcfResult(detectorId, modelId, point, rcfResultListener);

        modelManager.maintenance();

        modelManager.getRcfResult(detectorId, modelId, point, rcfResultListener);
        verify(checkpointDao, times(1)).getModelCheckpoint(eq(modelId), any(ActionListener.class));
    }

    @Test
    public void maintenance_stopInactiveThresholdModel() {
        String modelId = "testModelId";
        double score = 1.;
        when(checkpointDao.getModelCheckpoint(modelId)).thenReturn(Optional.of(checkpoint));
        doReturn(hybridThresholdingModel).when(gson).fromJson(checkpoint, thresholdingModelClass);
        doReturn(checkpoint).when(gson).toJson(hybridThresholdingModel);
        when(clock.instant()).thenReturn(Instant.MIN, Instant.EPOCH, Instant.EPOCH.plus(modelTtl).plus(Duration.ofSeconds(1)));
        modelManager.getThresholdingResult(detectorId, modelId, score, thresholdResultListener);

        modelManager.maintenance();

        modelManager.getThresholdingResult(detectorId, modelId, score, thresholdResultListener);
        verify(checkpointDao, times(2)).getModelCheckpoint(eq(modelId), any(ActionListener.class));
    }

    @Test
    public void maintenance_keepActiveThresholdModel() {
        String modelId = "testModelId";
        double score = 1.;
        when(checkpointDao.getModelCheckpoint(modelId)).thenReturn(Optional.of(checkpoint));
        doReturn(hybridThresholdingModel).when(gson).fromJson(checkpoint, thresholdingModelClass);
        doReturn(checkpoint).when(gson).toJson(hybridThresholdingModel);
        when(clock.instant()).thenReturn(Instant.MIN, Instant.EPOCH, Instant.EPOCH);
        modelManager.getThresholdingResult(detectorId, modelId, score, thresholdResultListener);

        modelManager.maintenance();

        modelManager.getThresholdingResult(detectorId, modelId, score, thresholdResultListener);
        verify(checkpointDao, times(1)).getModelCheckpoint(eq(modelId), any(ActionListener.class));
    }

    @Test
    public void maintenance_skipCheckpoint_whenLastCheckpointIsRecent() {
        String modelId = "testModelId";
        double score = 1.;
        when(checkpointDao.getModelCheckpoint(modelId)).thenReturn(Optional.of(checkpoint));
        doReturn(hybridThresholdingModel).when(gson).fromJson(checkpoint, thresholdingModelClass);
        doReturn(checkpoint).when(gson).toJson(hybridThresholdingModel);
        when(clock.instant()).thenReturn(Instant.MIN, Instant.EPOCH);
        modelManager.getThresholdingResult(detectorId, modelId, score, thresholdResultListener);

        modelManager.maintenance();
        modelManager.maintenance();

        verify(checkpointDao, times(1)).putModelCheckpoint(eq(modelId), anyObject());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void maintenance_returnExpectedToListener_forRcfModel() {
        String successModelId = "testSuccessModelId";
        String failModelId = "testFailModelId";
        String successCheckpoint = "testSuccessCheckpoint";
        String failCheckpoint = "testFailCheckpoint";
        double[] point = new double[0];
        RandomCutForest forest = mock(RandomCutForest.class);
        RandomCutForest failForest = mock(RandomCutForest.class);

        doAnswer(invocation -> {
            ActionListener<Optional<String>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(successCheckpoint));
            return null;
        }).when(checkpointDao).getModelCheckpoint(eq(successModelId), any(ActionListener.class));
        doAnswer(invocation -> {
            ActionListener<Optional<String>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(failCheckpoint));
            return null;
        }).when(checkpointDao).getModelCheckpoint(eq(failModelId), any(ActionListener.class));
        doAnswer(invocation -> {
            ActionListener<Optional<String>> listener = invocation.getArgument(2);
            listener.onResponse(null);
            return null;
        }).when(checkpointDao).putModelCheckpoint(eq(successModelId), eq(successCheckpoint), any(ActionListener.class));
        doAnswer(invocation -> {
            ActionListener<Optional<String>> listener = invocation.getArgument(2);
            listener.onFailure(new RuntimeException());
            return null;
        }).when(checkpointDao).putModelCheckpoint(eq(failModelId), eq(failCheckpoint), any(ActionListener.class));
        when(rcfSerde.fromJson(successCheckpoint)).thenReturn(forest);
        when(rcfSerde.fromJson(failCheckpoint)).thenReturn(failForest);
        when(rcfSerde.toJson(forest)).thenReturn(successCheckpoint);
        when(rcfSerde.toJson(failForest)).thenReturn(failCheckpoint);
        when(clock.instant()).thenReturn(Instant.EPOCH);
        ActionListener<RcfResult> scoreListener = mock(ActionListener.class);
        modelManager.getRcfResult(detectorId, successModelId, point, scoreListener);
        modelManager.getRcfResult(detectorId, failModelId, point, scoreListener);

        ActionListener<Void> listener = mock(ActionListener.class);
        modelManager.maintenance(listener);

        verify(listener).onResponse(eq(null));
        verify(checkpointDao, times(1)).putModelCheckpoint(eq(successModelId), eq(successCheckpoint), any(ActionListener.class));
        verify(checkpointDao, times(1)).putModelCheckpoint(eq(failModelId), eq(failCheckpoint), any(ActionListener.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void maintenance_returnExpectedToListener_forThresholdModel() {
        String successModelId = "testSuccessModelId";
        String failModelId = "testFailModelId";
        String successCheckpoint = "testSuccessCheckpoint";
        String failCheckpoint = "testFailCheckpoint";
        double score = 1.;
        HybridThresholdingModel failThresholdModel = mock(HybridThresholdingModel.class);
        doAnswer(invocation -> {
            ActionListener<Optional<String>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(successCheckpoint));
            return null;
        }).when(checkpointDao).getModelCheckpoint(eq(successModelId), any(ActionListener.class));
        doAnswer(invocation -> {
            ActionListener<Optional<String>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(failCheckpoint));
            return null;
        }).when(checkpointDao).getModelCheckpoint(eq(failModelId), any(ActionListener.class));
        doAnswer(invocation -> {
            ActionListener<Optional<String>> listener = invocation.getArgument(2);
            listener.onResponse(null);
            return null;
        }).when(checkpointDao).putModelCheckpoint(eq(successModelId), eq(successCheckpoint), any(ActionListener.class));
        doAnswer(invocation -> {
            ActionListener<Optional<String>> listener = invocation.getArgument(2);
            listener.onFailure(new RuntimeException());
            return null;
        }).when(checkpointDao).putModelCheckpoint(eq(failModelId), eq(failCheckpoint), any(ActionListener.class));
        doReturn(hybridThresholdingModel).when(gson).fromJson(successCheckpoint, thresholdingModelClass);
        doReturn(failThresholdModel).when(gson).fromJson(failCheckpoint, thresholdingModelClass);
        doReturn(successCheckpoint).when(gson).toJson(hybridThresholdingModel);
        doThrow(new RuntimeException()).when(gson).toJson(failThresholdModel);
        when(clock.instant()).thenReturn(Instant.EPOCH);
        ActionListener<ThresholdingResult> scoreListener = mock(ActionListener.class);
        modelManager.getThresholdingResult(detectorId, successModelId, score, scoreListener);
        modelManager.getThresholdingResult(detectorId, failModelId, score, scoreListener);

        ActionListener<Void> listener = mock(ActionListener.class);
        modelManager.maintenance(listener);

        verify(listener).onResponse(eq(null));
        verify(checkpointDao, times(1)).putModelCheckpoint(eq(successModelId), eq(successCheckpoint), any(ActionListener.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void maintenance_returnExpectedToListener_stopModel() {
        double[] point = new double[0];
        RandomCutForest forest = mock(RandomCutForest.class);

        doAnswer(invocation -> {
            ActionListener<Optional<String>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(checkpoint));
            return null;
        }).when(checkpointDao).getModelCheckpoint(eq(rcfModelId), any(ActionListener.class));
        doAnswer(invocation -> {
            ActionListener<Optional<String>> listener = invocation.getArgument(2);
            listener.onResponse(null);
            return null;
        }).when(checkpointDao).putModelCheckpoint(eq(rcfModelId), eq(checkpoint), any(ActionListener.class));
        when(rcfSerde.fromJson(checkpoint)).thenReturn(forest);
        when(rcfSerde.toJson(forest)).thenReturn(checkpoint);
        when(clock.instant()).thenReturn(Instant.EPOCH, Instant.EPOCH, Instant.EPOCH.plus(modelTtl.plusSeconds(1)));
        ActionListener<RcfResult> scoreListener = mock(ActionListener.class);
        modelManager.getRcfResult(detectorId, rcfModelId, point, scoreListener);

        ActionListener<Void> listener = mock(ActionListener.class);
        modelManager.maintenance(listener);
        verify(listener).onResponse(eq(null));

        modelManager.getRcfResult(detectorId, rcfModelId, point, scoreListener);
        verify(checkpointDao, times(2)).getModelCheckpoint(eq(rcfModelId), any(ActionListener.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void maintenance_returnExpectedToListener_doNothing() {
        double[] point = new double[0];
        RandomCutForest forest = mock(RandomCutForest.class);

        doAnswer(invocation -> {
            ActionListener<Optional<String>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(checkpoint));
            return null;
        }).when(checkpointDao).getModelCheckpoint(eq(rcfModelId), any(ActionListener.class));
        doAnswer(invocation -> {
            ActionListener<Optional<String>> listener = invocation.getArgument(2);
            listener.onResponse(null);
            return null;
        }).when(checkpointDao).putModelCheckpoint(eq(rcfModelId), eq(checkpoint), any(ActionListener.class));
        when(rcfSerde.fromJson(checkpoint)).thenReturn(forest);
        when(forest.getAnomalyAttribution(point)).thenReturn(attributionVec);
        when(rcfSerde.toJson(forest)).thenReturn(checkpoint);
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
        verify(checkpointDao, times(1)).getModelCheckpoint(eq(rcfModelId), any(ActionListener.class));
    }

    @Test
    public void getPreviewResults_returnNoAnomalies_forNoAnomalies() {
        int numPoints = 1000;
        double[][] points = Stream.generate(() -> new double[] { 0 }).limit(numPoints).toArray(double[][]::new);

        List<ThresholdingResult> results = modelManager.getPreviewResults(points);

        assertEquals(numPoints, results.size());
        assertTrue(results.stream().noneMatch(r -> r.getGrade() > 0));
    }

    @Test
    public void getPreviewResults_returnAnomalies_forLastAnomaly() {
        int numPoints = 1000;
        double[][] points = Stream.generate(() -> new double[] { 0 }).limit(numPoints).toArray(double[][]::new);
        points[points.length - 1] = new double[] { 1. };

        List<ThresholdingResult> results = modelManager.getPreviewResults(points);

        assertEquals(numPoints, results.size());
        assertTrue(results.stream().limit(numPoints - 1).noneMatch(r -> r.getGrade() > 0));
        assertTrue(results.get(numPoints - 1).getGrade() > 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getPreviewResults_throwIllegalArgument_forInvalidInput() {
        modelManager.getPreviewResults(new double[0][0]);
    }
}
