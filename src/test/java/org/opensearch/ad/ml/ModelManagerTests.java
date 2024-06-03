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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
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
import java.util.AbstractMap.SimpleEntry;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.ratelimit.ADCheckpointWriteWorker;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.monitor.jvm.JvmService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.MemoryTracker;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.common.exception.LimitExceededException;
import org.opensearch.timeseries.common.exception.ResourceNotFoundException;
import org.opensearch.timeseries.dataprocessor.LinearUniformImputer;
import org.opensearch.timeseries.feature.FeatureManager;
import org.opensearch.timeseries.feature.Features;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.ml.ModelManager;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.ml.Sample;
import org.opensearch.timeseries.ml.SingleStreamModelIdMapper;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;

import com.amazon.randomcutforest.RandomCutForest;
import com.amazon.randomcutforest.parkservices.AnomalyDescriptor;
import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;
import com.amazon.randomcutforest.returntypes.DiVector;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import test.org.opensearch.ad.util.MLUtil;
import test.org.opensearch.ad.util.RandomModelStateConfig;

@RunWith(JUnitParamsRunner.class)
@SuppressWarnings("unchecked")
public class ModelManagerTests {

    private ADModelManager modelManager;

    @Mock
    private AnomalyDetector anomalyDetector;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private DiscoveryNodeFilterer nodeFilter;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private JvmService jvmService;

    @Mock
    private ADCheckpointDao checkpointDao;

    @Mock
    private Clock clock;

    @Mock
    private FeatureManager featureManager;

    @Mock
    private ADColdStart entityColdStarter;

    @Mock
    private ModelState<ThresholdedRandomCutForest> modelState;

    @Mock
    private ThresholdedRandomCutForest trcf;

    private double modelMaxSizePercentage;
    private int numTrees;
    private int numSamples;
    private int numFeatures;
    private int numMinSamples;
    private double thresholdMinPvalue;
    private int minPreviewSize;
    private Duration modelTtl;
    // private ThresholdedRandomCutForest rcf;

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
    private AnomalyDescriptor descriptor;

    @Mock
    private ActionListener<ThresholdingResult> rcfResultListener;

    @Mock
    private ActionListener<ThresholdingResult> thresholdResultListener;
    private MemoryTracker memoryTracker;
    private Instant now;

    @Mock
    private CircuitBreakerService adCircuitBreakerService;

    private String modelId = "modelId";

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        modelMaxSizePercentage = 0.1;
        numTrees = 100;
        numSamples = 10;
        numFeatures = 1;
        numMinSamples = 32;
        thresholdMinPvalue = 0.95;
        minPreviewSize = 500;
        modelTtl = Duration.ofHours(1);
        shingleSize = 1;
        attribution = new double[] { 1, 1 };
        attributionVec = new DiVector(attribution.length);
        for (int i = 0; i < attribution.length; i++) {
            attributionVec.high[i] = attribution[i];
            attributionVec.low[i] = attribution[i] - 1;
        }
        point = new double[] { 2 };

        // rcf = mock(ThresholdedRandomCutForest.class);
        double score = 11.;

        double confidence = 0.091353632;
        double grade = 0.1;
        descriptor = new AnomalyDescriptor(point, 0);
        descriptor.setRCFScore(score);
        descriptor.setNumberOfTrees(numTrees);
        descriptor.setDataConfidence(confidence);
        descriptor.setAnomalyGrade(grade);
        descriptor.setAttribution(attributionVec);
        descriptor.setTotalUpdates(numSamples);
        descriptor.setRelevantAttribution(new double[] { 0, 0, 0, 0, 0 });
        when(trcf.process(any(), anyLong())).thenReturn(descriptor);

        ExecutorService executorService = mock(ExecutorService.class);
        when(threadPool.executor(TimeSeriesAnalyticsPlugin.AD_THREAD_POOL_NAME)).thenReturn(executorService);
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(executorService).execute(any(Runnable.class));

        now = Instant.now();
        when(clock.instant()).thenReturn(now);

        memoryTracker = mock(MemoryTracker.class);
        when(memoryTracker.isHostingAllowed(anyString(), any())).thenReturn(true);

        settings = Settings
            .builder()
            .put("plugins.anomaly_detection.model_max_size_percent", modelMaxSizePercentage)
            .put("plugins.anomaly_detection.checkpoint_saving_freq", TimeValue.timeValueHours(12))
            .build();

        modelManager = spy(
            new ADModelManager(
                checkpointDao,
                clock,
                numTrees,
                numSamples,
                numMinSamples,
                thresholdMinPvalue,
                minPreviewSize,
                modelTtl,
                AnomalyDetectorSettings.AD_CHECKPOINT_SAVING_FREQ,
                entityColdStarter,
                featureManager,
                memoryTracker,
                settings,
                null
            )
        );

        detectorId = "detectorId";
        rcfModelId = "detectorId_model_rcf_1";
        thresholdModelId = "detectorId_model_threshold";

        when(this.modelState.getModel()).thenReturn(Optional.of(this.trcf));

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
        assertEquals(expectedDetectorId, SingleStreamModelIdMapper.getConfigIdForModelId(modelId));
    }

    private Object[] getDetectorIdForModelIdIllegalArgument() {
        return new Object[] { new Object[] { "testId" }, new Object[] { "testid_" }, new Object[] { "_testId" }, };
    }

    @Test(expected = IllegalArgumentException.class)
    @Parameters(method = "getDetectorIdForModelIdIllegalArgument")
    public void getDetectorIdForModelId_throwIllegalArgument_forInvalidId(String modelId) {
        SingleStreamModelIdMapper.getConfigIdForModelId(modelId);
    }

    private Map<String, DiscoveryNode> createDataNodes(int numDataNodes) {
        Map<String, DiscoveryNode> dataNodes = new HashMap<>();
        for (int i = 0; i < numDataNodes; i++) {
            dataNodes.put("foo" + i, mock(DiscoveryNode.class));
        }
        return dataNodes;
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
        // input length is 2
        when(rcf.getDimensions()).thenReturn(16);
        when(rcf.getShingleSize()).thenReturn(8);
        double score = 11.;

        doAnswer(invocation -> {
            ActionListener<Optional<ThresholdedRandomCutForest>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(rForest));
            return null;
        }).when(checkpointDao).getTRCFModel(eq(rcfModelId), any(ActionListener.class));

        double confidence = 0.091353632;
        double grade = 0.1;
        int relativeIndex = 0;
        double[] currentTimeAttribution = new double[] { 0.5, 0.5 };
        double[] pastalues = new double[] { 123, 456 };
        double[][] expectedValuesList = new double[][] { new double[] { 789, 12 } };
        double[] likelihood = new double[] { 1 };
        double threshold = 1.1d;

        AnomalyDescriptor descriptor = new AnomalyDescriptor(point, 0);
        descriptor.setRCFScore(score);
        descriptor.setNumberOfTrees(numTrees);
        descriptor.setDataConfidence(confidence);
        descriptor.setAnomalyGrade(grade);
        descriptor.setAttribution(attributionVec);
        descriptor.setTotalUpdates(numSamples);
        descriptor.setRelativeIndex(relativeIndex);
        descriptor.setRelevantAttribution(currentTimeAttribution);
        descriptor.setPastValues(pastalues);
        descriptor.setExpectedValuesList(expectedValuesList);
        descriptor.setLikelihoodOfValues(likelihood);
        descriptor.setThreshold(threshold);

        when(rForest.process(any(), anyLong())).thenReturn(descriptor);

        ActionListener<ThresholdingResult> listener = mock(ActionListener.class);
        modelManager.getTRcfResult(detectorId, rcfModelId, point, listener);

        ThresholdingResult expected = new ThresholdingResult(
            grade,
            confidence,
            score,
            numSamples,
            relativeIndex,
            currentTimeAttribution,
            pastalues,
            expectedValuesList,
            likelihood,
            threshold,
            numTrees
        );
        verify(listener).onResponse(eq(expected));

        descriptor.setTotalUpdates(numSamples + 1L);
        listener = mock(ActionListener.class);
        modelManager.getTRcfResult(detectorId, rcfModelId, point, listener);

        ArgumentCaptor<ThresholdingResult> responseCaptor = ArgumentCaptor.forClass(ThresholdingResult.class);
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

        ActionListener<ThresholdingResult> listener = mock(ActionListener.class);
        modelManager.getTRcfResult(detectorId, rcfModelId, new double[0], listener);

        verify(listener).onFailure(any(ResourceNotFoundException.class));
    }

    @Test
    public void getRcfResult_throwToListener_whenHeapLimitExceed() {
        ThresholdedRandomCutForest rcf = ThresholdedRandomCutForest
            .builder()
            .dimensions(numFeatures)
            .sampleSize(numSamples)
            .numberOfTrees(numTrees)
            .build();

        doAnswer(invocation -> {
            ActionListener<Optional<ThresholdedRandomCutForest>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(rcf));
            return null;
        }).when(checkpointDao).getTRCFModel(eq(rcfModelId), any(ActionListener.class));

        when(jvmService.info().getMem().getHeapMax().getBytes()).thenReturn(1_000L);

        MemoryTracker memoryTracker = new MemoryTracker(jvmService, modelMaxSizePercentage, null, adCircuitBreakerService);

        ActionListener<ThresholdingResult> listener = mock(ActionListener.class);

        // use new memoryTracker
        modelManager = spy(
            new ADModelManager(
                checkpointDao,
                clock,
                numTrees,
                numSamples,
                numMinSamples,
                thresholdMinPvalue,
                minPreviewSize,
                modelTtl,
                AnomalyDetectorSettings.AD_CHECKPOINT_SAVING_FREQ,
                entityColdStarter,
                featureManager,
                memoryTracker,
                settings,
                null
            )
        );

        modelManager.getTRcfResult(detectorId, rcfModelId, new double[0], listener);

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

        modelManager.getTRcfResult(detectorId, rcfModelId, new double[0], rcfResultListener);
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
        modelManager.getTRcfResult(detectorId, rcfModelId, new double[0], rcfResultListener);
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
        modelManager.getTRcfResult(detectorId, rcfModelId, new double[0], rcfResultListener);
        modelManager.getTRcfResult(otherModelId, otherModelId, new double[0], rcfResultListener);
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
            listener.onResponse(Optional.of(trcf));
            return null;
        }).when(checkpointDao).getTRCFModel(eq(rcfModelId), any(ActionListener.class));
        modelManager.getTRcfResult(detectorId, rcfModelId, new double[0], rcfResultListener);
        doAnswer(invocation -> {
            ActionListener<Void> listener = invocation.getArgument(1);
            listener.onFailure(new RuntimeException());
            return null;
        }).when(checkpointDao).deleteModelCheckpoint(eq(rcfModelId), any(ActionListener.class));

        ActionListener<Void> listener = mock(ActionListener.class);
        modelManager.clear(detectorId, listener);

        verify(listener).onFailure(any(Exception.class));
    }

    private Object[] trainModelIllegalArgumentData() {
        return new Object[] { new Object[] { new double[][] {} }, new Object[] { new double[][] { {} } } };
    }

    @Test
    public void getRcfModelId_returnNonEmptyString() {
        String rcfModelId = SingleStreamModelIdMapper.getRcfModelId(anomalyDetector.getId(), 0);

        assertFalse(rcfModelId.isEmpty());
    }

    @Test
    public void getThresholdModelId_returnNonEmptyString() {
        String thresholdModelId = SingleStreamModelIdMapper.getThresholdModelId(anomalyDetector.getId());

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
        ActionListener<ThresholdingResult> scoreListener = mock(ActionListener.class);
        modelManager.getTRcfResult(detectorId, successModelId, point, scoreListener);
        modelManager.getTRcfResult(detectorId, failModelId, point, scoreListener);

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
        ActionListener<ThresholdingResult> scoreListener = mock(ActionListener.class);
        modelManager.getTRcfResult(detectorId, rcfModelId, point, scoreListener);

        ActionListener<Void> listener = mock(ActionListener.class);
        modelManager.maintenance(listener);
        verify(listener).onResponse(eq(null));

        modelManager.getTRcfResult(detectorId, rcfModelId, point, scoreListener);
        verify(checkpointDao, times(2)).getTRCFModel(eq(rcfModelId), any(ActionListener.class));
    }

    @Test
    public void maintenance_returnExpectedToListener_doNothing() {
        double[] point = new double[0];

        doAnswer(invocation -> {
            ActionListener<Optional<ThresholdedRandomCutForest>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(trcf));
            return null;
        }).when(checkpointDao).getTRCFModel(eq(rcfModelId), any(ActionListener.class));
        doAnswer(invocation -> {
            ActionListener<Void> listener = invocation.getArgument(2);
            listener.onResponse(null);
            return null;
        }).when(checkpointDao).putTRCFCheckpoint(eq(rcfModelId), eq(trcf), any(ActionListener.class));
        when(clock.instant()).thenReturn(Instant.MIN);
        ActionListener<ThresholdingResult> scoreListener = mock(ActionListener.class);
        modelManager.getTRcfResult(detectorId, rcfModelId, point, scoreListener);
        ActionListener<Void> listener = mock(ActionListener.class);
        modelManager.maintenance(listener);
        verify(listener).onResponse(eq(null));

        listener = mock(ActionListener.class);
        modelManager.maintenance(listener);
        verify(listener).onResponse(eq(null));

        modelManager.getTRcfResult(detectorId, rcfModelId, point, scoreListener);
        verify(checkpointDao, times(1)).getTRCFModel(eq(rcfModelId), any(ActionListener.class));
    }

    @Test
    public void getPreviewResults_returnNoAnomalies_forNoAnomalies() {
        int numPoints = 1000;
        double[][] points = Stream.generate(() -> new double[] { 0 }).limit(numPoints).toArray(double[][]::new);
        List<Entry<Long, Long>> timeRanges = IntStream
            .rangeClosed(1, points.length) // Start at 1, go up to points.length (inclusive)
            .mapToObj(i -> new SimpleEntry<Long, Long>((long) i, (long) i + 1))
            .collect(Collectors.toList());
        Features features = new Features(timeRanges, points);

        List<ThresholdingResult> results = modelManager.getPreviewResults(features, shingleSize, 0.0001);

        assertEquals(numPoints, results.size());
        assertTrue(results.stream().noneMatch(r -> r.getGrade() > 0));
    }

    @Test
    public void getPreviewResults_returnAnomalies_forLastAnomaly() {
        int numPoints = 1000;
        double[][] points = Stream.generate(() -> new double[] { 0 }).limit(numPoints).toArray(double[][]::new);
        points[points.length - 1] = new double[] { 1. };
        List<Entry<Long, Long>> timeRanges = IntStream
            .rangeClosed(1, points.length) // Start at 1, go up to points.length (inclusive)
            .mapToObj(i -> new SimpleEntry<Long, Long>((long) i, (long) i + 1))
            .collect(Collectors.toList());
        Features features = new Features(timeRanges, points);

        List<ThresholdingResult> results = modelManager.getPreviewResults(features, shingleSize, 0.0001);

        assertEquals(numPoints, results.size());
        assertTrue(results.stream().limit(numPoints - 1).noneMatch(r -> r.getGrade() > 0));
        assertTrue(results.get(numPoints - 1).getGrade() > 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getPreviewResults_throwIllegalArgument_forInvalidInput() {
        Features features = new Features(new ArrayList<Entry<Long, Long>>(), new double[0][0]);
        modelManager.getPreviewResults(features, shingleSize, 0.0001);
    }

    @Test
    public void getNullState() {
        assertEquals(
            new ThresholdingResult(0, 0, 0),
            modelManager.getResult(new Sample(new double[] {}, Instant.now(), Instant.now()), null, "", anomalyDetector, "")
        );
    }

    @Test
    public void getEmptyStateFullSamples() {
        SearchFeatureDao searchFeatureDao = mock(SearchFeatureDao.class);

        LinearUniformImputer interpolator = new LinearUniformImputer(true);

        NodeStateManager stateManager = mock(NodeStateManager.class);
        featureManager = new FeatureManager(
            searchFeatureDao,
            interpolator,
            TimeSeriesSettings.TRAIN_SAMPLE_TIME_RANGE_IN_HOURS,
            TimeSeriesSettings.MIN_TRAIN_SAMPLES,
            AnomalyDetectorSettings.MAX_SHINGLE_PROPORTION_MISSING,
            AnomalyDetectorSettings.MAX_IMPUTATION_NEIGHBOR_DISTANCE,
            AnomalyDetectorSettings.PREVIEW_SAMPLE_RATE,
            AnomalyDetectorSettings.MAX_PREVIEW_SAMPLES,
            threadPool
        );

        ADCheckpointWriteWorker checkpointWriteQueue = mock(ADCheckpointWriteWorker.class);

        entityColdStarter = new ADColdStart(
            clock,
            threadPool,
            stateManager,
            TimeSeriesSettings.NUM_SAMPLES_PER_TREE,
            TimeSeriesSettings.NUM_TREES,
            numMinSamples,
            AnomalyDetectorSettings.MAX_SAMPLE_STRIDE,
            AnomalyDetectorSettings.MAX_TRAIN_SAMPLE,
            searchFeatureDao,
            TimeSeriesSettings.THRESHOLD_MIN_PVALUE,
            featureManager,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            checkpointWriteQueue,
            TimeSeriesSettings.MAX_COLD_START_ROUNDS,
            1
        );

        modelManager = spy(
            new ADModelManager(
                checkpointDao,
                clock,
                numTrees,
                numSamples,
                numMinSamples,
                thresholdMinPvalue,
                minPreviewSize,
                modelTtl,
                AnomalyDetectorSettings.AD_CHECKPOINT_SAVING_FREQ,
                entityColdStarter,
                featureManager,
                memoryTracker,
                settings,
                clusterService
            )
        );

        ModelState<ThresholdedRandomCutForest> state = MLUtil
            .randomModelState(new RandomModelStateConfig.Builder().fullModel(false).sampleSize(numMinSamples).build());
        Optional<ThresholdedRandomCutForest> model = state.getModel();
        assertTrue(model.isEmpty());
        ThresholdingResult result = modelManager
            .getResult(new Sample(new double[] { -1 }, Instant.now(), Instant.now()), state, "", anomalyDetector, "");
        // model outputs scores
        assertTrue(result.getRcfScore() != 0);
        // added the sample to score since our model is empty
        assertEquals(0, state.getSamples().size());
    }

    @Test
    public void getAnomalyResultForEntityNoModel() {
        ModelState<ThresholdedRandomCutForest> modelState = new ModelState<>(
            null,
            modelId,
            detectorId,
            ModelManager.ModelType.TRCF.getName(),
            clock
        );
        ThresholdingResult result = modelManager
            .getResult(new Sample(new double[] { -1 }, Instant.now(), Instant.now()), modelState, modelId, anomalyDetector, "");
        // model outputs scores
        assertEquals(new ThresholdingResult(0, 0, 0), result);
        // added the sample to score since our model is empty
        assertEquals(1, modelState.getSamples().size());
    }

    @Test
    public void getEmptyStateNotFullSamples() {
        ModelState<ThresholdedRandomCutForest> state = MLUtil
            .randomModelState(new RandomModelStateConfig.Builder().fullModel(false).sampleSize(numMinSamples - 1).build());
        assertEquals(
            new ThresholdingResult(0, 0, 0),
            modelManager.getResult(new Sample(new double[] { -1 }, Instant.now(), Instant.now()), state, "", anomalyDetector, "")
        );
        assertEquals(numMinSamples, state.getSamples().size());
    }

    @Test
    public void scoreSamples() {
        ModelState<ThresholdedRandomCutForest> state = MLUtil
            .randomModelState(new RandomModelStateConfig.Builder().fullModel(true).build());
        modelManager.getResult(new Sample(new double[] { -1 }, Instant.now(), Instant.now()), state, "", anomalyDetector, "");
        assertEquals(0, state.getSamples().size());
        assertEquals(now, state.getLastUsedTime());
    }

    public void getAnomalyResultForEntity_withTrcf() {
        AnomalyDescriptor anomalyDescriptor = new AnomalyDescriptor(point, 0);
        anomalyDescriptor.setRCFScore(2);
        anomalyDescriptor.setDataConfidence(1);
        anomalyDescriptor.setAnomalyGrade(1);
        when(this.trcf.process(this.point, 0)).thenReturn(anomalyDescriptor);

        ThresholdingResult result = modelManager
            .getResult(new Sample(this.point, Instant.now(), Instant.now()), this.modelState, this.detectorId, anomalyDetector, "");
        assertEquals(
            new ThresholdingResult(
                anomalyDescriptor.getAnomalyGrade(),
                anomalyDescriptor.getDataConfidence(),
                anomalyDescriptor.getRCFScore()
            ),
            result
        );
    }

    @Test
    public void score_with_trcf() {
        RandomCutForest rcf = mock(RandomCutForest.class);
        when(rcf.getShingleSize()).thenReturn(8);
        when(rcf.getDimensions()).thenReturn(40);
        when(this.trcf.getForest()).thenReturn(rcf);
        when(this.modelState.getSamples())
            .thenReturn(new ArrayDeque<>(Arrays.asList(new Sample(this.point, Instant.now(), Instant.now()))));

        ThresholdingResult result = modelManager
            .score(new Sample(this.point, Instant.now(), Instant.now()), this.modelId, this.modelState, anomalyDetector);
        assertEquals(
            new ThresholdingResult(
                descriptor.getAnomalyGrade(),
                descriptor.getDataConfidence(),
                descriptor.getRCFScore(),
                descriptor.getTotalUpdates(),
                descriptor.getRelativeIndex(),
                descriptor.getRelevantAttribution(),
                descriptor.getPastValues(),
                descriptor.getExpectedValuesList(),
                descriptor.getLikelihoodOfValues(),
                descriptor.getThreshold(),
                numTrees
            ),
            result
        );
    }

    @Test(expected = IllegalArgumentException.class)
    public void score_throw() {
        AnomalyDescriptor anomalyDescriptor = new AnomalyDescriptor(point, 0);
        anomalyDescriptor.setRCFScore(2);
        anomalyDescriptor.setAnomalyGrade(1);
        // input dimension is 5
        anomalyDescriptor.setRelevantAttribution(new double[] { 0, 0, 0, 0, 0 });
        RandomCutForest rcf = mock(RandomCutForest.class);
        when(rcf.getShingleSize()).thenReturn(8);
        when(rcf.getDimensions()).thenReturn(40);
        when(this.trcf.getForest()).thenReturn(rcf);
        doThrow(new IllegalArgumentException()).when(trcf).process(any(), anyLong());
        when(this.modelState.getSamples())
            .thenReturn(new ArrayDeque<>(Arrays.asList(new Sample(this.point, Instant.now(), Instant.now()))));
        modelManager.score(new Sample(this.point, Instant.now(), Instant.now()), this.modelId, this.modelState, anomalyDetector);
    }
}
