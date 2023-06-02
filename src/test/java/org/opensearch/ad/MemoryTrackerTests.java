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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.opensearch.ad.breaker.ADCircuitBreakerService;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.monitor.jvm.JvmInfo;
import org.opensearch.monitor.jvm.JvmInfo.Mem;
import org.opensearch.monitor.jvm.JvmService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.common.exception.LimitExceededException;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

import com.amazon.randomcutforest.config.Precision;
import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

public class MemoryTrackerTests extends OpenSearchTestCase {

    int inputFeatures;
    int rcfSampleSize;
    int numberOfTrees;
    double rcfTimeDecay;
    int numMinSamples;
    int shingleSize;
    int dimension;
    MemoryTracker tracker;
    long expectedRCFModelSize;
    String detectorId;
    long largeHeapSize;
    long smallHeapSize;
    Mem mem;
    ThresholdedRandomCutForest trcf;
    float modelMaxPercen;
    ClusterService clusterService;
    double modelMaxSizePercentage;
    double modelDesiredSizePercentage;
    JvmService jvmService;
    AnomalyDetector detector;
    ADCircuitBreakerService circuitBreaker;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        inputFeatures = 1;
        rcfSampleSize = 256;
        numberOfTrees = 30;
        rcfTimeDecay = 0.2;
        numMinSamples = 128;
        shingleSize = 8;
        dimension = inputFeatures * shingleSize;

        jvmService = mock(JvmService.class);
        JvmInfo info = mock(JvmInfo.class);
        mem = mock(Mem.class);
        // 800 MB is the limit
        largeHeapSize = 800_000_000;
        smallHeapSize = 1_000_000;

        when(jvmService.info()).thenReturn(info);
        when(info.getMem()).thenReturn(mem);

        modelMaxSizePercentage = 0.1;
        modelDesiredSizePercentage = 0.0002;

        clusterService = mock(ClusterService.class);
        modelMaxPercen = 0.1f;
        Settings settings = Settings.builder().put(AnomalyDetectorSettings.MODEL_MAX_SIZE_PERCENTAGE.getKey(), modelMaxPercen).build();
        ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(AnomalyDetectorSettings.MODEL_MAX_SIZE_PERCENTAGE)))
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        expectedRCFModelSize = 382784;
        detectorId = "123";

        trcf = ThresholdedRandomCutForest
            .builder()
            .dimensions(dimension)
            .sampleSize(rcfSampleSize)
            .numberOfTrees(numberOfTrees)
            .timeDecay(rcfTimeDecay)
            .outputAfter(numMinSamples)
            .initialAcceptFraction(numMinSamples * 1.0d / rcfSampleSize)
            .parallelExecutionEnabled(false)
            .compact(true)
            .precision(Precision.FLOAT_32)
            .boundingBoxCacheFraction(TimeSeriesSettings.REAL_TIME_BOUNDING_BOX_CACHE_RATIO)
            .shingleSize(shingleSize)
            .internalShinglingEnabled(true)
            .build();

        detector = mock(AnomalyDetector.class);
        when(detector.getEnabledFeatureIds()).thenReturn(Collections.singletonList("a"));
        when(detector.getShingleSize()).thenReturn(1);

        circuitBreaker = mock(ADCircuitBreakerService.class);
        when(circuitBreaker.isOpen()).thenReturn(false);
    }

    private void setUpBigHeap() {
        ByteSizeValue value = new ByteSizeValue(largeHeapSize);
        when(mem.getHeapMax()).thenReturn(value);
        tracker = new MemoryTracker(jvmService, modelMaxSizePercentage, modelDesiredSizePercentage, clusterService, circuitBreaker);
    }

    private void setUpSmallHeap() {
        ByteSizeValue value = new ByteSizeValue(smallHeapSize);
        when(mem.getHeapMax()).thenReturn(value);
        tracker = new MemoryTracker(jvmService, modelMaxSizePercentage, modelDesiredSizePercentage, clusterService, circuitBreaker);
    }

    public void testEstimateModelSize() {
        setUpBigHeap();

        assertEquals(403491, tracker.estimateTRCFModelSize(trcf));
        assertTrue(tracker.isHostingAllowed(detectorId, trcf));

        ThresholdedRandomCutForest rcf2 = ThresholdedRandomCutForest
            .builder()
            .dimensions(32) // 32 to trigger another calculation of point store usage
            .sampleSize(rcfSampleSize)
            .numberOfTrees(numberOfTrees)
            .timeDecay(rcfTimeDecay)
            .outputAfter(numMinSamples)
            .initialAcceptFraction(numMinSamples * 1.0d / rcfSampleSize)
            .parallelExecutionEnabled(false)
            .compact(true)
            .precision(Precision.FLOAT_32)
            .boundingBoxCacheFraction(TimeSeriesSettings.REAL_TIME_BOUNDING_BOX_CACHE_RATIO)
            .internalShinglingEnabled(true)
            // same with dimension for opportunistic memory saving
            .shingleSize(shingleSize)
            .build();
        assertEquals(603708, tracker.estimateTRCFModelSize(rcf2));
        assertTrue(tracker.isHostingAllowed(detectorId, rcf2));

        ThresholdedRandomCutForest rcf3 = ThresholdedRandomCutForest
            .builder()
            .dimensions(9)
            .sampleSize(rcfSampleSize)
            .numberOfTrees(numberOfTrees)
            .timeDecay(rcfTimeDecay)
            .outputAfter(numMinSamples)
            .initialAcceptFraction(numMinSamples * 1.0d / rcfSampleSize)
            .parallelExecutionEnabled(false)
            .compact(true)
            .precision(Precision.FLOAT_32)
            .boundingBoxCacheFraction(AnomalyDetectorSettings.BATCH_BOUNDING_BOX_CACHE_RATIO)
            .internalShinglingEnabled(false)
            // same with dimension for opportunistic memory saving
            .shingleSize(1)
            .build();
        assertEquals(1685208, tracker.estimateTRCFModelSize(rcf3));

        ThresholdedRandomCutForest rcf4 = ThresholdedRandomCutForest
            .builder()
            .dimensions(6)
            .sampleSize(rcfSampleSize)
            .numberOfTrees(numberOfTrees)
            .timeDecay(rcfTimeDecay)
            .outputAfter(numMinSamples)
            .parallelExecutionEnabled(false)
            .compact(true)
            .precision(Precision.FLOAT_32)
            .boundingBoxCacheFraction(TimeSeriesSettings.REAL_TIME_BOUNDING_BOX_CACHE_RATIO)
            .internalShinglingEnabled(true)
            // same with dimension for opportunistic memory saving
            .shingleSize(1)
            .build();
        assertEquals(521304, tracker.estimateTRCFModelSize(rcf4));

        ThresholdedRandomCutForest rcf5 = ThresholdedRandomCutForest
            .builder()
            .dimensions(8)
            .sampleSize(rcfSampleSize)
            .numberOfTrees(numberOfTrees)
            .timeDecay(rcfTimeDecay)
            .outputAfter(numMinSamples)
            .parallelExecutionEnabled(false)
            .compact(true)
            .precision(Precision.FLOAT_32)
            .boundingBoxCacheFraction(TimeSeriesSettings.REAL_TIME_BOUNDING_BOX_CACHE_RATIO)
            .internalShinglingEnabled(true)
            // same with dimension for opportunistic memory saving
            .shingleSize(2)
            .build();
        assertEquals(467340, tracker.estimateTRCFModelSize(rcf5));

        ThresholdedRandomCutForest rcf6 = ThresholdedRandomCutForest
            .builder()
            .dimensions(32)
            .sampleSize(rcfSampleSize)
            .numberOfTrees(numberOfTrees)
            .timeDecay(rcfTimeDecay)
            .outputAfter(numMinSamples)
            .parallelExecutionEnabled(false)
            .compact(true)
            .precision(Precision.FLOAT_32)
            .boundingBoxCacheFraction(TimeSeriesSettings.REAL_TIME_BOUNDING_BOX_CACHE_RATIO)
            .internalShinglingEnabled(true)
            // same with dimension for opportunistic memory saving
            .shingleSize(4)
            .build();
        assertEquals(603676, tracker.estimateTRCFModelSize(rcf6));

        ThresholdedRandomCutForest rcf7 = ThresholdedRandomCutForest
            .builder()
            .dimensions(16)
            .sampleSize(rcfSampleSize)
            .numberOfTrees(numberOfTrees)
            .timeDecay(rcfTimeDecay)
            .outputAfter(numMinSamples)
            .parallelExecutionEnabled(false)
            .compact(true)
            .precision(Precision.FLOAT_32)
            .boundingBoxCacheFraction(TimeSeriesSettings.REAL_TIME_BOUNDING_BOX_CACHE_RATIO)
            .internalShinglingEnabled(true)
            // same with dimension for opportunistic memory saving
            .shingleSize(16)
            .build();
        assertEquals(401481, tracker.estimateTRCFModelSize(rcf7));

        ThresholdedRandomCutForest rcf8 = ThresholdedRandomCutForest
            .builder()
            .dimensions(320)
            .sampleSize(rcfSampleSize)
            .numberOfTrees(numberOfTrees)
            .timeDecay(rcfTimeDecay)
            .outputAfter(numMinSamples)
            .parallelExecutionEnabled(false)
            .compact(true)
            .precision(Precision.FLOAT_32)
            .boundingBoxCacheFraction(TimeSeriesSettings.REAL_TIME_BOUNDING_BOX_CACHE_RATIO)
            .internalShinglingEnabled(true)
            // same with dimension for opportunistic memory saving
            .shingleSize(32)
            .build();
        assertEquals(1040432, tracker.estimateTRCFModelSize(rcf8));

        ThresholdedRandomCutForest rcf9 = ThresholdedRandomCutForest
            .builder()
            .dimensions(320)
            .sampleSize(rcfSampleSize)
            .numberOfTrees(numberOfTrees)
            .timeDecay(rcfTimeDecay)
            .outputAfter(numMinSamples)
            .parallelExecutionEnabled(false)
            .compact(true)
            .precision(Precision.FLOAT_32)
            .boundingBoxCacheFraction(TimeSeriesSettings.REAL_TIME_BOUNDING_BOX_CACHE_RATIO)
            .internalShinglingEnabled(true)
            // same with dimension for opportunistic memory saving
            .shingleSize(64)
            .build();
        assertEquals(1040688, tracker.estimateTRCFModelSize(rcf9));

        ThresholdedRandomCutForest rcf10 = ThresholdedRandomCutForest
            .builder()
            .dimensions(325)
            .sampleSize(rcfSampleSize)
            .numberOfTrees(numberOfTrees)
            .timeDecay(rcfTimeDecay)
            .outputAfter(numMinSamples)
            .parallelExecutionEnabled(false)
            .compact(true)
            .precision(Precision.FLOAT_32)
            .boundingBoxCacheFraction(TimeSeriesSettings.REAL_TIME_BOUNDING_BOX_CACHE_RATIO)
            .internalShinglingEnabled(true)
            // same with dimension for opportunistic memory saving
            .shingleSize(65)
            .build();
        expectThrows(IllegalArgumentException.class, () -> tracker.estimateTRCFModelSize(rcf10));
    }

    public void testCanAllocate() {
        setUpBigHeap();

        assertTrue(tracker.canAllocate((long) (largeHeapSize * modelMaxPercen)));
        assertTrue(!tracker.canAllocate((long) (largeHeapSize * modelMaxPercen + 10)));

        long bytesToUse = 100_000;
        tracker.consumeMemory(bytesToUse, false, MemoryTracker.Origin.HC_DETECTOR);
        assertTrue(!tracker.canAllocate((long) (largeHeapSize * modelMaxPercen)));

        tracker.releaseMemory(bytesToUse, false, MemoryTracker.Origin.HC_DETECTOR);
        assertTrue(tracker.canAllocate((long) (largeHeapSize * modelMaxPercen)));
    }

    public void testCannotHost() {
        setUpSmallHeap();
        expectThrows(LimitExceededException.class, () -> tracker.isHostingAllowed(detectorId, trcf));
    }

    public void testMemoryToShed() {
        setUpSmallHeap();
        long bytesToUse = 100_000;
        assertEquals(bytesToUse, tracker.getHeapLimit());
        assertEquals((long) (smallHeapSize * modelDesiredSizePercentage), tracker.getDesiredModelSize());
        tracker.consumeMemory(bytesToUse, false, MemoryTracker.Origin.HC_DETECTOR);
        tracker.consumeMemory(bytesToUse, true, MemoryTracker.Origin.HC_DETECTOR);
        assertEquals(2 * bytesToUse, tracker.getTotalMemoryBytes());

        assertEquals(bytesToUse, tracker.memoryToShed());
        assertTrue(!tracker.syncMemoryState(MemoryTracker.Origin.HC_DETECTOR, 2 * bytesToUse, bytesToUse));
    }
}
