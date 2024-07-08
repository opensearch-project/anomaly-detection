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

import java.util.Collections;

import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.timeseries.AbstractMemoryTrackerTest;
import org.opensearch.timeseries.MemoryTracker;
import org.opensearch.timeseries.common.exception.LimitExceededException;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

import com.amazon.randomcutforest.config.Precision;
import com.amazon.randomcutforest.config.TransformMethod;
import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

public class TRCFMemoryTests extends AbstractMemoryTrackerTest {
    long expectedRCFModelSize;
    ThresholdedRandomCutForest trcf;
    AnomalyDetector detector;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        expectedRCFModelSize = 382784;

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
            .transformMethod(TransformMethod.NORMALIZE)
            .alertOnce(true)
            .autoAdjust(true)
            .build();

        detector = mock(AnomalyDetector.class);
        when(detector.getEnabledFeatureIds()).thenReturn(Collections.singletonList("a"));
        when(detector.getShingleSize()).thenReturn(1);
    }

    private void setUpSmallHeap() {
        ByteSizeValue value = new ByteSizeValue(smallHeapSize);
        when(mem.getHeapMax()).thenReturn(value);
        tracker = new MemoryTracker(jvmService, modelMaxSizePercentage, clusterService, circuitBreaker);
    }

    public void testEstimateModelSize() {
        setUpBigHeap();

        assertEquals(400768, tracker.estimateTRCFModelSize(trcf));
        assertTrue(tracker.isHostingAllowed(configId, trcf));

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
            .transformMethod(TransformMethod.NORMALIZE)
            .alertOnce(true)
            .autoAdjust(true)
            .build();
        assertEquals(623944, tracker.estimateTRCFModelSize(rcf2));
        assertTrue(tracker.isHostingAllowed(configId, rcf2));

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
            .boundingBoxCacheFraction(TimeSeriesSettings.BATCH_BOUNDING_BOX_CACHE_RATIO)
            .internalShinglingEnabled(false)
            // same with dimension for opportunistic memory saving
            .shingleSize(1)
            .transformMethod(TransformMethod.NORMALIZE)
            .alertOnce(true)
            .autoAdjust(true)
            .build();
        assertEquals(1789092, tracker.estimateTRCFModelSize(rcf3));

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
            .transformMethod(TransformMethod.NORMALIZE)
            .alertOnce(true)
            .autoAdjust(true)
            .build();
        assertEquals(609244, tracker.estimateTRCFModelSize(rcf4));

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
            .transformMethod(TransformMethod.NORMALIZE)
            .alertOnce(true)
            .autoAdjust(true)
            .build();
        assertEquals(518960, tracker.estimateTRCFModelSize(rcf5));

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
            .transformMethod(TransformMethod.NORMALIZE)
            .alertOnce(true)
            .autoAdjust(true)
            .build();
        assertEquals(746392, tracker.estimateTRCFModelSize(rcf6));

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
            .transformMethod(TransformMethod.NORMALIZE)
            .alertOnce(true)
            .autoAdjust(true)
            .build();
        assertEquals(434080, tracker.estimateTRCFModelSize(rcf7));

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
            .transformMethod(TransformMethod.NORMALIZE)
            .alertOnce(true)
            .autoAdjust(true)
            .build();
        assertEquals(1571852, tracker.estimateTRCFModelSize(rcf8));

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
            .transformMethod(TransformMethod.NORMALIZE)
            .alertOnce(true)
            .autoAdjust(true)
            .build();
        assertEquals(1243540, tracker.estimateTRCFModelSize(rcf9));

        ThresholdedRandomCutForest rcf10 = ThresholdedRandomCutForest
            .builder()
            .dimensions(387)
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
            .shingleSize(129)
            .transformMethod(TransformMethod.NORMALIZE)
            .alertOnce(true)
            .autoAdjust(true)
            .build();
        expectThrows(IllegalArgumentException.class, () -> tracker.estimateTRCFModelSize(rcf10));
    }

    public void testCanAllocate() {
        setUpBigHeap();

        assertTrue(tracker.canAllocate((long) (largeHeapSize * modelMaxPercen)));
        assertTrue(!tracker.canAllocate((long) (largeHeapSize * modelMaxPercen + 10)));

        long bytesToUse = 100_000;
        tracker.consumeMemory(bytesToUse, false, MemoryTracker.Origin.REAL_TIME_DETECTOR);
        assertTrue(!tracker.canAllocate((long) (largeHeapSize * modelMaxPercen)));

        tracker.releaseMemory(bytesToUse, false, MemoryTracker.Origin.REAL_TIME_DETECTOR);
        assertTrue(tracker.canAllocate((long) (largeHeapSize * modelMaxPercen)));
    }

    public void testCannotHost() {
        setUpSmallHeap();
        expectThrows(LimitExceededException.class, () -> tracker.isHostingAllowed(configId, trcf));
    }

    public void testMemoryToShed() {
        setUpSmallHeap();
        long bytesToUse = 100_000;
        assertEquals(bytesToUse, tracker.getHeapLimit());
        tracker.consumeMemory(bytesToUse, false, MemoryTracker.Origin.REAL_TIME_DETECTOR);
        tracker.consumeMemory(bytesToUse, true, MemoryTracker.Origin.REAL_TIME_DETECTOR);
        assertEquals(2 * bytesToUse, tracker.getTotalMemoryBytes());

        assertEquals(bytesToUse, tracker.memoryToShed());
        assertTrue(!tracker.syncMemoryState(MemoryTracker.Origin.REAL_TIME_DETECTOR, 2 * bytesToUse, bytesToUse));
    }
}
