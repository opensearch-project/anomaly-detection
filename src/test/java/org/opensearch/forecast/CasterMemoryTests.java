/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast;

import org.opensearch.timeseries.AbstractMemoryTrackerTest;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

import com.amazon.randomcutforest.config.ForestMode;
import com.amazon.randomcutforest.config.Precision;
import com.amazon.randomcutforest.config.TransformMethod;
import com.amazon.randomcutforest.parkservices.RCFCaster;

public class CasterMemoryTests extends AbstractMemoryTrackerTest {
    RCFCaster caster;
    int forecastHorizon;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        forecastHorizon = shingleSize * 3;
        caster = RCFCaster
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
            .forecastHorizon(forecastHorizon)
            .forestMode(ForestMode.STANDARD)
            .build();
    }

    public void testEstimateModelSize() {
        setUpBigHeap();

        assertEquals(416216, tracker.estimateCasterModelSize(caster));
        assertTrue(tracker.isHostingAllowed(configId, caster));

        inputFeatures = 2;
        shingleSize = 4;
        dimension = inputFeatures * shingleSize;
        forecastHorizon = 3 * shingleSize;
        RCFCaster caster2 = RCFCaster
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
            .forecastHorizon(forecastHorizon)
            .forestMode(ForestMode.STANDARD)
            .build();
        assertEquals(423324, tracker.estimateCasterModelSize(caster2));
        assertTrue(tracker.isHostingAllowed(configId, caster2));

        inputFeatures = 2;
        shingleSize = 32;
        dimension = inputFeatures * shingleSize;
        forecastHorizon = 3 * shingleSize;
        RCFCaster caster3 = RCFCaster
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
            .forecastHorizon(forecastHorizon)
            .forestMode(ForestMode.STANDARD)
            .build();
        assertEquals(824748, tracker.estimateCasterModelSize(caster3));
        assertTrue(tracker.isHostingAllowed(configId, caster3));

        inputFeatures = 2;
        shingleSize = 16;
        dimension = inputFeatures * shingleSize;
        forecastHorizon = 3 * shingleSize;
        RCFCaster caster4 = RCFCaster
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
            .boundingBoxCacheFraction(TimeSeriesSettings.BATCH_BOUNDING_BOX_CACHE_RATIO)
            .shingleSize(shingleSize)
            .internalShinglingEnabled(true)
            .transformMethod(TransformMethod.NORMALIZE)
            .forecastHorizon(forecastHorizon)
            .forestMode(ForestMode.STANDARD)
            .build();
        assertEquals(4000812, tracker.estimateCasterModelSize(caster4));
        assertTrue(tracker.isHostingAllowed(configId, caster4));

        inputFeatures = 8;
        shingleSize = 4;
        dimension = inputFeatures * shingleSize;
        forecastHorizon = 3 * shingleSize;
        RCFCaster caster5 = RCFCaster
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
            .boundingBoxCacheFraction(TimeSeriesSettings.BATCH_BOUNDING_BOX_CACHE_RATIO)
            .shingleSize(shingleSize)
            .internalShinglingEnabled(true)
            .transformMethod(TransformMethod.NORMALIZE)
            .forecastHorizon(forecastHorizon)
            .forestMode(ForestMode.STANDARD)
            .build();
        assertEquals(4161556, tracker.estimateCasterModelSize(caster5));
        assertTrue(tracker.isHostingAllowed(configId, caster5));

        inputFeatures = 8;
        shingleSize = 8;
        dimension = inputFeatures * shingleSize;
        forecastHorizon = 3 * shingleSize;
        RCFCaster caster6 = RCFCaster
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
            .boundingBoxCacheFraction(TimeSeriesSettings.BATCH_BOUNDING_BOX_CACHE_RATIO)
            .shingleSize(shingleSize)
            .internalShinglingEnabled(true)
            .transformMethod(TransformMethod.NORMALIZE)
            .forecastHorizon(forecastHorizon)
            .forestMode(ForestMode.STANDARD)
            .build();
        assertEquals(7672708, tracker.estimateCasterModelSize(caster6));
        assertTrue(tracker.isHostingAllowed(configId, caster6));

        inputFeatures = 8;
        shingleSize = 16;
        dimension = inputFeatures * shingleSize;
        forecastHorizon = 3 * shingleSize;
        RCFCaster caster7 = RCFCaster
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
            .boundingBoxCacheFraction(TimeSeriesSettings.BATCH_BOUNDING_BOX_CACHE_RATIO)
            .shingleSize(shingleSize)
            .internalShinglingEnabled(true)
            .transformMethod(TransformMethod.NORMALIZE)
            .forecastHorizon(forecastHorizon)
            .forestMode(ForestMode.STANDARD)
            .build();
        assertEquals(14693988, tracker.estimateCasterModelSize(caster7));
        assertTrue(tracker.isHostingAllowed(configId, caster7));

        inputFeatures = 8;
        shingleSize = 129;
        dimension = inputFeatures * shingleSize;
        forecastHorizon = 3 * shingleSize;
        RCFCaster caster8 = RCFCaster
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
            .boundingBoxCacheFraction(TimeSeriesSettings.BATCH_BOUNDING_BOX_CACHE_RATIO)
            .shingleSize(129)
            .internalShinglingEnabled(true)
            .transformMethod(TransformMethod.NORMALIZE)
            .forecastHorizon(forecastHorizon)
            .forestMode(ForestMode.STANDARD)
            .build();
        expectThrows(IllegalArgumentException.class, () -> tracker.estimateTRCFModelSize(caster8));
    }
}
