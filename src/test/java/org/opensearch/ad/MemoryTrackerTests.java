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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.opensearch.ad.breaker.ADCircuitBreakerService;
import org.opensearch.ad.common.exception.LimitExceededException;
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

import com.amazon.randomcutforest.RandomCutForest;
import com.amazon.randomcutforest.config.Precision;

public class MemoryTrackerTests extends OpenSearchTestCase {

    int rcfNumFeatures;
    int rcfSampleSize;
    int numberOfTrees;
    double rcfTimeDecay;
    int numMinSamples;
    MemoryTracker tracker;
    long expectedRCFModelSize;
    String detectorId;
    long largeHeapSize;
    long smallHeapSize;
    Mem mem;
    RandomCutForest rcf;
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
        rcfNumFeatures = 1;
        rcfSampleSize = 256;
        numberOfTrees = 10;
        rcfTimeDecay = 0.2;
        numMinSamples = 128;

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

        expectedRCFModelSize = 118144;
        detectorId = "123";

        rcf = RandomCutForest
            .builder()
            .dimensions(rcfNumFeatures)
            .sampleSize(rcfSampleSize)
            .numberOfTrees(numberOfTrees)
            .timeDecay(rcfTimeDecay)
            .outputAfter(numMinSamples)
            .parallelExecutionEnabled(false)
            .compact(true)
            .precision(Precision.FLOAT_32)
            .boundingBoxCacheFraction(AnomalyDetectorSettings.REAL_TIME_BOUNDING_BOX_CACHE_RATIO)
            // same with dimension for opportunistic memory saving
            .shingleSize(rcfNumFeatures)
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

        assertEquals(expectedRCFModelSize + tracker.getThresholdModelBytes(), tracker.estimateTotalModelSize(rcf));
        assertTrue(tracker.isHostingAllowed(detectorId, rcf));

        assertEquals(
            expectedRCFModelSize + tracker.getThresholdModelBytes(),
            tracker.estimateTotalModelSize(detector, numberOfTrees, AnomalyDetectorSettings.REAL_TIME_BOUNDING_BOX_CACHE_RATIO)
        );
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
        expectThrows(LimitExceededException.class, () -> tracker.isHostingAllowed(detectorId, rcf));
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
