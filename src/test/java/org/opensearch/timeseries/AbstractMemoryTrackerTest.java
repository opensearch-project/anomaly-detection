/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.monitor.jvm.JvmInfo;
import org.opensearch.monitor.jvm.JvmInfo.Mem;
import org.opensearch.monitor.jvm.JvmService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.breaker.CircuitBreakerService;

public class AbstractMemoryTrackerTest extends OpenSearchTestCase {
    protected long largeHeapSize;
    protected long smallHeapSize;
    protected Mem mem;
    protected MemoryTracker tracker;
    protected JvmService jvmService;
    protected double modelMaxSizePercentage;
    protected double modelDesiredSizePercentage;
    protected ClusterService clusterService;
    protected float modelMaxPercen;
    protected CircuitBreakerService circuitBreaker;
    protected int inputFeatures;
    protected int rcfSampleSize;
    protected int numberOfTrees;
    protected double rcfTimeDecay;
    protected int numMinSamples;
    protected int shingleSize;
    protected int dimension;
    protected String configId;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mem = mock(Mem.class);
        // 800 MB is the limit
        largeHeapSize = 800_000_000;
        smallHeapSize = 1_000_000;

        jvmService = mock(JvmService.class);
        JvmInfo info = mock(JvmInfo.class);

        when(jvmService.info()).thenReturn(info);
        when(info.getMem()).thenReturn(mem);

        modelMaxSizePercentage = 0.1;
        modelDesiredSizePercentage = 0.0002;

        clusterService = mock(ClusterService.class);
        Settings settings = Settings.builder().put(AnomalyDetectorSettings.AD_MODEL_MAX_SIZE_PERCENTAGE.getKey(), modelMaxPercen).build();
        ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(AnomalyDetectorSettings.AD_MODEL_MAX_SIZE_PERCENTAGE)))
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        modelMaxPercen = 0.1f;
        circuitBreaker = mock(CircuitBreakerService.class);
        when(circuitBreaker.isOpen()).thenReturn(false);

        inputFeatures = 1;
        rcfSampleSize = 256;
        numberOfTrees = 50;
        rcfTimeDecay = 0.2;
        numMinSamples = 128;
        shingleSize = 8;
        dimension = inputFeatures * shingleSize;
        configId = "123";
    }

    protected void setUpBigHeap() {
        ByteSizeValue value = new ByteSizeValue(largeHeapSize);
        when(mem.getHeapMax()).thenReturn(value);
        tracker = new MemoryTracker(jvmService, modelMaxSizePercentage, clusterService, circuitBreaker);
    }
}
