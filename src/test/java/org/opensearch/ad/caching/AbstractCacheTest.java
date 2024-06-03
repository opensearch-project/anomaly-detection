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

package org.opensearch.ad.caching;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Optional;
import java.util.Random;

import org.junit.Before;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.ratelimit.ADCheckpointMaintainWorker;
import org.opensearch.ad.ratelimit.ADCheckpointWriteWorker;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.MemoryTracker;
import org.opensearch.timeseries.caching.PriorityTracker;
import org.opensearch.timeseries.ml.ModelManager;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

import test.org.opensearch.ad.util.MLUtil;

public class AbstractCacheTest extends AbstractTimeSeriesTest {
    protected String modelId1, modelId2, modelId3, modelId4;
    protected Entity entity1, entity2, entity3, entity4;
    protected ModelState<ThresholdedRandomCutForest> modelState1, modelState2, modelState3, modelState4;
    protected String detectorId;
    protected AnomalyDetector detector;
    protected Clock clock;
    protected Duration detectorDuration;
    protected float initialPriority;
    protected ADCacheBuffer cacheBuffer;
    protected long memoryPerEntity;
    protected MemoryTracker memoryTracker;
    protected ADCheckpointWriteWorker checkpointWriteQueue;
    protected ADCheckpointMaintainWorker checkpointMaintainQueue;
    protected Random random;
    protected int shingleSize;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        detector = mock(AnomalyDetector.class);
        detectorId = "123";
        when(detector.getId()).thenReturn(detectorId);
        detectorDuration = Duration.ofMinutes(5);
        when(detector.getIntervalDuration()).thenReturn(detectorDuration);
        when(detector.getIntervalInSeconds()).thenReturn(detectorDuration.getSeconds());
        when(detector.getEnabledFeatureIds()).thenReturn(new ArrayList<String>() {
            {
                add("a");
                add("b");
                add("c");
            }
        });
        shingleSize = 4;
        when(detector.getShingleSize()).thenReturn(shingleSize);

        entity1 = Entity.createSingleAttributeEntity("attributeName1", "attributeVal1");
        entity2 = Entity.createSingleAttributeEntity("attributeName1", "attributeVal2");
        entity3 = Entity.createSingleAttributeEntity("attributeName1", "attributeVal3");
        entity4 = Entity.createSingleAttributeEntity("attributeName1", "attributeVal4");
        modelId1 = entity1.getModelId(detectorId).get();
        modelId2 = entity2.getModelId(detectorId).get();
        modelId3 = entity3.getModelId(detectorId).get();
        modelId4 = entity4.getModelId(detectorId).get();

        clock = mock(Clock.class);
        when(clock.instant()).thenReturn(Instant.now());

        memoryPerEntity = 81920;
        memoryTracker = mock(MemoryTracker.class);

        checkpointWriteQueue = mock(ADCheckpointWriteWorker.class);
        checkpointMaintainQueue = mock(ADCheckpointMaintainWorker.class);

        PriorityTracker tracker = new PriorityTracker(
            clock,
            detectorDuration.getSeconds(),
            clock.instant().getEpochSecond(),
            TimeSeriesSettings.MAX_TRACKING_ENTITIES
        );
        cacheBuffer = new ADCacheBuffer(
            1,
            clock,
            memoryTracker,
            Duration.ofHours(12).toHoursPart(),
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            memoryPerEntity,
            checkpointWriteQueue,
            checkpointMaintainQueue,
            detectorId,
            tracker
        );

        initialPriority = cacheBuffer.getPriorityTracker().getUpdatedPriority(0);

        modelState1 = new ModelState<ThresholdedRandomCutForest>(
            MLUtil.createNonEmptyModel(detectorId, 0, entity1).getLeft(),
            modelId1,
            detectorId,
            ModelManager.ModelType.TRCF.getName(),
            clock,
            0,
            Optional.of(entity1),
            new ArrayDeque<>()
        );

        modelState2 = new ModelState<ThresholdedRandomCutForest>(
            MLUtil.createNonEmptyModel(detectorId, 0, entity2).getLeft(),
            modelId2,
            detectorId,
            ModelManager.ModelType.TRCF.getName(),
            clock,
            0,
            Optional.of(entity2),
            new ArrayDeque<>()
        );

        modelState3 = new ModelState<ThresholdedRandomCutForest>(
            MLUtil.createNonEmptyModel(detectorId, 0, entity3).getLeft(),
            modelId3,
            detectorId,
            ModelManager.ModelType.TRCF.getName(),
            clock,
            0,
            Optional.of(entity3),
            new ArrayDeque<>()
        );

        modelState4 = new ModelState<ThresholdedRandomCutForest>(
            MLUtil.createNonEmptyModel(detectorId, 0, entity4).getLeft(),
            modelId4,
            detectorId,
            ModelManager.ModelType.TRCF.getName(),
            clock,
            0,
            Optional.of(entity4),
            new ArrayDeque<>()
        );
    }
}
