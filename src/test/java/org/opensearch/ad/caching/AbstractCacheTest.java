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
import java.util.Random;

import org.junit.Before;
import org.opensearch.ad.ml.EntityModel;
import org.opensearch.ad.ml.ModelManager.ModelType;
import org.opensearch.ad.ml.ModelState;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.ratelimit.CheckpointMaintainWorker;
import org.opensearch.ad.ratelimit.CheckpointWriteWorker;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.MemoryTracker;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

public class AbstractCacheTest extends AbstractTimeSeriesTest {
    protected String modelId1, modelId2, modelId3, modelId4;
    protected Entity entity1, entity2, entity3, entity4;
    protected ModelState<EntityModel> modelState1, modelState2, modelState3, modelState4;
    protected String detectorId;
    protected AnomalyDetector detector;
    protected Clock clock;
    protected Duration detectorDuration;
    protected float initialPriority;
    protected CacheBuffer cacheBuffer;
    protected long memoryPerEntity;
    protected MemoryTracker memoryTracker;
    protected CheckpointWriteWorker checkpointWriteQueue;
    protected CheckpointMaintainWorker checkpointMaintainQueue;
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

        checkpointWriteQueue = mock(CheckpointWriteWorker.class);
        checkpointMaintainQueue = mock(CheckpointMaintainWorker.class);

        cacheBuffer = new CacheBuffer(
            1,
            1,
            memoryPerEntity,
            memoryTracker,
            clock,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            detectorId,
            checkpointWriteQueue,
            checkpointMaintainQueue,
            Duration.ofHours(12).toHoursPart()
        );

        initialPriority = cacheBuffer.getPriorityTracker().getUpdatedPriority(0);

        modelState1 = new ModelState<>(
            new EntityModel(entity1, new ArrayDeque<>(), null),
            modelId1,
            detectorId,
            ModelType.ENTITY.getName(),
            clock,
            0
        );

        modelState2 = new ModelState<>(
            new EntityModel(entity2, new ArrayDeque<>(), null),
            modelId2,
            detectorId,
            ModelType.ENTITY.getName(),
            clock,
            0
        );

        modelState3 = new ModelState<>(
            new EntityModel(entity3, new ArrayDeque<>(), null),
            modelId3,
            detectorId,
            ModelType.ENTITY.getName(),
            clock,
            0
        );

        modelState4 = new ModelState<>(
            new EntityModel(entity4, new ArrayDeque<>(), null),
            modelId4,
            detectorId,
            ModelType.ENTITY.getName(),
            clock,
            0
        );
    }
}
