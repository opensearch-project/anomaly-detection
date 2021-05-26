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

package org.opensearch.ad.caching;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map.Entry;

import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.opensearch.ad.MemoryTracker;
import org.opensearch.ad.ml.CheckpointDao;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.test.OpenSearchTestCase;

import test.org.opensearch.ad.util.MLUtil;

public class CacheBufferTests extends OpenSearchTestCase {
    CacheBuffer cacheBuffer;
    CheckpointDao checkpointDao;
    MemoryTracker memoryTracker;
    Clock clock;
    String detectorId;
    float initialPriority;
    long memoryPerEntity;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        checkpointDao = mock(CheckpointDao.class);
        memoryTracker = mock(MemoryTracker.class);
        clock = mock(Clock.class);
        when(clock.instant()).thenReturn(Instant.now());

        detectorId = "123";
        memoryPerEntity = 81920;

        cacheBuffer = new CacheBuffer(
            1,
            1,
            checkpointDao,
            memoryPerEntity,
            memoryTracker,
            clock,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            detectorId
        );
        initialPriority = cacheBuffer.getPriorityTracker().getUpdatedPriority(0);
    }

    // cache.put(1, 1);
    // cache.put(2, 2);
    // cache.get(1); // returns 1
    // cache.put(3, 3); // evicts key 2
    // cache.get(2); // returns -1 (not found)
    // cache.get(3); // returns 3.
    // cache.put(4, 4); // evicts key 1.
    // cache.get(1); // returns -1 (not found)
    // cache.get(3); // returns 3
    // cache.get(4); // returns 4
    public void testRemovalCandidate() {
        String modelId1 = "1";
        String modelId2 = "2";
        String modelId3 = "3";
        String modelId4 = "4";

        cacheBuffer.put(modelId1, MLUtil.randomModelState(initialPriority, modelId1));
        cacheBuffer.put(modelId2, MLUtil.randomModelState(initialPriority, modelId2));
        assertEquals(modelId1, cacheBuffer.get(modelId1).getModelId());
        Entry<String, Float> removalCandidate = cacheBuffer.getPriorityTracker().getMinimumScaledPriority();
        assertEquals(modelId2, removalCandidate.getKey());
        cacheBuffer.remove();
        cacheBuffer.put(modelId3, MLUtil.randomModelState(initialPriority, modelId3));
        assertEquals(null, cacheBuffer.get(modelId2));
        assertEquals(modelId3, cacheBuffer.get(modelId3).getModelId());
        removalCandidate = cacheBuffer.getPriorityTracker().getMinimumScaledPriority();
        assertEquals(modelId1, removalCandidate.getKey());
        cacheBuffer.remove(modelId1);
        assertEquals(null, cacheBuffer.get(modelId1));
        cacheBuffer.put(modelId4, MLUtil.randomModelState(initialPriority, modelId4));
        assertEquals(modelId3, cacheBuffer.get(modelId3).getModelId());
        assertEquals(modelId4, cacheBuffer.get(modelId4).getModelId());
    }

    // cache.put(3, 3);
    // cache.put(2, 2);
    // cache.put(2, 2);
    // cache.put(4, 4);
    // cache.get(2) => returns 2
    public void testRemovalCandidate2() throws InterruptedException {
        String modelId2 = "2";
        String modelId3 = "3";
        String modelId4 = "4";
        float initialPriority = cacheBuffer.getPriorityTracker().getUpdatedPriority(0);
        cacheBuffer.put(modelId3, MLUtil.randomModelState(initialPriority, modelId3));
        cacheBuffer.put(modelId2, MLUtil.randomModelState(initialPriority, modelId2));
        cacheBuffer.put(modelId2, MLUtil.randomModelState(initialPriority, modelId2));
        cacheBuffer.put(modelId4, MLUtil.randomModelState(initialPriority, modelId4));
        assertTrue(cacheBuffer.getModel(modelId2).isPresent());

        ArgumentCaptor<Long> memoryReleased = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<Boolean> reserved = ArgumentCaptor.forClass(Boolean.class);
        ArgumentCaptor<MemoryTracker.Origin> orign = ArgumentCaptor.forClass(MemoryTracker.Origin.class);
        cacheBuffer.clear();
        verify(memoryTracker, times(2)).releaseMemory(memoryReleased.capture(), reserved.capture(), orign.capture());

        List<Long> capturedMemoryReleased = memoryReleased.getAllValues();
        List<Boolean> capturedreserved = reserved.getAllValues();
        List<MemoryTracker.Origin> capturedOrigin = orign.getAllValues();
        assertEquals(3 * memoryPerEntity, capturedMemoryReleased.stream().reduce(0L, (a, b) -> a + b).intValue());
        assertTrue(capturedreserved.get(0));
        assertTrue(!capturedreserved.get(1));
        assertEquals(MemoryTracker.Origin.MULTI_ENTITY_DETECTOR, capturedOrigin.get(0));

        assertTrue(!cacheBuffer.expired(Duration.ofHours(1)));
    }

    public void testCanRemove() {
        String modelId1 = "1";
        String modelId2 = "2";
        String modelId3 = "3";
        assertTrue(cacheBuffer.dedicatedCacheAvailable());
        assertTrue(!cacheBuffer.canReplaceWithinDetector(100));

        cacheBuffer.put(modelId1, MLUtil.randomModelState(initialPriority, modelId1));
        assertTrue(cacheBuffer.canReplaceWithinDetector(100));
        assertTrue(!cacheBuffer.dedicatedCacheAvailable());
        assertTrue(!cacheBuffer.canRemove());
        cacheBuffer.put(modelId2, MLUtil.randomModelState(initialPriority, modelId2));
        assertTrue(cacheBuffer.canRemove());
        cacheBuffer.replace(modelId3, MLUtil.randomModelState(initialPriority, modelId3));
        assertTrue(cacheBuffer.isActive(modelId2));
        assertTrue(cacheBuffer.isActive(modelId3));
        assertEquals(modelId3, cacheBuffer.getPriorityTracker().getHighestPriorityEntityId().get());
        assertEquals(2, cacheBuffer.getActiveEntities());
    }

    public void testMaintenance() {
        String modelId1 = "1";
        String modelId2 = "2";
        String modelId3 = "3";
        cacheBuffer.put(modelId1, MLUtil.randomModelState(initialPriority, modelId1));
        cacheBuffer.put(modelId2, MLUtil.randomModelState(initialPriority, modelId2));
        cacheBuffer.put(modelId3, MLUtil.randomModelState(initialPriority, modelId3));
        cacheBuffer.maintenance();
        assertEquals(3, cacheBuffer.getActiveEntities());
        assertEquals(3, cacheBuffer.getAllModels().size());
        when(clock.instant()).thenReturn(Instant.MAX);
        cacheBuffer.maintenance();
        assertEquals(0, cacheBuffer.getActiveEntities());
    }

    /**
     * Test that if we remove a non-existent key, there is no exception
     */
    public void testRemovedNull() {
        assertEquals(null, cacheBuffer.remove("foo"));
    }
}
