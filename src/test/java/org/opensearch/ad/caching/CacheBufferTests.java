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

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;

import org.mockito.ArgumentCaptor;
import org.opensearch.timeseries.MemoryTracker;
import org.opensearch.timeseries.ratelimit.CheckpointMaintainRequest;

import test.org.opensearch.ad.util.MLUtil;
import test.org.opensearch.ad.util.RandomModelStateConfig;

public class CacheBufferTests extends AbstractCacheTest {

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
        cacheBuffer.put(modelId1, modelState1);
        cacheBuffer.put(modelId2, modelState2);
        assertEquals(modelId1, cacheBuffer.get(modelId1).getModelId());
        Optional<Entry<String, Float>> removalCandidate = cacheBuffer.getPriorityTracker().getMinimumScaledPriority();
        assertEquals(modelId2, removalCandidate.get().getKey());
        cacheBuffer.remove();
        cacheBuffer.put(modelId3, modelState3);
        assertEquals(null, cacheBuffer.get(modelId2));
        assertEquals(modelId3, cacheBuffer.get(modelId3).getModelId());
        removalCandidate = cacheBuffer.getPriorityTracker().getMinimumScaledPriority();
        assertEquals(modelId1, removalCandidate.get().getKey());
        cacheBuffer.remove(modelId1);
        assertEquals(null, cacheBuffer.get(modelId1));
        cacheBuffer.put(modelId4, modelState4);
        assertEquals(modelId3, cacheBuffer.get(modelId3).getModelId());
        assertEquals(modelId4, cacheBuffer.get(modelId4).getModelId());
    }

    // cache.put(3, 3);
    // cache.put(2, 2);
    // cache.put(2, 2);
    // cache.put(4, 4);
    // cache.get(2) => returns 2
    public void testRemovalCandidate2() throws InterruptedException {
        cacheBuffer.put(modelId3, modelState3);
        cacheBuffer.put(modelId2, modelState2);
        cacheBuffer.put(modelId2, modelState2);
        cacheBuffer.put(modelId4, modelState4);
        assertTrue(cacheBuffer.getModelState(modelId2) != null);

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
        assertEquals(MemoryTracker.Origin.REAL_TIME_DETECTOR, capturedOrigin.get(0));

        assertTrue(!cacheBuffer.expired(Duration.ofHours(1)));
    }

    public void testCanRemove() {
        String modelId1 = "1";
        String modelId2 = "2";
        String modelId3 = "3";
        assertTrue(cacheBuffer.dedicatedCacheAvailable());
        assertTrue(!cacheBuffer.canReplaceWithinConfig(100));

        cacheBuffer.put(modelId1, MLUtil.randomModelState(new RandomModelStateConfig.Builder().priority(initialPriority).build()));
        assertTrue(cacheBuffer.canReplaceWithinConfig(100));
        assertTrue(!cacheBuffer.dedicatedCacheAvailable());
        assertTrue(!cacheBuffer.canRemove());
        cacheBuffer.put(modelId2, MLUtil.randomModelState(new RandomModelStateConfig.Builder().priority(initialPriority).build()));
        assertTrue(cacheBuffer.canRemove());
        cacheBuffer.replace(modelId3, MLUtil.randomModelState(new RandomModelStateConfig.Builder().priority(initialPriority).build()));
        assertTrue(cacheBuffer.isActive(modelId2));
        assertTrue(cacheBuffer.isActive(modelId3));
        assertEquals(modelId3, cacheBuffer.getPriorityTracker().getHighestPriorityEntityId().get());
        assertEquals(2, cacheBuffer.getActiveEntities());
    }

    public void testMaintenance() {
        String modelId1 = "1";
        String modelId2 = "2";
        String modelId3 = "3";
        cacheBuffer.put(modelId1, MLUtil.randomModelState(new RandomModelStateConfig.Builder().priority(initialPriority).build()));
        cacheBuffer.put(modelId2, MLUtil.randomModelState(new RandomModelStateConfig.Builder().priority(initialPriority).build()));
        cacheBuffer.put(modelId3, MLUtil.randomModelState(new RandomModelStateConfig.Builder().priority(initialPriority).build()));
        cacheBuffer.maintenance();
        assertEquals(3, cacheBuffer.getActiveEntities());
        assertEquals(3, cacheBuffer.getAllModelStates().size());
        // the year of 2122, 100 years later to simulate we are gonna remove all cached entries
        when(clock.instant()).thenReturn(Instant.ofEpochSecond(4814540761L));
        cacheBuffer.maintenance();
        assertEquals(0, cacheBuffer.getActiveEntities());
    }

    @SuppressWarnings("unchecked")
    public void testMaintainByHourNothingToSave() {
        // hash code 49 % 6 = 1
        String modelId1 = "1";
        // hash code 50 % 6 = 2
        String modelId2 = "2";
        // hash code 51 % 6 = 3
        String modelId3 = "3";
        // hour 17. 17 % 6 (check point frequency) = 5
        when(clock.instant()).thenReturn(Instant.ofEpochSecond(1658854904L));
        cacheBuffer.put(modelId1, MLUtil.randomModelState(new RandomModelStateConfig.Builder().priority(initialPriority).build()));
        cacheBuffer.put(modelId2, MLUtil.randomModelState(new RandomModelStateConfig.Builder().priority(initialPriority).build()));
        cacheBuffer.put(modelId3, MLUtil.randomModelState(new RandomModelStateConfig.Builder().priority(initialPriority).build()));

        ArgumentCaptor<List<CheckpointMaintainRequest>> savedStates = ArgumentCaptor.forClass(List.class);
        cacheBuffer.maintenance();
        verify(checkpointMaintainQueue, times(1)).putAll(savedStates.capture());
        assertTrue(savedStates.getValue().isEmpty());

        // hour 13. 13 % 6 (check point frequency) = 1
        when(clock.instant()).thenReturn(Instant.ofEpochSecond(1658928080L));

    }

    @SuppressWarnings("unchecked")
    public void testMaintainByHourSaveOne() {
        // hash code 49 % 6 = 1
        String modelId1 = "1";
        // hash code 50 % 6 = 2
        String modelId2 = "2";
        // hash code 51 % 6 = 3
        String modelId3 = "3";
        // hour 13. 13 % 6 (check point frequency) = 1
        when(clock.instant()).thenReturn(Instant.ofEpochSecond(1658928080L));
        cacheBuffer.put(modelId1, MLUtil.randomModelState(new RandomModelStateConfig.Builder().priority(initialPriority).build()));
        cacheBuffer.put(modelId2, MLUtil.randomModelState(new RandomModelStateConfig.Builder().priority(initialPriority).build()));
        cacheBuffer.put(modelId3, MLUtil.randomModelState(new RandomModelStateConfig.Builder().priority(initialPriority).build()));

        ArgumentCaptor<List<CheckpointMaintainRequest>> savedStates = ArgumentCaptor.forClass(List.class);
        cacheBuffer.maintenance();
        verify(checkpointMaintainQueue, times(1)).putAll(savedStates.capture());
        List<CheckpointMaintainRequest> toSave = savedStates.getValue();
        assertEquals(1, toSave.size());
        assertEquals(modelId1, toSave.get(0).getModelId());
    }

    /**
     * Test that if we remove a non-existent key, there is no exception
     */
    public void testRemovedNull() {
        assertEquals(null, cacheBuffer.remove("foo"));
    }
}
