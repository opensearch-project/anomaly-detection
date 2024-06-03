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
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import org.junit.Before;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.caching.PriorityTracker;

public class PriorityTrackerTests extends OpenSearchTestCase {
    Clock clock;
    PriorityTracker tracker;
    Instant now;
    String entity1, entity2, entity3;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        clock = mock(Clock.class);
        now = Instant.now();
        tracker = new PriorityTracker(clock, 1, now.getEpochSecond(), 3);
        entity1 = "entity1";
        entity2 = "entity2";
        entity3 = "entity3";
    }

    public void testNormal() {
        when(clock.instant()).thenReturn(now);
        // first interval entity 1 and 3
        tracker.updatePriority(entity1);
        tracker.updatePriority(entity3);
        when(clock.instant()).thenReturn(now.plusSeconds(60L));
        // second interval entity 1 and 2
        tracker.updatePriority(entity1);
        tracker.updatePriority(entity2);
        // we should have entity 1, 2, 3 in order. 2 comes before 3 because it happens later
        List<String> top3 = tracker.getTopNEntities(3);
        assertEquals(entity1, top3.get(0));
        assertEquals(entity2, top3.get(1));
        assertEquals(entity3, top3.get(2));

        // even though I want top 4, but there are only 3 entities
        List<String> top4 = tracker.getTopNEntities(4);
        assertEquals(3, top4.size());
        assertEquals(entity1, top3.get(0));
        assertEquals(entity2, top3.get(1));
        assertEquals(entity3, top3.get(2));
    }

    public void testOverflow() {
        when(clock.instant()).thenReturn(now);
        tracker.updatePriority(entity1);
        float priority1 = tracker.getMinimumScaledPriority().get().getValue();

        // when(clock.instant()).thenReturn(now.plusSeconds(60L));
        tracker.updatePriority(entity1);
        float priority2 = tracker.getMinimumScaledPriority().get().getValue();
        // we incremented the priority
        assertTrue("The following is expected: " + priority2 + " > " + priority1, priority2 > priority1);

        when(clock.instant()).thenReturn(now.plus(3, ChronoUnit.DAYS));
        tracker.updatePriority(entity1);
        // overflow happens, we use increment as the new priority
        assertEquals(0, tracker.getMinimumScaledPriority().get().getValue().floatValue(), 0.001);
    }

    public void testTooManyEntities() {
        when(clock.instant()).thenReturn(now);
        tracker = new PriorityTracker(clock, 1, now.getEpochSecond(), 2);
        tracker.updatePriority(entity1);
        tracker.updatePriority(entity3);
        assertEquals(2, tracker.size());
        tracker.updatePriority(entity2);
        // one entity is kicked out due to the size limit is reached.
        assertEquals(2, tracker.size());
    }

    public void testEmptyTracker() {
        assertTrue(!tracker.getMinimumScaledPriority().isPresent());
        assertTrue(!tracker.getMinimumPriority().isPresent());
        assertTrue(!tracker.getMinimumPriorityEntityId().isPresent());
        assertTrue(!tracker.getHighestPriorityEntityId().isPresent());
    }
}
