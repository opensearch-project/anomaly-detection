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
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import java.time.Clock;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.annotation.Generated;

/**
 * A priority tracker for entities. Read docs/entity-priority.pdf for details.
 *
 * HC detectors use a 1-pass algorithm for estimating heavy hitters in a stream.
 * Our method maintains a time-decayed count for each entity, which allows us to
 * compare the frequencies/priorities of entities from different detectors in the
 *  stream.
 * This class contains the heavy-hitter tracking logic.  When an entity is hit,
 * a user calls PriorityTracker.updatePriority to update the entity's priority.
 * The user can find the most frequently occurring entities in the stream using
 * PriorityTracker.getTopNEntities.  A typical usage is listed below:
 *
 * <pre>
 * PriorityTracker tracker =  ...
 *
 * // at time t1
 * tracker.updatePriority(entity1);
 * tracker.updatePriority(entity3);
 *
 * //  at time t2
 * tracker.updatePriority(entity1);
 * tracker.updatePriority(entity2);
 *
 * // we should have entity 1, 2, 3 in order. 2 comes before 3 because it happens later
 * List&#60;String&#62; top3 = tracker.getTopNEntities(3);
 * </pre>
 *
 */
public class PriorityTracker {
    private static final Logger LOG = LogManager.getLogger(PriorityTracker.class);

    // data structure for an entity and its priority
    static class PriorityNode {
        // entity key
        private String key;
        // time-decayed priority
        private float priority;

        PriorityNode(String key, float priority) {
            this.priority = priority;
            this.key = key;
        }

        @Generated
        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            if (obj instanceof PriorityNode) {
                PriorityNode other = (PriorityNode) obj;

                EqualsBuilder equalsBuilder = new EqualsBuilder();
                equalsBuilder.append(key, other.key);
                return equalsBuilder.isEquals();
            }
            return false;
        }

        @Generated
        @Override
        public int hashCode() {
            return new HashCodeBuilder().append(key).toHashCode();
        }

        @Generated
        @Override
        public String toString() {
            ToStringBuilder builder = new ToStringBuilder(this);
            builder.append("key", key);
            builder.append("priority", priority);
            return builder.toString();
        }
    }

    // Comparator between two entities. Used to sort entities in a priority queue
    static class PriorityNodeComparator implements Comparator<PriorityNode> {

        @Override
        public int compare(PriorityNode priority, PriorityNode priority2) {
            int equality = priority.key.compareTo(priority2.key);
            if (equality == 0) {
                // this is consistent with PriorityNode's equals method
                return 0;
            }
            // if not equal, first check priority
            int cmp = Float.compare(priority.priority, priority2.priority);
            if (cmp == 0) {
                // if priority is equal, use lexicographical order of key
                cmp = equality;
            }
            return cmp;
        }
    }

    // key -> Priority node
    private final ConcurrentHashMap<String, PriorityNode> key2Priority;
    // when detector is created.  Can be reset.  Unit: seconds
    private long landmarkEpoch;
    // a list of priority nodes
    private final ConcurrentSkipListSet<PriorityNode> priorityList;
    // Used to get current time.
    private final Clock clock;
    // length of seconds in one interval.  Used to compute elapsed periods
    // since the detector has been enabled.
    private final long intervalSecs;
    // determines how fast the decay is
    // We use the decay constant 0.125. The half life (https://en.wikipedia.org/wiki/Exponential_decay)
    // is 8* ln(2). This means the old value falls to one half with roughly 5.6 intervals.
    // We chose 0.125 because multiplying 0.125 can be implemented efficiently using 3 right
    // shift and the half life is not too fast or slow .
    private final int DECAY_CONSTANT;
    // the max number of entities to track
    private final int maxEntities;

    /**
     * Create a priority tracker for a detector.  Detector and priority tracker
     * have 1:1 mapping.
     *
     * @param clock Used to get current time.
     * @param intervalSecs Detector interval seconds.
     * @param landmarkEpoch The epoch time when the priority tracking starts.
     * @param maxEntities the max number of entities to track
     */
    public PriorityTracker(Clock clock, long intervalSecs, long landmarkEpoch, int maxEntities) {
        this.key2Priority = new ConcurrentHashMap<>();
        this.clock = clock;
        this.intervalSecs = intervalSecs;
        this.landmarkEpoch = landmarkEpoch;
        this.priorityList = new ConcurrentSkipListSet<>(new PriorityNodeComparator());
        this.DECAY_CONSTANT = 3;
        this.maxEntities = maxEntities;
    }

    /**
     * Get the minimum priority entity and compute its scaled priority.
     * Used to compare entity priorities among detectors.
     * @return the minimum priority entity's ID and scaled priority or Optional.empty
     *  if the priority list is empty
     */
    public Optional<Entry<String, Float>> getMinimumScaledPriority() {
        if (priorityList.isEmpty()) {
            return Optional.empty();
        }
        PriorityNode smallest = priorityList.first();
        return Optional.of(new SimpleImmutableEntry<>(smallest.key, getScaledPriority(smallest.priority)));
    }

    /**
     * Get the minimum priority entity and compute its scaled priority.
     * Used to compare entity priorities within the same detector.
     * @return the minimum priority entity's ID and scaled priority or Optional.empty
     *  if the priority list is empty
     */
    public Optional<Entry<String, Float>> getMinimumPriority() {
        if (priorityList.isEmpty()) {
            return Optional.empty();
        }
        PriorityNode smallest = priorityList.first();
        return Optional.of(new SimpleImmutableEntry<>(smallest.key, smallest.priority));
    }

    /**
     *
     * @return the minimum priority entity's Id or Optional.empty
     *  if the priority list is empty
     */
    public Optional<String> getMinimumPriorityEntityId() {
        if (priorityList.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(priorityList).map(list -> list.first()).map(node -> node.key);
    }

    /**
    *
    * @return Get maximum priority entity's Id
    */
    public Optional<String> getHighestPriorityEntityId() {
        if (priorityList.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(priorityList).map(list -> list.last()).map(node -> node.key);
    }

    /**
     * Update an entity's priority with count increment
     * @param entityId Entity Id
     */
    public void updatePriority(String entityId) {
        PriorityNode node = key2Priority.computeIfAbsent(entityId, k -> new PriorityNode(entityId, 0f));
        // reposition this node
        this.priorityList.remove(node);
        node.priority = getUpdatedPriority(node.priority);
        this.priorityList.add(node);

        adjustSizeIfRequired();
    }

    /**
     * Associate the specified priority with the entity Id
     * @param entityId Entity Id
     * @param priority priority
     */
    protected void addPriority(String entityId, float priority) {
        PriorityNode node = new PriorityNode(entityId, priority);
        key2Priority.put(entityId, node);
        priorityList.add(node);

        adjustSizeIfRequired();
    }

    /**
     * Adjust tracking list if the size exceeded the limit
     */
    private void adjustSizeIfRequired() {
        if (key2Priority.size() > maxEntities) {
            Optional<String> minPriorityId = getMinimumPriorityEntityId();
            if (minPriorityId.isPresent()) {
                removePriority(minPriorityId.get());
            }
        }
    }

    /**
     * Remove an entity in the tracker
     * @param entityId Entity Id
     */
    protected void removePriority(String entityId) {
        // remove if the key matches; priority does not matter
        priorityList.remove(new PriorityNode(entityId, 0));
        key2Priority.remove(entityId);
    }

    /**
     * Remove all of entities
     */
    protected void clearPriority() {
        key2Priority.clear();
        priorityList.clear();
    }

    /**
     * Return the updated priority with new priority increment. Used when comparing
     * entities' priorities within the same detector.
     *
     * Each detector maintains an ordered map, filled by entities's accumulated sum of g(i−L),
     * which is what this function computes.
     *
     * g(n) = e^{0.125n}.  i is current period. L is the landmark: period 0 when the
     * detector is enabled. i - L measures the elapsed periods since detector starts.
     * 0.125 is the decay constant.
     *
     * Since g(i−L) is changing and they are the same for all entities of the same detector,
     * we can compare entities' priorities by considering the accumulated sum of g(i−L).
     *
     * @param oldPriority Existing priority
     *
     * @return new priority
     */
    float getUpdatedPriority(float oldPriority) {
        long increment = computeWeightedPriorityIncrement();
        oldPriority += Math.log(1 + Math.exp(increment - oldPriority));
        // if overflow happens, using the most recent decayed count instead.
        if (oldPriority == Float.POSITIVE_INFINITY) {
            oldPriority = increment;
        }
        return oldPriority;
    }

    /**
     * Return the scaled priority. Used when comparing entities' priorities among
     * different detectors.
     *
     * Updated priority = current priority - log(g(t - L)), where g(n) = e^{0.125n},
     * t is current time, and L is the landmark. t - L measures the number of elapsed
     * periods relative to the landmark.
     *
     * When replacing an entity, we query the minimum from each ordered map and
     * compute w(i,p) for each minimum entity by scaling the sum by g(p−L). Notice g(p−L)
     * can be different if detectors start at different timestamps. The minimum of the minimum
     * is selected to be replaced. The number of multi-entity detectors is limited (we consider
     * to support ten currently), so the computation is cheap.
     *
     * @param currentPriority Current priority
     * @return the scaled priority
     */
    float getScaledPriority(float currentPriority) {
        return currentPriority - computeWeightedPriorityIncrement();
    }

    /**
     * Compute the weighted priority increment using 0.125n, where n is the number of
     * periods relative to the landmark.
     * Each detector has its own landmark L: period 0 when the detector is enabled.
     *
     * @return the weighted priority increment used in the priority update step.
     */
    long computeWeightedPriorityIncrement() {
        long periods = (clock.instant().getEpochSecond() - landmarkEpoch) / intervalSecs;
        return periods >> DECAY_CONSTANT;
    }

    /**
     *
     * @param n the number of entities to return.  Can be less than n if there are not enough entities stored.
     * @return top entities in the descending order of priority
     */
    public List<String> getTopNEntities(int n) {
        List<String> entities = new ArrayList<>();
        Iterator<PriorityNode> entityIterator = priorityList.descendingIterator();
        for (int i = 0; i < n && entityIterator.hasNext(); i++) {
            entities.add(entityIterator.next().key);
        }
        return entities;
    }

    /**
     *
     * @return the number of tracked entities
     */
    public int size() {
        return key2Priority.size();
    }
}
