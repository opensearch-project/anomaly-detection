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

import java.time.Clock;
import java.time.Duration;

import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.ml.ADCheckpointDao;
import org.opensearch.ad.ratelimit.ADCheckpointMaintainWorker;
import org.opensearch.ad.ratelimit.ADCheckpointWriteWorker;
import org.opensearch.timeseries.MemoryTracker;
import org.opensearch.timeseries.MemoryTracker.Origin;
import org.opensearch.timeseries.caching.CacheBuffer;
import org.opensearch.timeseries.caching.PriorityTracker;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

/**
 * We use a layered cache to manage active entities’ states.  We have a two-level
 * cache that stores active entity states in each node.  Each detector has its
 * dedicated cache that stores ten (dynamically adjustable) entities’ states per
 * node.  A detector’s hottest entities load their states in the dedicated cache.
 * If less than 10 entities use the dedicated cache, the secondary cache can use
 * the rest of the free memory available to AD.  The secondary cache is a shared
 * memory among all detectors for the long tail.  The shared cache size is 10%
 * heap minus all of the dedicated cache consumed by single-entity and multi-entity
 * detectors.  The shared cache’s size shrinks as the dedicated cache is filled
 * up or more detectors are started.
 *
 * Implementation-wise, both dedicated cache and shared cache are stored in items
 * and minimumCapacity controls the boundary. If items size is equals to or less
 * than minimumCapacity, consider items as dedicated cache; otherwise, consider
 * top minimumCapacity active entities (last X entities in priorityList) as in dedicated
 * cache and all others in shared cache.
 */
public class ADCacheBuffer extends
    CacheBuffer<ThresholdedRandomCutForest, ADIndex, ADIndexManagement, ADCheckpointDao, ADCheckpointWriteWorker, ADCheckpointMaintainWorker> {

    public ADCacheBuffer(
        int minimumCapacity,
        Clock clock,
        MemoryTracker memoryTracker,
        int checkpointIntervalHrs,
        Duration modelTtl,
        long memoryConsumptionPerEntity,
        ADCheckpointWriteWorker checkpointWriteQueue,
        ADCheckpointMaintainWorker checkpointMaintainQueue,
        String configId,
        PriorityTracker priorityTracker
    ) {
        super(
            minimumCapacity,
            clock,
            memoryTracker,
            checkpointIntervalHrs,
            modelTtl,
            memoryConsumptionPerEntity,
            checkpointWriteQueue,
            checkpointMaintainQueue,
            configId,
            Origin.REAL_TIME_DETECTOR,
            priorityTracker
        );
    }
}
