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

package org.opensearch.forecast.caching;

import java.time.Clock;
import java.time.Duration;

import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.forecast.ml.ForecastCheckpointDao;
import org.opensearch.forecast.ratelimit.ForecastCheckpointMaintainWorker;
import org.opensearch.forecast.ratelimit.ForecastCheckpointWriteWorker;
import org.opensearch.timeseries.MemoryTracker;
import org.opensearch.timeseries.MemoryTracker.Origin;
import org.opensearch.timeseries.caching.CacheBuffer;
import org.opensearch.timeseries.caching.PriorityTracker;

import com.amazon.randomcutforest.parkservices.RCFCaster;

public class ForecastCacheBuffer extends
    CacheBuffer<RCFCaster, ForecastIndex, ForecastIndexManagement, ForecastCheckpointDao, ForecastCheckpointWriteWorker, ForecastCheckpointMaintainWorker> {

    public ForecastCacheBuffer(
        int minimumCapacity,
        Clock clock,
        MemoryTracker memoryTracker,
        int checkpointIntervalHrs,
        Duration modelTtl,
        long memoryConsumptionPerEntity,
        ForecastCheckpointWriteWorker checkpointWriteQueue,
        ForecastCheckpointMaintainWorker checkpointMaintainQueue,
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
            Origin.REAL_TIME_FORECASTER,
            priorityTracker
        );
    }
}
