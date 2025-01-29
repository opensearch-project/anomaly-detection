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

package org.opensearch.ad.ratelimit;

import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_RESULT_WRITE_QUEUE_BATCH_SIZE;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_RESULT_WRITE_QUEUE_CONCURRENCY;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Random;

import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.transport.ADResultBulkRequest;
import org.opensearch.ad.transport.handler.ADIndexMemoryPressureAwareResultHandler;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.ratelimit.RequestPriority;
import org.opensearch.timeseries.ratelimit.ResultWriteWorker;

public class ADResultWriteWorker extends
    ResultWriteWorker<AnomalyResult, ADResultWriteRequest, ADResultBulkRequest, ADIndex, ADIndexManagement, ADIndexMemoryPressureAwareResultHandler> {
    public static final String WORKER_NAME = "ad-result-write";

    public ADResultWriteWorker(
        long heapSizeInBytes,
        int singleRequestSizeInBytes,
        Setting<Float> maxHeapPercentForQueueSetting,
        ClusterService clusterService,
        Random random,
        CircuitBreakerService adCircuitBreakerService,
        ThreadPool threadPool,
        Settings settings,
        float maxQueuedTaskRatio,
        Clock clock,
        float mediumSegmentPruneRatio,
        float lowSegmentPruneRatio,
        int maintenanceFreqConstant,
        Duration executionTtl,
        ADIndexMemoryPressureAwareResultHandler resultHandler,
        NamedXContentRegistry xContentRegistry,
        NodeStateManager stateManager,
        Duration stateTtl
    ) {
        super(
            WORKER_NAME,
            heapSizeInBytes,
            singleRequestSizeInBytes,
            maxHeapPercentForQueueSetting,
            clusterService,
            random,
            adCircuitBreakerService,
            threadPool,
            TimeSeriesAnalyticsPlugin.AD_THREAD_POOL_NAME,
            settings,
            maxQueuedTaskRatio,
            clock,
            mediumSegmentPruneRatio,
            lowSegmentPruneRatio,
            maintenanceFreqConstant,
            AD_RESULT_WRITE_QUEUE_CONCURRENCY,
            executionTtl,
            AD_RESULT_WRITE_QUEUE_BATCH_SIZE,
            stateTtl,
            stateManager,
            resultHandler,
            xContentRegistry,
            AnomalyResult::parse,
            AnalysisType.AD
        );
    }

    @Override
    protected ADResultBulkRequest toBatchRequest(List<ADResultWriteRequest> toProcess) {
        final ADResultBulkRequest bulkRequest = new ADResultBulkRequest();
        for (ADResultWriteRequest request : toProcess) {
            bulkRequest.add(request);
        }
        return bulkRequest;
    }

    @Override
    protected ADResultWriteRequest createResultWriteRequest(
        long expirationEpochMs,
        String configId,
        RequestPriority priority,
        AnomalyResult result,
        String resultIndex,
        String flattenResultIndex
    ) {
        return new ADResultWriteRequest(expirationEpochMs, configId, priority, result, resultIndex, flattenResultIndex);
    }
}
