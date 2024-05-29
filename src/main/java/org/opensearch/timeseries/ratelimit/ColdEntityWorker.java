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

package org.opensearch.timeseries.ratelimit;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.caching.TimeSeriesCache;
import org.opensearch.timeseries.indices.IndexManagement;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.ml.CheckpointDao;
import org.opensearch.timeseries.ml.IntermediateResult;
import org.opensearch.timeseries.ml.ModelColdStart;
import org.opensearch.timeseries.ml.ModelManager;
import org.opensearch.timeseries.model.IndexableResult;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

public class ColdEntityWorker<RCFModelType extends ThresholdedRandomCutForest, IndexableResultType extends IndexableResult, IndexType extends Enum<IndexType> & TimeSeriesIndex, IndexManagementType extends IndexManagement<IndexType>, CheckpointDaoType extends CheckpointDao<RCFModelType, IndexType, IndexManagementType>, RCFResultType extends IntermediateResult<IndexableResultType>, ModelManagerType extends ModelManager<RCFModelType, IndexableResultType, RCFResultType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriteWorkerType, ColdStarterType>, CheckpointWriteWorkerType extends CheckpointWriteWorker<RCFModelType, IndexType, IndexManagementType, CheckpointDaoType>, ColdStarterType extends ModelColdStart<RCFModelType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriteWorkerType>, CacheType extends TimeSeriesCache<RCFModelType>, SaveResultStrategyType extends SaveResultStrategy<IndexableResultType, RCFResultType>, ColdStartWorkerType extends ColdStartWorker<RCFModelType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriteWorkerType, ColdStarterType, CacheType, IndexableResultType, RCFResultType, ModelManagerType, SaveResultStrategyType>, CheckpointReadWorkerType extends CheckpointReadWorker<RCFModelType, IndexableResultType, RCFResultType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriteWorkerType, ColdStarterType, ModelManagerType, CacheType, SaveResultStrategyType, ColdStartWorkerType>>
    extends ScheduledWorker<FeatureRequest, FeatureRequest> {

    public ColdEntityWorker(
        String workerName,
        long heapSizeInBytes,
        int singleRequestSizeInBytes,
        Setting<Float> maxHeapPercentForQueueSetting,
        ClusterService clusterService,
        Random random,
        CircuitBreakerService adCircuitBreakerService,
        ThreadPool threadPool,
        String threadPoolName,
        Settings settings,
        float maxQueuedTaskRatio,
        Clock clock,
        float mediumSegmentPruneRatio,
        float lowSegmentPruneRatio,
        int maintenanceFreqConstant,
        CheckpointReadWorkerType checkpointReadQueue,
        Duration stateTtl,
        NodeStateManager nodeStateManager,
        Setting<Integer> checkpointReadBatchSizeSetting,
        Setting<Integer> expectedColdEntityExecutionMillsSetting,
        AnalysisType context
    ) {
        super(
            workerName,
            heapSizeInBytes,
            singleRequestSizeInBytes,
            maxHeapPercentForQueueSetting,
            clusterService,
            random,
            adCircuitBreakerService,
            threadPool,
            threadPoolName,
            settings,
            maxQueuedTaskRatio,
            clock,
            mediumSegmentPruneRatio,
            lowSegmentPruneRatio,
            maintenanceFreqConstant,
            checkpointReadQueue,
            stateTtl,
            nodeStateManager,
            context
        );

        this.batchSize = checkpointReadBatchSizeSetting.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(checkpointReadBatchSizeSetting, it -> this.batchSize = it);

        this.expectedExecutionTimeInMilliSecsPerRequest = expectedColdEntityExecutionMillsSetting.get(settings);
        clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(expectedColdEntityExecutionMillsSetting, it -> this.expectedExecutionTimeInMilliSecsPerRequest = it);
    }

    @Override
    protected List<FeatureRequest> transformRequests(List<FeatureRequest> requests) {
        // guarantee we only send low priority requests
        return requests.stream().filter(request -> request.getPriority() == RequestPriority.LOW).collect(Collectors.toList());
    }
}
