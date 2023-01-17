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
import java.util.Map;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Provider;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.Strings;
import org.opensearch.timeseries.caching.TimeSeriesCache;
import org.opensearch.timeseries.indices.IndexManagement;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.ml.CheckpointDao;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.util.DateUtils;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

/**
 * Convert from ModelRequest to CheckpointWriteRequest
 *
 */
public class CheckPointMaintainRequestAdapter<RCFModelType extends ThresholdedRandomCutForest, IndexType extends Enum<IndexType> & TimeSeriesIndex, IndexManagementType extends IndexManagement<IndexType>, CheckpointDaoType extends CheckpointDao<RCFModelType, IndexType, IndexManagementType>, CacheType extends TimeSeriesCache<RCFModelType>> {
    private static final Logger LOG = LogManager.getLogger(CheckPointMaintainRequestAdapter.class);
    private CheckpointDaoType checkpointDao;
    private String indexName;
    private Duration checkpointInterval;
    private Clock clock;
    private Provider<CacheType> cache;

    public CheckPointMaintainRequestAdapter(
        CheckpointDaoType checkpointDao,
        String indexName,
        Setting<TimeValue> checkpointIntervalSetting,
        Clock clock,
        ClusterService clusterService,
        Settings settings,
        Provider<CacheType> cache
    ) {
        this.checkpointDao = checkpointDao;
        this.indexName = indexName;

        this.checkpointInterval = DateUtils.toDuration(checkpointIntervalSetting.get(settings));
        clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(checkpointIntervalSetting, it -> this.checkpointInterval = DateUtils.toDuration(it));

        this.clock = clock;
        this.cache = cache;
    }

    public Optional<CheckpointWriteRequest> convert(CheckpointMaintainRequest request) {
        String configId = request.getConfigId();
        String modelId = request.getModelId();

        Optional<ModelState<RCFModelType>> stateToMaintain = cache.get().getForMaintainance(configId, modelId);
        if (stateToMaintain.isPresent()) {
            ModelState<RCFModelType> state = stateToMaintain.get();
            if (!checkpointDao.shouldSave(state, false, checkpointInterval, clock)) {
                return Optional.empty();
            }

            try {
                Map<String, Object> source = checkpointDao.toIndexSource(state);

                // the model state is bloated or empty (empty samples and models), skip
                if (source == null || source.isEmpty() || Strings.isEmpty(modelId)) {
                    return Optional.empty();
                }

                return Optional
                    .of(
                        new CheckpointWriteRequest(
                            request.getExpirationEpochMs(),
                            configId,
                            request.getPriority(),
                            // If the document does not already exist, the contents of the upsert element
                            // are inserted as a new document.
                            // If the document exists, update fields in the map
                            new UpdateRequest(indexName, modelId).docAsUpsert(true).doc(source)
                        )
                    );
            } catch (Exception e) {
                // Example exception:
                // ConcurrentModificationException when calling toIndexSource
                // and updating rcf model at the same time. To prevent this,
                // we need to have a deep copy of models or have a lock. Both
                // options are costly.
                // As we are gonna retry serializing either when the entity is
                // evicted out of cache or during the next maintenance period,
                // don't do anything when the exception happens.
                LOG.error(new ParameterizedMessage("Exception while serializing models for [{}]", modelId), e);
            }
        }
        return Optional.empty();
    }
}
