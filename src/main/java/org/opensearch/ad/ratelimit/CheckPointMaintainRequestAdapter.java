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

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.ad.caching.CacheProvider;
import org.opensearch.ad.ml.CheckpointDao;
import org.opensearch.ad.ml.EntityModel;
import org.opensearch.ad.ml.ModelState;
import org.opensearch.ad.util.DateUtils;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.Strings;

public class CheckPointMaintainRequestAdapter {
    private static final Logger LOG = LogManager.getLogger(CheckPointMaintainRequestAdapter.class);
    private CacheProvider cache;
    private CheckpointDao checkpointDao;
    private String indexName;
    private Duration checkpointInterval;
    private Clock clock;

    public CheckPointMaintainRequestAdapter(
        CacheProvider cache,
        CheckpointDao checkpointDao,
        String indexName,
        Setting<TimeValue> checkpointIntervalSetting,
        Clock clock,
        ClusterService clusterService,
        Settings settings
    ) {
        this.cache = cache;
        this.checkpointDao = checkpointDao;
        this.indexName = indexName;

        this.checkpointInterval = DateUtils.toDuration(checkpointIntervalSetting.get(settings));
        clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(checkpointIntervalSetting, it -> this.checkpointInterval = DateUtils.toDuration(it));

        this.clock = clock;
    }

    public Optional<CheckpointWriteRequest> convert(CheckpointMaintainRequest request) {
        String detectorId = request.getDetectorId();
        String modelId = request.getEntityModelId();

        Optional<ModelState<EntityModel>> stateToMaintain = cache.get().getForMaintainance(detectorId, modelId);
        if (!stateToMaintain.isEmpty()) {
            ModelState<EntityModel> state = stateToMaintain.get();
            Instant instant = state.getLastCheckpointTime();
            if (!checkpointDao.shouldSave(instant, false, checkpointInterval, clock)) {
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
                            detectorId,
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
