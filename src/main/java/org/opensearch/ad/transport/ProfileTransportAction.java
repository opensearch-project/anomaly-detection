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

package org.opensearch.ad.transport;

import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_MAX_MODEL_SIZE_PER_NODE;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.ad.caching.CacheProvider;
import org.opensearch.ad.feature.FeatureManager;
import org.opensearch.ad.ml.ModelManager;
import org.opensearch.ad.model.DetectorProfileName;
import org.opensearch.ad.model.ModelProfile;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 *  This class contains the logic to extract the stats from the nodes
 */
public class ProfileTransportAction extends TransportNodesAction<ProfileRequest, ProfileResponse, ProfileNodeRequest, ProfileNodeResponse> {
    private static final Logger LOG = LogManager.getLogger(ProfileTransportAction.class);
    private ModelManager modelManager;
    private FeatureManager featureManager;
    private CacheProvider cacheProvider;
    // the number of models to return. Defaults to 10.
    private volatile int numModelsToReturn;

    /**
     * Constructor
     *
     * @param threadPool ThreadPool to use
     * @param clusterService ClusterService
     * @param transportService TransportService
     * @param actionFilters Action Filters
     * @param modelManager model manager object
     * @param featureManager feature manager object
     * @param cacheProvider cache provider
     * @param settings Node settings accessor
     */
    @Inject
    public ProfileTransportAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        ModelManager modelManager,
        FeatureManager featureManager,
        CacheProvider cacheProvider,
        Settings settings
    ) {
        super(
            ProfileAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            ProfileRequest::new,
            ProfileNodeRequest::new,
            ThreadPool.Names.MANAGEMENT,
            ProfileNodeResponse.class
        );
        this.modelManager = modelManager;
        this.featureManager = featureManager;
        this.cacheProvider = cacheProvider;
        this.numModelsToReturn = AD_MAX_MODEL_SIZE_PER_NODE.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(AD_MAX_MODEL_SIZE_PER_NODE, it -> this.numModelsToReturn = it);
    }

    @Override
    protected ProfileResponse newResponse(ProfileRequest request, List<ProfileNodeResponse> responses, List<FailedNodeException> failures) {
        return new ProfileResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected ProfileNodeRequest newNodeRequest(ProfileRequest request) {
        return new ProfileNodeRequest(request);
    }

    @Override
    protected ProfileNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new ProfileNodeResponse(in);
    }

    @Override
    protected ProfileNodeResponse nodeOperation(ProfileNodeRequest request) {
        String detectorId = request.getId();
        Set<DetectorProfileName> profiles = request.getProfilesToBeRetrieved();
        int shingleSize = -1;
        long activeEntity = 0;
        long totalUpdates = 0;
        Map<String, Long> modelSize = null;
        List<ModelProfile> modelProfiles = null;
        int modelCount = 0;
        if (request.isForMultiEntityDetector()) {
            if (profiles.contains(DetectorProfileName.ACTIVE_ENTITIES)) {
                activeEntity = cacheProvider.get().getActiveEntities(detectorId);
            }
            if (profiles.contains(DetectorProfileName.INIT_PROGRESS)) {
                totalUpdates = cacheProvider.get().getTotalUpdates(detectorId);// get toal updates
            }
            if (profiles.contains(DetectorProfileName.TOTAL_SIZE_IN_BYTES)) {
                modelSize = cacheProvider.get().getModelSize(detectorId);
            }
            // need to provide entity info for HCAD
            if (profiles.contains(DetectorProfileName.MODELS)) {
                modelProfiles = cacheProvider.get().getAllModelProfile(detectorId);
                modelCount = modelProfiles.size();
                int limit = Math.min(numModelsToReturn, modelCount);
                if (limit != modelCount) {
                    LOG.info("model number limit reached");
                    modelProfiles = modelProfiles.subList(0, limit);
                }
            }
        } else {
            if (profiles.contains(DetectorProfileName.COORDINATING_NODE) || profiles.contains(DetectorProfileName.SHINGLE_SIZE)) {
                shingleSize = featureManager.getShingleSize(detectorId);
            }

            if (profiles.contains(DetectorProfileName.TOTAL_SIZE_IN_BYTES) || profiles.contains(DetectorProfileName.MODELS)) {
                modelSize = modelManager.getModelSize(detectorId);
            }
        }

        return new ProfileNodeResponse(
            clusterService.localNode(),
            modelSize,
            shingleSize,
            activeEntity,
            totalUpdates,
            modelProfiles,
            modelCount
        );
    }
}
