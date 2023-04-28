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

import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_MODEL_SIZE_PER_NODE;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.TransportAction;
import org.opensearch.ad.caching.CacheProvider;
import org.opensearch.ad.feature.FeatureManager;
import org.opensearch.ad.ml.ModelManager;
import org.opensearch.ad.model.DetectorProfileName;
import org.opensearch.ad.model.ModelProfile;
import org.opensearch.common.settings.Settings;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.SDKClusterService;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;

import com.google.inject.Inject;

/**
 *  This class contains the logic to extract the stats from the nodes
 */
public class ProfileTransportAction extends TransportAction<ProfileRequest, ProfileResponse> {
    private static final Logger LOG = LogManager.getLogger(ProfileTransportAction.class);
    private ModelManager modelManager;
    private FeatureManager featureManager;
    private CacheProvider cacheProvider;
    private SDKClusterService sdkClusterService;
    // the number of models to return. Defaults to 10.
    private volatile int numModelsToReturn;

    /**
     * Constructor
     *
     * @param actionFilters Action Filters
     * @param modelManager model manager object
     * @param featureManager feature manager object
     * @param cacheProvider cache provider
     */
    @Inject
    public ProfileTransportAction(
        ExtensionsRunner extensionsRunner,
        ActionFilters actionFilters,
        TaskManager taskManager,
        ModelManager modelManager,
        SDKClusterService sdkClusterService,
        FeatureManager featureManager,
        CacheProvider cacheProvider
    ) {
        super(ProfileAction.NAME, actionFilters, taskManager);
        this.modelManager = modelManager;
        this.featureManager = featureManager;
        this.cacheProvider = cacheProvider;
        this.sdkClusterService = sdkClusterService;
        Settings settings = extensionsRunner.getEnvironmentSettings();
        this.numModelsToReturn = MAX_MODEL_SIZE_PER_NODE.get(settings);
        this.sdkClusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_MODEL_SIZE_PER_NODE, it -> this.numModelsToReturn = it);
    }

    private ProfileResponse newResponse(ProfileRequest request, List<ProfileNodeResponse> responses, List<FailedNodeException> failures) {
        return new ProfileResponse(sdkClusterService.state().getClusterName(), responses, failures);
    }

    @Override
    protected void doExecute(Task task, ProfileRequest request, ActionListener<ProfileResponse> actionListener) {
        String detectorId = request.getDetectorId();
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

        actionListener
            .onResponse(
                newResponse(
                    request,
                    new ArrayList<>(
                        List
                            .of(
                                new ProfileNodeResponse(
                                    sdkClusterService.localNode(),
                                    modelSize,
                                    shingleSize,
                                    activeEntity,
                                    totalUpdates,
                                    modelProfiles,
                                    modelCount
                                )
                            )
                    ),
                    new ArrayList<>() // empty failures
                )
            );
    }
}
