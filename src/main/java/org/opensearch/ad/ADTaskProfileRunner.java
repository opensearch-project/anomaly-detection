/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskProfile;
import org.opensearch.ad.transport.ADTaskProfileAction;
import org.opensearch.ad.transport.ADTaskProfileNodeResponse;
import org.opensearch.ad.transport.ADTaskProfileRequest;
import org.opensearch.client.Client;
import org.opensearch.core.action.ActionListener;
import org.opensearch.timeseries.TaskProfileRunner;
import org.opensearch.timeseries.cluster.HashRing;
import org.opensearch.timeseries.model.EntityTaskProfile;

public class ADTaskProfileRunner implements TaskProfileRunner<ADTask, ADTaskProfile> {
    public final Logger logger = LogManager.getLogger(ADTaskProfileRunner.class);

    private final HashRing hashRing;
    private final Client client;

    public ADTaskProfileRunner(HashRing hashRing, Client client) {
        this.hashRing = hashRing;
        this.client = client;
    }

    @Override
    public void getTaskProfile(ADTask configLevelTask, ActionListener<ADTaskProfile> listener) {
        String detectorId = configLevelTask.getConfigId();

        hashRing.getAllEligibleDataNodesWithKnownVersion(dataNodes -> {
            ADTaskProfileRequest adTaskProfileRequest = new ADTaskProfileRequest(detectorId, dataNodes);
            client.execute(ADTaskProfileAction.INSTANCE, adTaskProfileRequest, ActionListener.wrap(response -> {
                if (response.hasFailures()) {
                    listener.onFailure(response.failures().get(0));
                    return;
                }

                List<EntityTaskProfile> adEntityTaskProfiles = new ArrayList<>();
                ADTaskProfile detectorTaskProfile = new ADTaskProfile(configLevelTask);
                for (ADTaskProfileNodeResponse node : response.getNodes()) {
                    ADTaskProfile taskProfile = node.getAdTaskProfile();
                    if (taskProfile != null) {
                        if (taskProfile.getNodeId() != null) {
                            // HC detector: task profile from coordinating node
                            // Single entity detector: task profile from worker node
                            detectorTaskProfile.setTaskId(taskProfile.getTaskId());
                            detectorTaskProfile.setRcfTotalUpdates(taskProfile.getRcfTotalUpdates());
                            detectorTaskProfile.setThresholdModelTrained(taskProfile.getThresholdModelTrained());
                            detectorTaskProfile.setThresholdModelTrainingDataSize(taskProfile.getThresholdModelTrainingDataSize());
                            detectorTaskProfile.setModelSizeInBytes(taskProfile.getModelSizeInBytes());
                            detectorTaskProfile.setNodeId(taskProfile.getNodeId());
                            detectorTaskProfile.setTotalEntitiesCount(taskProfile.getTotalEntitiesCount());
                            detectorTaskProfile.setDetectorTaskSlots(taskProfile.getDetectorTaskSlots());
                            detectorTaskProfile.setPendingEntitiesCount(taskProfile.getPendingEntitiesCount());
                            detectorTaskProfile.setRunningEntitiesCount(taskProfile.getRunningEntitiesCount());
                            detectorTaskProfile.setRunningEntities(taskProfile.getRunningEntities());
                            detectorTaskProfile.setTaskType(taskProfile.getTaskType());
                        }
                        if (taskProfile.getEntityTaskProfiles() != null) {
                            adEntityTaskProfiles.addAll(taskProfile.getEntityTaskProfiles());
                        }
                    }
                }
                if (adEntityTaskProfiles != null && adEntityTaskProfiles.size() > 0) {
                    detectorTaskProfile.setEntityTaskProfiles(adEntityTaskProfiles);
                }
                listener.onResponse(detectorTaskProfile);
            }, e -> {
                logger.error("Failed to get task profile for task " + configLevelTask.getTaskId(), e);
                listener.onFailure(e);
            }));
        }, listener);

    }

}
