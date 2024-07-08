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

package org.opensearch.timeseries.transport;

import java.io.IOException;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.caching.CacheProvider;
import org.opensearch.timeseries.caching.TimeSeriesCache;
import org.opensearch.timeseries.cluster.HashRing;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.EntityProfileName;
import org.opensearch.timeseries.model.ModelProfile;
import org.opensearch.timeseries.model.ModelProfileOnNode;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

/**
 * Transport action to get entity profile.
 */
public class BaseEntityProfileTransportAction<RCFModelType extends ThresholdedRandomCutForest, CacheType extends TimeSeriesCache<RCFModelType>, CacheProviderType extends CacheProvider<RCFModelType, CacheType>>
    extends HandledTransportAction<EntityProfileRequest, EntityProfileResponse> {

    private static final Logger LOG = LogManager.getLogger(BaseEntityProfileTransportAction.class);
    public static final String NO_NODE_FOUND_MSG = "Cannot find model hosting node";
    public static final String NO_MODEL_ID_FOUND_MSG = "Cannot find model id";
    static final String FAIL_TO_GET_ENTITY_PROFILE_MSG = "Cannot get entity profile info";

    private final TransportService transportService;
    private final HashRing hashRing;
    private final TransportRequestOptions option;
    private final ClusterService clusterService;
    private final CacheProviderType cacheProvider;
    private final String entityProfileAction;

    public BaseEntityProfileTransportAction(
        ActionFilters actionFilters,
        TransportService transportService,
        Settings settings,
        HashRing hashRing,
        ClusterService clusterService,
        CacheProviderType cacheProvider,
        String entityProfileAction,
        Setting<TimeValue> requestTimeOut
    ) {
        super(entityProfileAction, transportService, actionFilters, EntityProfileRequest::new);
        this.transportService = transportService;
        this.hashRing = hashRing;
        this.option = TransportRequestOptions
            .builder()
            .withType(TransportRequestOptions.Type.REG)
            .withTimeout(requestTimeOut.get(settings))
            .build();
        this.clusterService = clusterService;
        this.cacheProvider = cacheProvider;
        this.entityProfileAction = entityProfileAction;
    }

    @Override
    protected void doExecute(Task task, EntityProfileRequest request, ActionListener<EntityProfileResponse> listener) {

        String adID = request.getConfigID();
        Entity entityValue = request.getEntityValue();
        Optional<String> modelIdOptional = entityValue.getModelId(adID);
        if (false == modelIdOptional.isPresent()) {
            listener.onFailure(new TimeSeriesException(adID, NO_MODEL_ID_FOUND_MSG));
            return;
        }
        // we use entity's toString (e.g., app_0) to find its node
        // This should be consistent with how we land a model node in AnomalyResultTransportAction
        Optional<DiscoveryNode> node = hashRing.getOwningNodeWithSameLocalVersionForRealtime(entityValue.toString());
        if (false == node.isPresent()) {
            listener.onFailure(new TimeSeriesException(adID, NO_NODE_FOUND_MSG));
            return;
        }
        String nodeId = node.get().getId();
        String modelId = modelIdOptional.get();
        DiscoveryNode localNode = clusterService.localNode();
        if (localNode.getId().equals(nodeId)) {
            CacheType cache = cacheProvider.get();
            Set<EntityProfileName> profilesToCollect = request.getProfilesToCollect();
            EntityProfileResponse.Builder builder = new EntityProfileResponse.Builder();
            if (profilesToCollect.contains(EntityProfileName.ENTITY_INFO)) {
                builder.setActive(cache.isActive(adID, modelId));
                builder.setLastActiveMs(cache.getLastActiveTime(adID, modelId));
            }
            if (profilesToCollect.contains(EntityProfileName.INIT_PROGRESS) || profilesToCollect.contains(EntityProfileName.STATE)) {
                builder.setTotalUpdates(cache.getTotalUpdates(adID, modelId));
            }
            if (profilesToCollect.contains(EntityProfileName.MODELS)) {
                Optional<ModelProfile> modleProfile = cache.getModelProfile(adID, modelId);
                if (modleProfile.isPresent()) {
                    builder.setModelProfile(new ModelProfileOnNode(nodeId, modleProfile.get()));
                }
            }
            listener.onResponse(builder.build());
        } else if (request.remoteAddress() == null) {
            // redirect if request comes from local host.
            // If a request comes from remote machine, it is already redirected.
            // One redirection should be enough.
            // We don't want a potential infinite loop due to any bug and thus give up.
            LOG.info("Sending entity profile request to {} for detector {}, entity {}", nodeId, adID, entityValue);

            try {
                transportService
                    .sendRequest(node.get(), entityProfileAction, request, option, new TransportResponseHandler<EntityProfileResponse>() {

                        @Override
                        public EntityProfileResponse read(StreamInput in) throws IOException {
                            return new EntityProfileResponse(in);
                        }

                        @Override
                        public void handleResponse(EntityProfileResponse response) {
                            listener.onResponse(response);
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            listener.onFailure(exp);
                        }

                        @Override
                        public String executor() {
                            return ThreadPool.Names.SAME;
                        }

                    });
            } catch (Exception e) {
                LOG.error(String.format(Locale.ROOT, "Fail to get entity profile for detector {}, entity {}", adID, entityValue), e);
                listener.onFailure(new TimeSeriesException(adID, FAIL_TO_GET_ENTITY_PROFILE_MSG, e));
            }

        } else {
            // Prior to Opensearch 1.1, we map a node using model id in the profile API.
            // This is not consistent how we map node in AnomalyResultTransportAction, where
            // we use entity values. We fixed that bug in Opensearch 1.1. But this can cause
            // issue when a request involving an old node according to model id.
            // The new node finds the entity value does not map to itself, so it redirects to another node.
            // The redirection can cause an infinite loop. This branch breaks the loop and gives up.
            LOG
                .error(
                    "Fail to get entity profile for detector {}, entity {}. Maybe because old and new node"
                        + " are using different keys for the hash ring.",
                    adID,
                    entityValue
                );
            listener.onFailure(new TimeSeriesException(adID, FAIL_TO_GET_ENTITY_PROFILE_MSG));
        }
    }
}
