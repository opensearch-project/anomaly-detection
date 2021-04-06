/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.ad.transport;

import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.REQUEST_TIMEOUT;
import static com.amazon.opendistroforelasticsearch.ad.util.ParseUtils.getUserContext;
import static com.amazon.opendistroforelasticsearch.ad.util.ParseUtils.resolveUserAndExecute;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.rest.handler.IndexAnomalyDetectorJobActionHandler;
import com.amazon.opendistroforelasticsearch.ad.task.ADTaskManager;
import com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils;
import com.amazon.opendistroforelasticsearch.commons.authuser.User;

public class AnomalyDetectorJobTransportAction extends HandledTransportAction<AnomalyDetectorJobRequest, AnomalyDetectorJobResponse> {
    private final Logger logger = LogManager.getLogger(AnomalyDetectorJobTransportAction.class);

    private final Client client;
    private final ClusterService clusterService;
    private final Settings settings;
    private final AnomalyDetectionIndices anomalyDetectionIndices;
    private final NamedXContentRegistry xContentRegistry;
    private volatile Boolean filterByEnabled;
    private final ADTaskManager adTaskManager;
    private final TransportService transportService;

    @Inject
    public AnomalyDetectorJobTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        ClusterService clusterService,
        Settings settings,
        AnomalyDetectionIndices anomalyDetectionIndices,
        NamedXContentRegistry xContentRegistry,
        ADTaskManager adTaskManager
    ) {
        super(AnomalyDetectorJobAction.NAME, transportService, actionFilters, AnomalyDetectorJobRequest::new);
        this.transportService = transportService;
        this.client = client;
        this.clusterService = clusterService;
        this.settings = settings;
        this.anomalyDetectionIndices = anomalyDetectionIndices;
        this.xContentRegistry = xContentRegistry;
        this.adTaskManager = adTaskManager;
        filterByEnabled = FILTER_BY_BACKEND_ROLES.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(FILTER_BY_BACKEND_ROLES, it -> filterByEnabled = it);
    }

    @Override
    protected void doExecute(Task task, AnomalyDetectorJobRequest request, ActionListener<AnomalyDetectorJobResponse> listener) {
        String detectorId = request.getDetectorID();
        long seqNo = request.getSeqNo();
        long primaryTerm = request.getPrimaryTerm();
        String rawPath = request.getRawPath();
        TimeValue requestTimeout = REQUEST_TIMEOUT.get(settings);

        // By the time request reaches here, the user permissions are validated by Security plugin.
        User user = getUserContext(client);
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            resolveUserAndExecute(
                user,
                detectorId,
                filterByEnabled,
                listener,
                () -> executeDetector(listener, detectorId, seqNo, primaryTerm, rawPath, requestTimeout, user),
                client,
                clusterService,
                xContentRegistry
            );
        } catch (Exception e) {
            logger.error(e);
            listener.onFailure(e);
        }
    }

    private void executeDetector(
        ActionListener<AnomalyDetectorJobResponse> listener,
        String detectorId,
        long seqNo,
        long primaryTerm,
        String rawPath,
        TimeValue requestTimeout,
        User user
    ) {
        IndexAnomalyDetectorJobActionHandler handler = new IndexAnomalyDetectorJobActionHandler(
            client,
            listener,
            anomalyDetectionIndices,
            detectorId,
            seqNo,
            primaryTerm,
            requestTimeout,
            xContentRegistry
        );
        if (rawPath.endsWith(RestHandlerUtils.START_JOB)) {
            adTaskManager.startDetector(detectorId, handler, user, transportService, listener);
        } else if (rawPath.endsWith(RestHandlerUtils.STOP_JOB)) {
            adTaskManager.stopDetector(detectorId, handler, user, transportService, listener);
        }
    }
}
