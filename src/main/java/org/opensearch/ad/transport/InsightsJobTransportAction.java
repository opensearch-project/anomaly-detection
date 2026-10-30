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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.rest.handler.InsightsJobActionHandler;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.tasks.Task;
import org.opensearch.timeseries.transport.InsightsJobRequest;
import org.opensearch.timeseries.util.PluginClient;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

public class InsightsJobTransportAction extends HandledTransportAction<InsightsJobRequest, InsightsJobResponse> {
    private static final Logger log = LogManager.getLogger(InsightsJobTransportAction.class);

    private final Client client;
    private final InsightsJobActionHandler jobHandler;

    @Inject
    public InsightsJobTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        PluginClient pluginClient,
        ClusterService clusterService,
        Settings settings,
        NamedXContentRegistry xContentRegistry,
        ADIndexManagement indexManagement
    ) {
        super(InsightsJobAction.NAME, transportService, actionFilters, InsightsJobRequest::new);
        this.client = client;
        this.jobHandler = new InsightsJobActionHandler(
            client,
            pluginClient,
            xContentRegistry,
            indexManagement,
            settings,
            AnomalyDetectorSettings.AD_REQUEST_TIMEOUT.get(settings)
        );
    }

    @Override
    protected void doExecute(Task task, InsightsJobRequest request, ActionListener<InsightsJobResponse> listener) {
        if (request.isStartOperation()) {
            handleStartOperation(request, listener);
        } else if (request.isStatusOperation()) {
            handleStatusOperation(request, listener);
        } else if (request.isStopOperation()) {
            handleStopOperation(request, listener);
        } else {
            listener.onFailure(new IllegalArgumentException("Unknown operation"));
        }
    }

    private void handleStartOperation(InsightsJobRequest request, ActionListener<InsightsJobResponse> listener) {
        log.info("Starting insights job with frequency: {}", request.getFrequency());

        jobHandler.startInsightsJob(request.getFrequency(), listener);
    }

    private void handleStatusOperation(InsightsJobRequest request, ActionListener<InsightsJobResponse> listener) {
        jobHandler.getInsightsJobStatus(listener);
    }

    private void handleStopOperation(InsightsJobRequest request, ActionListener<InsightsJobResponse> listener) {
        jobHandler.stopInsightsJob(listener);
    }
}
