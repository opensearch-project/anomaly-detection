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

package org.opensearch.ad.mock.transport;

import static org.opensearch.ad.settings.AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.REQUEST_TIMEOUT;
import static org.opensearch.ad.util.ParseUtils.resolveUserAndExecute;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.ad.ExecuteADResultResponseRecorder;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.model.DetectionDateRange;
import org.opensearch.ad.rest.handler.IndexAnomalyDetectorJobActionHandler;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.transport.AnomalyDetectorJobRequest;
import org.opensearch.ad.transport.AnomalyDetectorJobResponse;
import org.opensearch.ad.transport.AnomalyDetectorJobTransportAction;
import org.opensearch.ad.util.RestHandlerUtils;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.commons.authuser.User;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class MockAnomalyDetectorJobTransportActionWithUser extends
    HandledTransportAction<AnomalyDetectorJobRequest, AnomalyDetectorJobResponse> {
    private final Logger logger = LogManager.getLogger(AnomalyDetectorJobTransportAction.class);

    private final Client client;
    private final ClusterService clusterService;
    private final Settings settings;
    private final AnomalyDetectionIndices anomalyDetectionIndices;
    private final NamedXContentRegistry xContentRegistry;
    private volatile Boolean filterByEnabled;
    private ThreadContext.StoredContext context;
    private final ADTaskManager adTaskManager;
    private final TransportService transportService;
    private final ExecuteADResultResponseRecorder recorder;

    @Inject
    public MockAnomalyDetectorJobTransportActionWithUser(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        ClusterService clusterService,
        Settings settings,
        AnomalyDetectionIndices anomalyDetectionIndices,
        NamedXContentRegistry xContentRegistry,
        ADTaskManager adTaskManager,
        ExecuteADResultResponseRecorder recorder
    ) {
        super(MockAnomalyDetectorJobAction.NAME, transportService, actionFilters, AnomalyDetectorJobRequest::new);
        this.transportService = transportService;
        this.client = client;
        this.clusterService = clusterService;
        this.settings = settings;
        this.anomalyDetectionIndices = anomalyDetectionIndices;
        this.xContentRegistry = xContentRegistry;
        this.adTaskManager = adTaskManager;
        filterByEnabled = FILTER_BY_BACKEND_ROLES.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(FILTER_BY_BACKEND_ROLES, it -> filterByEnabled = it);

        ThreadContext threadContext = new ThreadContext(settings);
        context = threadContext.stashContext();
        this.recorder = recorder;
    }

    @Override
    protected void doExecute(Task task, AnomalyDetectorJobRequest request, ActionListener<AnomalyDetectorJobResponse> listener) {
        String detectorId = request.getDetectorID();
        DetectionDateRange detectionDateRange = request.getDetectionDateRange();
        boolean historical = request.isHistorical();
        long seqNo = request.getSeqNo();
        long primaryTerm = request.getPrimaryTerm();
        String rawPath = request.getRawPath();
        TimeValue requestTimeout = REQUEST_TIMEOUT.get(settings);
        String userStr = "user_name|backendrole1,backendrole2|roles1,role2";
        // By the time request reaches here, the user permissions are validated by Security plugin.
        User user = User.parse(userStr);
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            resolveUserAndExecute(
                user,
                detectorId,
                filterByEnabled,
                listener,
                (anomalyDetector) -> executeDetector(
                    listener,
                    detectorId,
                    seqNo,
                    primaryTerm,
                    rawPath,
                    requestTimeout,
                    user,
                    detectionDateRange,
                    historical
                ),
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
        User user,
        DetectionDateRange detectionDateRange,
        boolean historical
    ) {
        IndexAnomalyDetectorJobActionHandler handler = new IndexAnomalyDetectorJobActionHandler(
            client,
            anomalyDetectionIndices,
            detectorId,
            seqNo,
            primaryTerm,
            requestTimeout,
            xContentRegistry,
            transportService,
            adTaskManager,
            recorder
        );
        if (rawPath.endsWith(RestHandlerUtils.START_JOB)) {
            adTaskManager.startDetector(detectorId, detectionDateRange, handler, user, transportService, context, listener);
        } else if (rawPath.endsWith(RestHandlerUtils.STOP_JOB)) {
            // Stop detector
            adTaskManager.stopDetector(detectorId, historical, handler, user, transportService, listener);
        }
    }
}
