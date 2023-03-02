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

import static org.opensearch.ad.constant.CommonErrorMessages.FAIL_TO_START_DETECTOR;
import static org.opensearch.ad.constant.CommonErrorMessages.FAIL_TO_STOP_DETECTOR;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.REQUEST_TIMEOUT;
import static org.opensearch.ad.util.ParseUtils.getNullUser;
import static org.opensearch.ad.util.ParseUtils.resolveUserAndExecute;
import static org.opensearch.ad.util.RestHandlerUtils.wrapRestActionListener;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.TransportAction;
import org.opensearch.ad.auth.UserIdentity;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.model.DetectionDateRange;
import org.opensearch.ad.rest.handler.IndexAnomalyDetectorJobActionHandler;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.util.RestHandlerUtils;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.SDKClient.SDKRestClient;
import org.opensearch.sdk.SDKClusterService;
import org.opensearch.sdk.SDKNamedXContentRegistry;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;
import org.opensearch.transport.TransportService;

public class AnomalyDetectorJobTransportAction extends TransportAction<AnomalyDetectorJobRequest, AnomalyDetectorJobResponse> {
    private final Logger logger = LogManager.getLogger(AnomalyDetectorJobTransportAction.class);

    private final SDKRestClient client;
    private final SDKClusterService clusterService;
    private final Settings settings;
    private final AnomalyDetectionIndices anomalyDetectionIndices;
    private final SDKNamedXContentRegistry xContentRegistry;
    private volatile Boolean filterByEnabled;
    private final ADTaskManager adTaskManager;
    private final TransportService transportService;

    @Inject
    public AnomalyDetectorJobTransportAction(
        ExtensionsRunner extensionsRunner,
        ActionFilters actionFilters,
        TaskManager taskManager,
        SDKRestClient client,
        SDKClusterService clusterService,
        Settings settings,
        AnomalyDetectionIndices anomalyDetectionIndices,
        SDKNamedXContentRegistry xContentRegistry,
        ADTaskManager adTaskManager
    ) {
        super(AnomalyDetectorJobAction.NAME, actionFilters, taskManager);
        this.transportService = extensionsRunner.getExtensionTransportService();
        this.client = client;
        this.clusterService = clusterService;
        this.settings = settings;
        this.anomalyDetectionIndices = anomalyDetectionIndices;
        this.xContentRegistry = xContentRegistry;
        this.adTaskManager = adTaskManager;
        filterByEnabled = FILTER_BY_BACKEND_ROLES.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(FILTER_BY_BACKEND_ROLES, it -> filterByEnabled = it);
    }

    public void doExecute(Task task, AnomalyDetectorJobRequest request, ActionListener<AnomalyDetectorJobResponse> actionListener) {
        String detectorId = request.getDetectorID();
        DetectionDateRange detectionDateRange = request.getDetectionDateRange();
        boolean historical = request.isHistorical();
        long seqNo = request.getSeqNo();
        long primaryTerm = request.getPrimaryTerm();
        String rawPath = request.getRawPath();
        TimeValue requestTimeout = REQUEST_TIMEOUT.get(settings);
        String errorMessage = rawPath.endsWith(RestHandlerUtils.START_JOB) ? FAIL_TO_START_DETECTOR : FAIL_TO_STOP_DETECTOR;
        ActionListener<AnomalyDetectorJobResponse> listener = wrapRestActionListener(actionListener, errorMessage);

        // By the time request reaches here, the user permissions are validated by Security plugin.
        // Temporary null user for AD extension without security. Will always execute detector.
        UserIdentity user = getNullUser();
        try {
            resolveUserAndExecute(
                user,
                detectorId,
                filterByEnabled,
                listener,
                (anomalyDetector) -> executeDetector(
                    listener,
                    detectorId,
                    detectionDateRange,
                    historical,
                    seqNo,
                    primaryTerm,
                    rawPath,
                    requestTimeout,
                    user
                ),
                client,
                clusterService,
                xContentRegistry.getRegistry()
            );
        } catch (Exception e) {
            logger.error(e);
            listener.onFailure(e);
        }
    }

    private void executeDetector(
        ActionListener<AnomalyDetectorJobResponse> listener,
        String detectorId,
        DetectionDateRange detectionDateRange,
        boolean historical,
        long seqNo,
        long primaryTerm,
        String rawPath,
        TimeValue requestTimeout,
        UserIdentity user
    ) {
        IndexAnomalyDetectorJobActionHandler handler = new IndexAnomalyDetectorJobActionHandler(
            client,
            listener,
            anomalyDetectionIndices,
            detectorId,
            seqNo,
            primaryTerm,
            requestTimeout,
            xContentRegistry.getRegistry(),
            transportService,
            adTaskManager
        );
        if (rawPath.endsWith(RestHandlerUtils.START_JOB)) {
            adTaskManager.startDetector(detectorId, detectionDateRange, handler, user, transportService, listener);
        }
        // TODO : https://github.com/opensearch-project/opensearch-sdk-java/issues/384
        // else if (rawPath.endsWith(RestHandlerUtils.STOP_JOB)) {
        // adTaskManager.stopDetector(detectorId, historical, handler, user, transportService, listener);
        // }
    }
}
