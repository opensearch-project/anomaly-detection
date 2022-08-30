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

/*
package org.opensearch.ad.mock.transport;


public class MockAnomalyDetectorJobTransportActionWithUser extends
    HandledTransportAction<AnomalyDetectorJobRequest, AnomalyDetectorJobResponse> {
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
    public MockAnomalyDetectorJobTransportActionWithUser(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        ClusterService clusterService,
        Settings settings,
        AnomalyDetectionIndices anomalyDetectionIndices,
        NamedXContentRegistry xContentRegistry,
        ADTaskManager adTaskManager
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
        UserIdentity user = UserIdentity.parse(userStr);
        try {
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
        UserIdentity user,
        DetectionDateRange detectionDateRange,
        boolean historical
    ) {
        IndexAnomalyDetectorJobActionHandler handler = new IndexAnomalyDetectorJobActionHandler(
            client,
            listener,
            anomalyDetectionIndices,
            detectorId,
            seqNo,
            primaryTerm,
            requestTimeout,
            xContentRegistry,
            transportService,
            adTaskManager
        );
        if (rawPath.endsWith(RestHandlerUtils.START_JOB)) {
            adTaskManager.startDetector(detectorId, detectionDateRange, handler, user, transportService, listener);
        } else if (rawPath.endsWith(RestHandlerUtils.STOP_JOB)) {
            // Stop detector
            adTaskManager.stopDetector(detectorId, historical, handler, user, transportService, listener);
        }
    }
}
*/
