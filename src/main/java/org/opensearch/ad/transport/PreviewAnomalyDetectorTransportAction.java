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

import static org.opensearch.ad.constant.CommonErrorMessages.FAIL_TO_PREVIEW_DETECTOR;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_ANOMALY_FEATURES;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_CONCURRENT_PREVIEW;
import static org.opensearch.ad.util.ParseUtils.getNullUser;
import static org.opensearch.ad.util.ParseUtils.resolveUserAndExecute;
import static org.opensearch.ad.util.RestHandlerUtils.wrapRestActionListener;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Semaphore;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.ad.AnomalyDetectorRunner;
import org.opensearch.ad.auth.UserIdentity;
import org.opensearch.ad.breaker.ADCircuitBreakerService;
import org.opensearch.ad.common.exception.AnomalyDetectionException;
import org.opensearch.ad.common.exception.ClientException;
import org.opensearch.ad.common.exception.LimitExceededException;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.util.RestHandlerUtils;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.RestStatus;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class PreviewAnomalyDetectorTransportAction extends
    HandledTransportAction<PreviewAnomalyDetectorRequest, PreviewAnomalyDetectorResponse> {
    private final Logger logger = LogManager.getLogger(PreviewAnomalyDetectorTransportAction.class);
    private final AnomalyDetectorRunner anomalyDetectorRunner;
    private final ClusterService clusterService;
    private final Client client;
    private final NamedXContentRegistry xContentRegistry;
    private volatile Integer maxAnomalyFeatures;
    private volatile Boolean filterByEnabled;
    private final ADCircuitBreakerService adCircuitBreakerService;
    private Semaphore lock;

    @Inject
    public PreviewAnomalyDetectorTransportAction(
        Settings settings,
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        Client client,
        AnomalyDetectorRunner anomalyDetectorRunner,
        NamedXContentRegistry xContentRegistry,
        ADCircuitBreakerService adCircuitBreakerService
    ) {
        super(PreviewAnomalyDetectorAction.NAME, transportService, actionFilters, PreviewAnomalyDetectorRequest::new);
        this.clusterService = clusterService;
        this.client = client;
        this.anomalyDetectorRunner = anomalyDetectorRunner;
        this.xContentRegistry = xContentRegistry;
        maxAnomalyFeatures = MAX_ANOMALY_FEATURES.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_ANOMALY_FEATURES, it -> maxAnomalyFeatures = it);
        filterByEnabled = AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(FILTER_BY_BACKEND_ROLES, it -> filterByEnabled = it);
        this.adCircuitBreakerService = adCircuitBreakerService;
        this.lock = new Semaphore(MAX_CONCURRENT_PREVIEW.get(settings), true);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_CONCURRENT_PREVIEW, it -> { lock = new Semaphore(it); });
    }

    @Override
    protected void doExecute(
        Task task,
        PreviewAnomalyDetectorRequest request,
        ActionListener<PreviewAnomalyDetectorResponse> actionListener
    ) {
        String detectorId = request.getDetectorId();
        // Temporary null user for AD extension without security. Will always execute detector.
        UserIdentity user = getNullUser();
        ActionListener<PreviewAnomalyDetectorResponse> listener = wrapRestActionListener(actionListener, FAIL_TO_PREVIEW_DETECTOR);
        try {
            resolveUserAndExecute(
                user,
                detectorId,
                filterByEnabled,
                listener,
                (anomalyDetector) -> previewExecute(request, listener),
                // TODO: Switch these to SDKRestClient and SDKClusterService when implementing this
                null, // client,
                null, // clusterService,
                xContentRegistry
            );
        } catch (Exception e) {
            logger.error(e);
            listener.onFailure(e);
        }
    }

    void previewExecute(PreviewAnomalyDetectorRequest request, ActionListener<PreviewAnomalyDetectorResponse> listener) {
        if (adCircuitBreakerService.isOpen()) {
            listener
                .onFailure(new LimitExceededException(request.getDetectorId(), CommonErrorMessages.MEMORY_CIRCUIT_BROKEN_ERR_MSG, false));
            return;
        }
        try {
            if (!lock.tryAcquire()) {
                listener.onFailure(new ClientException(request.getDetectorId(), CommonErrorMessages.REQUEST_THROTTLED_MSG));
                return;
            }

            try {
                AnomalyDetector detector = request.getDetector();
                String detectorId = request.getDetectorId();
                Instant startTime = request.getStartTime();
                Instant endTime = request.getEndTime();
                ActionListener<PreviewAnomalyDetectorResponse> releaseListener = ActionListener.runAfter(listener, () -> lock.release());
                if (detector != null) {
                    String error = validateDetector(detector);
                    if (StringUtils.isNotBlank(error)) {
                        listener.onFailure(new OpenSearchStatusException(error, RestStatus.BAD_REQUEST));
                        lock.release();
                        return;
                    }
                    anomalyDetectorRunner
                        .executeDetector(detector, startTime, endTime, getPreviewDetectorActionListener(releaseListener, detector));
                } else {
                    previewAnomalyDetector(releaseListener, detectorId, detector, startTime, endTime);
                }
            } catch (Exception e) {
                logger.error("Fail to preview", e);
                lock.release();
            }
        } catch (Exception e) {
            logger.error(e);
            listener.onFailure(e);
        }
    }

    private String validateDetector(AnomalyDetector detector) {
        if (detector.getFeatureAttributes().isEmpty()) {
            return "Can't preview detector without feature";
        } else {
            return RestHandlerUtils.checkAnomalyDetectorFeaturesSyntax(detector, maxAnomalyFeatures);
        }
    }

    private ActionListener<List<AnomalyResult>> getPreviewDetectorActionListener(
        ActionListener<PreviewAnomalyDetectorResponse> listener,
        AnomalyDetector detector
    ) {
        return ActionListener.wrap(new CheckedConsumer<List<AnomalyResult>, Exception>() {
            @Override
            public void accept(List<AnomalyResult> anomalyResult) throws Exception {
                PreviewAnomalyDetectorResponse response = new PreviewAnomalyDetectorResponse(anomalyResult, detector);
                listener.onResponse(response);
            }
        }, exception -> {
            logger.error("Unexpected error running anomaly detector " + detector.getDetectorId(), exception);
            listener
                .onFailure(
                    new OpenSearchStatusException(
                        "Unexpected error running anomaly detector " + detector.getDetectorId() + ". " + exception.getMessage(),
                        RestStatus.INTERNAL_SERVER_ERROR
                    )
                );
        });
    }

    private void previewAnomalyDetector(
        ActionListener<PreviewAnomalyDetectorResponse> listener,
        String detectorId,
        AnomalyDetector detector,
        Instant startTime,
        Instant endTime
    ) throws IOException {
        if (!StringUtils.isBlank(detectorId)) {
            GetRequest getRequest = new GetRequest(AnomalyDetector.ANOMALY_DETECTORS_INDEX).id(detectorId);
            client.get(getRequest, onGetAnomalyDetectorResponse(listener, startTime, endTime));
        } else {
            anomalyDetectorRunner.executeDetector(detector, startTime, endTime, getPreviewDetectorActionListener(listener, detector));
        }
    }

    private ActionListener<GetResponse> onGetAnomalyDetectorResponse(
        ActionListener<PreviewAnomalyDetectorResponse> listener,
        Instant startTime,
        Instant endTime
    ) {
        return ActionListener.wrap(new CheckedConsumer<GetResponse, Exception>() {
            @Override
            public void accept(GetResponse response) throws Exception {
                if (!response.isExists()) {
                    listener
                        .onFailure(
                            new OpenSearchStatusException("Can't find anomaly detector with id:" + response.getId(), RestStatus.NOT_FOUND)
                        );
                    return;
                }

                try {
                    XContentParser parser = RestHandlerUtils
                        .createXContentParserFromRegistry(xContentRegistry, response.getSourceAsBytesRef());
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                    AnomalyDetector detector = AnomalyDetector.parse(parser, response.getId(), response.getVersion());

                    anomalyDetectorRunner
                        .executeDetector(detector, startTime, endTime, getPreviewDetectorActionListener(listener, detector));
                } catch (IOException e) {
                    listener.onFailure(e);
                }
            }
        }, exception -> { listener.onFailure(new AnomalyDetectionException("Could not execute get query to find detector")); });
    }
}
