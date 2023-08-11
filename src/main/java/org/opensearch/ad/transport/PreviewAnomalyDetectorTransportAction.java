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

import static org.opensearch.ad.constant.ADCommonMessages.FAIL_TO_PREVIEW_DETECTOR;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_ANOMALY_FEATURES;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_CONCURRENT_PREVIEW;
import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.timeseries.util.ParseUtils.getUserContext;
import static org.opensearch.timeseries.util.ParseUtils.resolveUserAndExecute;
import static org.opensearch.timeseries.util.RestHandlerUtils.wrapRestActionListener;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Semaphore;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.ad.AnomalyDetectorRunner;
import org.opensearch.ad.breaker.ADCircuitBreakerService;
import org.opensearch.ad.constant.ADCommonMessages;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.tasks.Task;
import org.opensearch.timeseries.common.exception.ClientException;
import org.opensearch.timeseries.common.exception.LimitExceededException;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.util.RestHandlerUtils;
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
        String detectorId = request.getId();
        User user = getUserContext(client);
        ActionListener<PreviewAnomalyDetectorResponse> listener = wrapRestActionListener(actionListener, FAIL_TO_PREVIEW_DETECTOR);
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            resolveUserAndExecute(
                user,
                detectorId,
                filterByEnabled,
                listener,
                (anomalyDetector) -> previewExecute(request, context, listener),
                client,
                clusterService,
                xContentRegistry,
                AnomalyDetector.class
            );
        } catch (Exception e) {
            logger.error(e);
            listener.onFailure(e);
        }
    }

    void previewExecute(
        PreviewAnomalyDetectorRequest request,
        ThreadContext.StoredContext context,
        ActionListener<PreviewAnomalyDetectorResponse> listener
    ) {
        if (adCircuitBreakerService.isOpen()) {
            listener.onFailure(new LimitExceededException(request.getId(), CommonMessages.MEMORY_CIRCUIT_BROKEN_ERR_MSG, false));
            return;
        }
        try {
            if (!lock.tryAcquire()) {
                listener.onFailure(new ClientException(request.getId(), ADCommonMessages.REQUEST_THROTTLED_MSG));
                return;
            }

            try {
                AnomalyDetector detector = request.getDetector();
                String detectorId = request.getId();
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
                        .executeDetector(
                            detector,
                            startTime,
                            endTime,
                            context,
                            getPreviewDetectorActionListener(releaseListener, detector)
                        );
                } else {
                    previewAnomalyDetector(releaseListener, detectorId, detector, startTime, endTime, context);
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
            return RestHandlerUtils.checkFeaturesSyntax(detector, maxAnomalyFeatures);
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
            logger.error("Unexpected error running anomaly detector " + detector.getId(), exception);
            listener
                .onFailure(
                    new OpenSearchStatusException(
                        "Unexpected error running anomaly detector " + detector.getId() + ". " + exception.getMessage(),
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
        Instant endTime,
        ThreadContext.StoredContext context
    ) throws IOException {
        if (!StringUtils.isBlank(detectorId)) {
            GetRequest getRequest = new GetRequest(CommonName.CONFIG_INDEX).id(detectorId);
            client.get(getRequest, onGetAnomalyDetectorResponse(listener, startTime, endTime, context));
        } else {
            anomalyDetectorRunner
                .executeDetector(detector, startTime, endTime, context, getPreviewDetectorActionListener(listener, detector));
        }
    }

    private ActionListener<GetResponse> onGetAnomalyDetectorResponse(
        ActionListener<PreviewAnomalyDetectorResponse> listener,
        Instant startTime,
        Instant endTime,
        ThreadContext.StoredContext context
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
                        .executeDetector(detector, startTime, endTime, context, getPreviewDetectorActionListener(listener, detector));
                } catch (IOException e) {
                    listener.onFailure(e);
                }
            }
        }, exception -> { listener.onFailure(new TimeSeriesException("Could not execute get query to find detector")); });
    }
}
