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

package org.opensearch.ad.transport;

import static org.opensearch.ad.settings.AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_ANOMALY_FEATURES;
import static org.opensearch.ad.util.ParseUtils.getUserContext;
import static org.opensearch.ad.util.ParseUtils.resolveUserAndExecute;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.ad.AnomalyDetectorRunner;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.util.RestHandlerUtils;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.commons.authuser.User;
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

    @Inject
    public PreviewAnomalyDetectorTransportAction(
        Settings settings,
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        Client client,
        AnomalyDetectorRunner anomalyDetectorRunner,
        NamedXContentRegistry xContentRegistry
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
    }

    @Override
    protected void doExecute(Task task, PreviewAnomalyDetectorRequest request, ActionListener<PreviewAnomalyDetectorResponse> listener) {
        String detectorId = request.getDetectorId();
        User user = getUserContext(client);
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            resolveUserAndExecute(
                user,
                detectorId,
                filterByEnabled,
                listener,
                () -> previewExecute(request, context, listener),
                client,
                clusterService,
                xContentRegistry
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
        try {
            AnomalyDetector detector = request.getDetector();
            String detectorId = request.getDetectorId();
            Instant startTime = request.getStartTime();
            Instant endTime = request.getEndTime();
            if (detector != null) {
                String error = validateDetector(detector);
                if (StringUtils.isNotBlank(error)) {
                    listener.onFailure(new OpenSearchException(error, RestStatus.BAD_REQUEST));
                    return;
                }
                anomalyDetectorRunner
                    .executeDetector(detector, startTime, endTime, context, getPreviewDetectorActionListener(listener, detector));
            } else {
                previewAnomalyDetector(listener, detectorId, startTime, endTime, context);
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
            return RestHandlerUtils.validateAnomalyDetector(detector, maxAnomalyFeatures);
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
                    new OpenSearchException(
                        "Unexpected error running anomaly detector " + detector.getDetectorId() + ". " + exception.getMessage(),
                        RestStatus.INTERNAL_SERVER_ERROR
                    )
                );
        });
    }

    private void previewAnomalyDetector(
        ActionListener<PreviewAnomalyDetectorResponse> listener,
        String detectorId,
        Instant startTime,
        Instant endTime,
        ThreadContext.StoredContext context
    ) {
        if (!StringUtils.isBlank(detectorId)) {
            GetRequest getRequest = new GetRequest(AnomalyDetector.ANOMALY_DETECTORS_INDEX).id(detectorId);
            client.get(getRequest, onGetAnomalyDetectorResponse(listener, startTime, endTime, context));
        } else {
            listener.onFailure(new OpenSearchException("Wrong input, no detector id", RestStatus.BAD_REQUEST));
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
                            new OpenSearchException("Can't find anomaly detector with id:" + response.getId(), RestStatus.NOT_FOUND)
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
        }, exception -> { listener.onFailure(new OpenSearchException("Could not execute get query to find detector")); });
    }
}
