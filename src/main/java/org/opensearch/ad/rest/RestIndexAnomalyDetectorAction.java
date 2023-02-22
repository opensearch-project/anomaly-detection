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

package org.opensearch.ad.rest;

import static org.opensearch.ad.util.RestHandlerUtils.DETECTOR_ID;
import static org.opensearch.ad.util.RestHandlerUtils.IF_PRIMARY_TERM;
import static org.opensearch.ad.util.RestHandlerUtils.IF_SEQ_NO;
import static org.opensearch.ad.util.RestHandlerUtils.REFRESH;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.ad.AnomalyDetectorExtension;
import org.opensearch.ad.AnomalyDetectorPlugin;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.feature.SearchFeatureDao;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.settings.EnabledSetting;
import org.opensearch.ad.transport.IndexAnomalyDetectorRequest;
import org.opensearch.ad.transport.IndexAnomalyDetectorResponse;
import org.opensearch.ad.transport.IndexAnomalyDetectorTransportAction;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.extensions.rest.ExtensionRestRequest;
import org.opensearch.extensions.rest.ExtensionRestResponse;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.RouteHandler;
import org.opensearch.sdk.SDKClient.SDKRestClient;
import org.opensearch.sdk.SDKClusterService;
import org.opensearch.transport.TransportService;

import com.google.common.collect.ImmutableList;

/**
 * Rest handlers to create and update anomaly detector.
 */
public class RestIndexAnomalyDetectorAction extends AbstractAnomalyDetectorAction {

    private static final String INDEX_ANOMALY_DETECTOR_ACTION = "index_anomaly_detector_action";
    private final Logger logger = LogManager.getLogger(RestIndexAnomalyDetectorAction.class);
    private NamedXContentRegistry namedXContentRegistry;
    private Settings environmentSettings;
    private TransportService transportService;
    private SDKRestClient restClient;
    private SDKClusterService sdkClusterService;

    public RestIndexAnomalyDetectorAction(ExtensionsRunner extensionsRunner, AnomalyDetectorExtension anomalyDetectorExtension) {
        super(extensionsRunner);
        this.namedXContentRegistry = extensionsRunner.getNamedXContentRegistry().getRegistry();
        this.environmentSettings = extensionsRunner.getEnvironmentSettings();
        this.transportService = extensionsRunner.getExtensionTransportService();
        this.restClient = anomalyDetectorExtension.getRestClient();
        this.sdkClusterService = new SDKClusterService(extensionsRunner);
    }

    // @Override
    public String getName() {
        return INDEX_ANOMALY_DETECTOR_ACTION;
    }

    @Override
    public List<RouteHandler> routeHandlers() {
        return ImmutableList
            .of(
                // Create
                new RouteHandler(RestRequest.Method.POST, AnomalyDetectorExtension.AD_BASE_DETECTORS_URI, handleRequest),
                // Update
                new RouteHandler(
                    RestRequest.Method.PUT,
                    String.format(Locale.ROOT, "%s/{%s}", AnomalyDetectorExtension.AD_BASE_DETECTORS_URI, DETECTOR_ID),
                    handleRequest
                )
            );
    }

    private Function<ExtensionRestRequest, ExtensionRestResponse> handleRequest = (request) -> {
        try {
            return prepareRequest(request);
        } catch (Exception e) {
            // TODO: handle the AD-specific exceptions separately
            return exceptionalRequest(request, e);
        }
    };

    protected ExtensionRestResponse prepareRequest(ExtensionRestRequest request) throws Exception {
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new IllegalStateException(CommonErrorMessages.DISABLED_ERR_MSG);
        }

        String detectorId = request.param(DETECTOR_ID, AnomalyDetector.NO_ID);
        logger.info("AnomalyDetector {} action for detectorId {}", request.method(), detectorId);

        XContentParser parser = request.contentParser(this.namedXContentRegistry);
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        // TODO: check detection interval < modelTTL
        AnomalyDetector detector = AnomalyDetector.parse(parser, detectorId, null, detectionInterval, detectionWindowDelay);

        long seqNo = request.paramAsLong(IF_SEQ_NO, SequenceNumbers.UNASSIGNED_SEQ_NO);
        long primaryTerm = request.paramAsLong(IF_PRIMARY_TERM, SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
        WriteRequest.RefreshPolicy refreshPolicy = request.hasParam(REFRESH)
            ? WriteRequest.RefreshPolicy.parse(request.param(REFRESH))
            : WriteRequest.RefreshPolicy.IMMEDIATE;
        RestRequest.Method method = request.method();

        IndexAnomalyDetectorRequest indexAnomalyDetectorRequest = new IndexAnomalyDetectorRequest(
            detectorId,
            seqNo,
            primaryTerm,
            refreshPolicy,
            detector,
            method,
            requestTimeout,
            maxSingleEntityDetectors,
            maxMultiEntityDetectors,
            maxAnomalyFeatures
        );

        // Here we would call client.execute(action, request, responseListener)
        // This delegates to transportAction(action).execute(request, responseListener)
        // IndexAnomalyDetectorAction is the key to the getActions map
        // IndexAnomalyDetectorTransportAction is the value, execute() calls doExecute()

        IndexAnomalyDetectorTransportAction indexAction = new IndexAnomalyDetectorTransportAction(
            transportService,
            null, // ActionFilters actionFilters
            restClient, // Client client
            sdkClusterService, // ClusterService clusterService,
            this.environmentSettings, // Settings settings
            new AnomalyDetectionIndices(
                restClient, // client,
                sdkClusterService, // clusterService,
                null, // threadPool,
                this.environmentSettings, // settings,
                null, // nodeFilter,
                AnomalyDetectorSettings.MAX_UPDATE_RETRY_TIMES
            ), // AnomalyDetectionIndices anomalyDetectionIndices
            this.namedXContentRegistry,
            null, // ADTaskManager adTaskManager
            new SearchFeatureDao(
                restClient,
                namedXContentRegistry,
                null, // interpolator
                null, // clientUtil,
                environmentSettings,
                sdkClusterService,
                maxAnomalyFeatures
            )
        );

        CompletableFuture<IndexAnomalyDetectorResponse> futureResponse = new CompletableFuture<>();
        indexAction
            .doExecute(
                null,
                indexAnomalyDetectorRequest,
                ActionListener.wrap(r -> futureResponse.complete(r), e -> futureResponse.completeExceptionally(e))
            );

        IndexAnomalyDetectorResponse response = futureResponse
            .orTimeout(AnomalyDetectorSettings.REQUEST_TIMEOUT.get(environmentSettings).getMillis(), TimeUnit.MILLISECONDS)
            .join();
        // TODO handle exceptional response
        return indexAnomalyDetectorResponse(request, response);
    }

    private ExtensionRestResponse indexAnomalyDetectorResponse(ExtensionRestRequest request, IndexAnomalyDetectorResponse response)
        throws IOException {
        RestStatus restStatus = RestStatus.CREATED;
        if (request.method() == RestRequest.Method.PUT) {
            restStatus = RestStatus.OK;
        } else {
            logger.info("Detector ID: {}", response.getId());
        }
        ExtensionRestResponse extensionRestResponse = new ExtensionRestResponse(
            request,
            restStatus,
            response.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS)
        );
        if (restStatus == RestStatus.CREATED) {
            String location = String.format(Locale.ROOT, "%s/%s", AnomalyDetectorPlugin.LEGACY_AD_BASE, response.getId());
            extensionRestResponse.addHeader("Location", location);
        }
        return extensionRestResponse;
    }
}
