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
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.settings.EnabledSetting;
import org.opensearch.ad.transport.IndexAnomalyDetectorRequest;
import org.opensearch.ad.transport.IndexAnomalyDetectorResponse;
import org.opensearch.ad.transport.IndexAnomalyDetectorSDKTransportAction;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.extensions.rest.ExtensionRestRequest;
import org.opensearch.extensions.rest.ExtensionRestResponse;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.RouteHandler;

import com.google.common.collect.ImmutableList;

/**
 * Rest handlers to create and update anomaly detector.
 */
public class RestSDKIndexAnomalyDetectorAction extends AbstractSDKAnomalyDetectorAction {

    private final Logger logger = LogManager.getLogger(RestSDKIndexAnomalyDetectorAction.class);
    private NamedXContentRegistry namedXContentRegistry;
    private Settings environmentSettings;
    private RestHighLevelClient restClient;

    public RestSDKIndexAnomalyDetectorAction(ExtensionsRunner extensionsRunner, AnomalyDetectorExtension anomalyDetectorExtension) {
        super(extensionsRunner.getEnvironmentSettings());
        this.namedXContentRegistry = extensionsRunner.getNamedXContentRegistry().getRegistry();
        this.environmentSettings = extensionsRunner.getEnvironmentSettings();
        this.restClient = anomalyDetectorExtension.getRestClient();
    }

    @Override
    public List<RouteHandler> routeHandlers() {
        return ImmutableList
            .of(
                // Create
                new RouteHandler(RestRequest.Method.POST, AnomalyDetectorPlugin.AD_BASE_DETECTORS_URI, handleRequest),
                // Update
                new RouteHandler(
                    RestRequest.Method.PUT,
                    String.format(Locale.ROOT, "%s/{%s}", AnomalyDetectorPlugin.AD_BASE_DETECTORS_URI, DETECTOR_ID),
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
        // TODO actually implement getActions which will take care of all this unused boilerplate

        // So here we call IndexAnomalyDetectorTransportAction.doExecute, SDK version
        IndexAnomalyDetectorSDKTransportAction indexAction = new IndexAnomalyDetectorSDKTransportAction(
            null, // TransportService transportService
            null, // ActionFilters actionFilters
            // Ignore this and substitute HLRC calls later
            restClient, // Client client
            // Disabled the settings update consumer that would cause NPE for this
            null, // ClusterService clusterService
            this.environmentSettings, // Settings settings
            null, // AnomalyDetectionIndices anomalyDetectionIndices
            this.namedXContentRegistry,
            null, // ADTaskManager adTaskManager
            null // SearchFeatureDao searchFeatureDao
        );

        CompletableFuture<IndexAnomalyDetectorResponse> futureResponse = new CompletableFuture<>();
        indexAction.doExecute(null, indexAnomalyDetectorRequest, new ActionListener<IndexAnomalyDetectorResponse>() {

            @Override
            public void onResponse(IndexAnomalyDetectorResponse response) {
                futureResponse.complete(response);
            }

            @Override
            public void onFailure(Exception e) {
                futureResponse.completeExceptionally(e);
            }

        });

        IndexAnomalyDetectorResponse response = futureResponse
            .orTimeout(AnomalyDetectorSettings.REQUEST_TIMEOUT.get(environmentSettings).getMillis(), TimeUnit.MILLISECONDS)
            .join();
        return indexAnomalyDetectorResponse(request, response);
    }

    private ExtensionRestResponse indexAnomalyDetectorResponse(ExtensionRestRequest request, IndexAnomalyDetectorResponse response)
        throws IOException {
        RestStatus restStatus = RestStatus.CREATED;
        if (request.method() == RestRequest.Method.PUT) {
            restStatus = RestStatus.OK;
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