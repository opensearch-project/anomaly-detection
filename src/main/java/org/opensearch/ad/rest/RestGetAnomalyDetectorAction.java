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
import static org.opensearch.ad.util.RestHandlerUtils.PROFILE;
import static org.opensearch.ad.util.RestHandlerUtils.TYPE;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.ad.AnomalyDetectorExtension;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.model.Entity;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.settings.EnabledSetting;
import org.opensearch.ad.transport.GetAnomalyDetectorAction;
import org.opensearch.ad.transport.GetAnomalyDetectorRequest;
import org.opensearch.ad.transport.GetAnomalyDetectorResponse;
import org.opensearch.common.Strings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.extensions.rest.ExtensionRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.rest.action.RestActions;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.SDKClient.SDKRestClient;
import org.opensearch.sdk.rest.BaseExtensionRestHandler;
import org.opensearch.sdk.rest.ReplacedRouteHandler;

import com.google.common.collect.ImmutableList;

/**
 * This class consists of the REST handler to retrieve an anomaly detector.
 */
public class RestGetAnomalyDetectorAction extends BaseExtensionRestHandler {

    private static final String GET_ANOMALY_DETECTOR_ACTION = "get_anomaly_detector";
    private static final Logger logger = LogManager.getLogger(RestGetAnomalyDetectorAction.class);
    private Settings settings;
    private SDKRestClient client;

    public RestGetAnomalyDetectorAction(ExtensionsRunner extensionsRunner, SDKRestClient client) {
        this.settings = extensionsRunner.getEnvironmentSettings();
        this.client = client;
    }

    // @Override
    public String getName() {
        return GET_ANOMALY_DETECTOR_ACTION;
    }

    protected ExtensionRestResponse prepareRequest(RestRequest request) throws IOException {
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new IllegalStateException(CommonErrorMessages.DISABLED_ERR_MSG);
        }
        String detectorId = request.param(DETECTOR_ID);
        String typesStr = request.param(TYPE);

        String rawPath = request.rawPath();
        boolean returnJob = request.paramAsBoolean("job", false);
        boolean returnTask = request.paramAsBoolean("task", false);
        boolean all = request.paramAsBoolean("_all", false);
        GetAnomalyDetectorRequest getAnomalyDetectorRequest = new GetAnomalyDetectorRequest(
            detectorId,
            RestActions.parseVersion(request),
            returnJob,
            returnTask,
            typesStr,
            rawPath,
            all,
            buildEntity(request, detectorId)
        );

        CompletableFuture<GetAnomalyDetectorResponse> futureResponse = new CompletableFuture<>();

        client
            .execute(
                GetAnomalyDetectorAction.INSTANCE,
                getAnomalyDetectorRequest,
                ActionListener.wrap(r -> futureResponse.complete(r), e -> futureResponse.completeExceptionally(e))
            );

        GetAnomalyDetectorResponse response = futureResponse
            .orTimeout(AnomalyDetectorSettings.REQUEST_TIMEOUT.get(settings).getMillis(), TimeUnit.MILLISECONDS)
            .join();

        // TODO handle exceptional response
        return getAnomalyDetectorResponse(request, response);
    }

    private Function<RestRequest, ExtensionRestResponse> handleRequest = (request) -> {
        try {
            return prepareRequest(request);
        } catch (Exception e) {
            // TODO: handle the AD-specific exceptions separately
            return exceptionalRequest(request, e);
        }
    };

    @Override
    public List<RouteHandler> routeHandlers() {
        return ImmutableList
            .of(
                // Opensearch-only API. Considering users may provide entity in the search body, support POST as well.
                new RouteHandler(
                    RestRequest.Method.POST,
                    String.format(Locale.ROOT, "%s/{%s}/%s", AnomalyDetectorExtension.AD_BASE_DETECTORS_URI, DETECTOR_ID, PROFILE),
                    handleRequest
                ),
                new RouteHandler(
                    RestRequest.Method.POST,
                    String
                        .format(Locale.ROOT, "%s/{%s}/%s/{%s}", AnomalyDetectorExtension.AD_BASE_DETECTORS_URI, DETECTOR_ID, PROFILE, TYPE),
                    handleRequest
                )
            );
    }

    @Override
    public List<ReplacedRouteHandler> replacedRouteHandlers() {
        String path = String.format(Locale.ROOT, "%s/{%s}", AnomalyDetectorExtension.LEGACY_OPENDISTRO_AD_BASE_URI, DETECTOR_ID);
        String newPath = String.format(Locale.ROOT, "%s/{%s}", AnomalyDetectorExtension.AD_BASE_DETECTORS_URI, DETECTOR_ID);
        return ImmutableList
            .of(
                new ReplacedRouteHandler(RestRequest.Method.GET, newPath, RestRequest.Method.GET, path, handleRequest),
                new ReplacedRouteHandler(RestRequest.Method.HEAD, newPath, RestRequest.Method.HEAD, path, handleRequest),
                new ReplacedRouteHandler(
                    RestRequest.Method.GET,
                    String.format(Locale.ROOT, "%s/{%s}/%s", AnomalyDetectorExtension.AD_BASE_DETECTORS_URI, DETECTOR_ID, PROFILE),
                    RestRequest.Method.GET,
                    String.format(Locale.ROOT, "%s/{%s}/%s", AnomalyDetectorExtension.LEGACY_OPENDISTRO_AD_BASE_URI, DETECTOR_ID, PROFILE),
                    handleRequest
                ),
                new ReplacedRouteHandler(
                    RestRequest.Method.GET,
                    String
                        .format(Locale.ROOT, "%s/{%s}/%s/{%s}", AnomalyDetectorExtension.AD_BASE_DETECTORS_URI, DETECTOR_ID, PROFILE, TYPE),
                    RestRequest.Method.GET,
                    String
                        .format(
                            Locale.ROOT,
                            "%s/{%s}/%s/{%s}",
                            AnomalyDetectorExtension.LEGACY_OPENDISTRO_AD_BASE_URI,
                            DETECTOR_ID,
                            PROFILE,
                            TYPE
                        ),
                    handleRequest
                )
            );
    }

    private Entity buildEntity(RestRequest request, String detectorId) throws IOException {
        if (Strings.isEmpty(detectorId)) {
            throw new IllegalStateException(CommonErrorMessages.AD_ID_MISSING_MSG);
        }

        String entityName = request.param(CommonName.CATEGORICAL_FIELD);
        String entityValue = request.param(CommonName.ENTITY_KEY);

        if (entityName != null && entityValue != null) {
            // single-stream profile request:
            // GET _plugins/_anomaly_detection/detectors/<detectorId>/_profile/init_progress?category_field=<field-name>&entity=<value>
            return Entity.createSingleAttributeEntity(entityName, entityValue);
        } else if (request.hasContent()) {
            /* HCAD profile request:
             * GET _plugins/_anomaly_detection/detectors/<detectorId>/_profile/init_progress
             * {
             *     "entity": [{
             *         "name": "clientip",
             *         "value": "13.24.0.0"
             *      }]
             * }
             */
            Optional<Entity> entity = Entity.fromJsonObject(request.contentParser());
            if (entity.isPresent()) {
                return entity.get();
            }
        }
        // not a valid profile request with correct entity information
        return null;
    }

    private ExtensionRestResponse getAnomalyDetectorResponse(RestRequest request, GetAnomalyDetectorResponse response) throws IOException {
        RestStatus restStatus = RestStatus.OK;
        ExtensionRestResponse extensionRestResponse = new ExtensionRestResponse(
            request,
            restStatus,
            response.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS)
        );
        return extensionRestResponse;
    }
}
