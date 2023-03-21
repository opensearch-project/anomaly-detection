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

import static org.opensearch.ad.util.RestHandlerUtils.TYPE;
import static org.opensearch.ad.util.RestHandlerUtils.VALIDATE;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.opensearch.action.ActionListener;
import org.opensearch.ad.AnomalyDetectorExtension;
import org.opensearch.ad.common.exception.ADValidationException;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.feature.SearchFeatureDao;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.DetectorValidationIssue;
import org.opensearch.ad.model.ValidationAspect;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.settings.EnabledSetting;
import org.opensearch.ad.transport.ValidateAnomalyDetectorRequest;
import org.opensearch.ad.transport.ValidateAnomalyDetectorResponse;
import org.opensearch.ad.transport.ValidateAnomalyDetectorTransportAction;
import org.opensearch.client.opensearch.OpenSearchAsyncClient;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.extensions.rest.ExtensionRestRequest;
import org.opensearch.extensions.rest.ExtensionRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.RouteHandler;
import org.opensearch.sdk.SDKClient.SDKRestClient;
import org.opensearch.sdk.SDKClusterService;
import org.opensearch.sdk.SDKNamedXContentRegistry;
import org.opensearch.transport.TransportService;

import com.google.common.collect.ImmutableList;

/**
 * This class consists of the REST handler to validate anomaly detector configurations.
 */
public class RestValidateAnomalyDetectorAction extends AbstractAnomalyDetectorAction {
    private static final String VALIDATE_ANOMALY_DETECTOR_ACTION = "validate_anomaly_detector_action";
    private SDKNamedXContentRegistry namedXContentRegistry;
    private Settings environmentSettings;
    private TransportService transportService;
    private SDKRestClient restClient;
    private OpenSearchAsyncClient sdkJavaAsyncClient;
    private SDKClusterService sdkClusterService;

    public static final Set<String> ALL_VALIDATION_ASPECTS_STRS = Arrays
        .asList(ValidationAspect.values())
        .stream()
        .map(aspect -> aspect.getName())
        .collect(Collectors.toSet());

    public RestValidateAnomalyDetectorAction(
        ExtensionsRunner extensionsRunner,
        SDKRestClient restClient,
        OpenSearchAsyncClient sdkJavaAsyncClient
    ) {
        super(extensionsRunner);
        this.namedXContentRegistry = extensionsRunner.getNamedXContentRegistry();
        this.environmentSettings = extensionsRunner.getEnvironmentSettings();
        this.transportService = extensionsRunner.getExtensionTransportService();
        this.restClient = restClient;
        this.sdkJavaAsyncClient = sdkJavaAsyncClient;
        this.sdkClusterService = new SDKClusterService(extensionsRunner);
    }

    // @Override
    public String getName() {
        return VALIDATE_ANOMALY_DETECTOR_ACTION;
    }

    @Override
    public List<RouteHandler> routeHandlers() {
        return ImmutableList
            .of(
                new RouteHandler(
                    RestRequest.Method.POST,
                    String.format(Locale.ROOT, "%s/%s", AnomalyDetectorExtension.AD_BASE_DETECTORS_URI, VALIDATE),
                    handleRequest
                ),
                new RouteHandler(
                    RestRequest.Method.POST,
                    String.format(Locale.ROOT, "%s/%s/{%s}", AnomalyDetectorExtension.AD_BASE_DETECTORS_URI, VALIDATE, TYPE),
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

    protected ExtensionRestResponse sendAnomalyDetectorValidationParseResponse(ExtensionRestRequest request, DetectorValidationIssue issue)
        throws IOException {
        return new ExtensionRestResponse(
            request,
            RestStatus.OK,
            new ValidateAnomalyDetectorResponse(issue).toXContent(JsonXContent.contentBuilder())
        );
    }

    private Boolean validationTypesAreAccepted(String validationType) {
        Set<String> typesInRequest = new HashSet<>(Arrays.asList(validationType.split(",")));
        return (!Collections.disjoint(typesInRequest, ALL_VALIDATION_ASPECTS_STRS));
    }

    protected ExtensionRestResponse prepareRequest(ExtensionRestRequest request) throws IOException {
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new IllegalStateException(CommonErrorMessages.DISABLED_ERR_MSG);
        }
        XContentParser parser = request.contentParser(this.namedXContentRegistry.getRegistry());
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        String typesStr = request.param(TYPE);

        // if type param isn't blank and isn't a part of possible validation types throws exception
        if (!StringUtils.isBlank(typesStr)) {
            if (!validationTypesAreAccepted(typesStr)) {
                throw new IllegalStateException(CommonErrorMessages.NOT_EXISTENT_VALIDATION_TYPE);
            }
        }

        AnomalyDetector detector;
        try {
            detector = AnomalyDetector.parse(parser);
        } catch (Exception ex) {
            if (ex instanceof ADValidationException) {
                ADValidationException ADException = (ADValidationException) ex;
                DetectorValidationIssue issue = new DetectorValidationIssue(
                    ADException.getAspect(),
                    ADException.getType(),
                    ADException.getMessage()
                );
                return sendAnomalyDetectorValidationParseResponse(request, issue);
            } else {
                throw ex;
            }
        }
        ValidateAnomalyDetectorRequest validateAnomalyDetectorRequest = new ValidateAnomalyDetectorRequest(
            detector,
            typesStr,
            maxSingleEntityDetectors,
            maxMultiEntityDetectors,
            maxAnomalyFeatures,
            requestTimeout
        );

        // Here we would call client.execute(action, request, responseListener)
        // This delegates to transportAction(action).execute(request, responseListener)
        // ValidateAnomalyDetectorAction is the key to the getActions map
        // ValidateAnomalyDetectorTransportAction is the value, execute() calls doExecute()

        ValidateAnomalyDetectorTransportAction validateAction = new ValidateAnomalyDetectorTransportAction(
            restClient, // Client client
            sdkClusterService, // ClusterService clusterService,
            this.namedXContentRegistry,
            this.environmentSettings, // Settings settings
            new AnomalyDetectionIndices(
                restClient, // client,
                sdkJavaAsyncClient,
                sdkClusterService, // clusterService,
                null, // threadPool,
                this.environmentSettings, // settings,
                null, // nodeFilter,
                AnomalyDetectorSettings.MAX_UPDATE_RETRY_TIMES
            ), // AnomalyDetectionIndices anomalyDetectionIndices
            null, // ActionFilters actionFilters
            transportService,
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

        CompletableFuture<ValidateAnomalyDetectorResponse> futureResponse = new CompletableFuture<>();
        validateAction
            .doExecute(
                null,
                validateAnomalyDetectorRequest,
                ActionListener.wrap(r -> futureResponse.complete(r), e -> futureResponse.completeExceptionally(e))
            );

        ValidateAnomalyDetectorResponse response = futureResponse
            .orTimeout(AnomalyDetectorSettings.REQUEST_TIMEOUT.get(environmentSettings).getMillis(), TimeUnit.MILLISECONDS)
            .join();
        // TODO handle exceptional response
        return validateAnomalyDetectorResponse(request, response);
    }

    private ExtensionRestResponse validateAnomalyDetectorResponse(ExtensionRestRequest request, ValidateAnomalyDetectorResponse response)
        throws IOException {
        RestStatus restStatus = RestStatus.OK;
        ExtensionRestResponse extensionRestResponse = new ExtensionRestResponse(
            request,
            restStatus,
            response.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS)
        );
        return extensionRestResponse;
    }
}
