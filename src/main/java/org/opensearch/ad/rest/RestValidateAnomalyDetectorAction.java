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
import static org.opensearch.rest.RestRequest.Method.POST;

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
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.DetectorValidationIssue;
import org.opensearch.ad.model.ValidationAspect;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.settings.EnabledSetting;
import org.opensearch.ad.transport.ValidateAnomalyDetectorAction;
import org.opensearch.ad.transport.ValidateAnomalyDetectorRequest;
import org.opensearch.ad.transport.ValidateAnomalyDetectorResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.extensions.rest.ExtensionRestResponse;
import org.opensearch.rest.NamedRoute;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.RestStatus;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.SDKClient.SDKRestClient;

import com.google.common.collect.ImmutableList;

/**
 * This class consists of the REST handler to validate anomaly detector configurations.
 */
public class RestValidateAnomalyDetectorAction extends AbstractAnomalyDetectorAction {
    private static final String VALIDATE_ANOMALY_DETECTOR_ACTION = "validate_anomaly_detector_action";
    private Settings settings;
    private SDKRestClient sdkRestClient;

    public static final Set<String> ALL_VALIDATION_ASPECTS_STRS = Arrays
        .asList(ValidationAspect.values())
        .stream()
        .map(aspect -> aspect.getName())
        .collect(Collectors.toSet());

    public RestValidateAnomalyDetectorAction(ExtensionsRunner extensionsRunner, SDKRestClient sdkRestClient) {
        super(extensionsRunner);
        this.settings = extensionsRunner.getEnvironmentSettings();
        this.sdkRestClient = sdkRestClient;
    }

    // @Override
    public String getName() {
        return VALIDATE_ANOMALY_DETECTOR_ACTION;
    }

    @Override
    public List<NamedRoute> routes() {
        return ImmutableList
            .of(
                new NamedRoute.Builder()
                    .method(POST)
                    .path(String.format(Locale.ROOT, "%s/%s", AnomalyDetectorExtension.AD_BASE_DETECTORS_URI, VALIDATE))
                    .uniqueName(addRouteNamePrefix("detectors/validate"))
                    .handler(handleRequest)
                    .build(),
                new NamedRoute.Builder()
                    .method(POST)
                    .path(String.format(Locale.ROOT, "%s/%s/{%s}", AnomalyDetectorExtension.AD_BASE_DETECTORS_URI, VALIDATE, TYPE))
                    .uniqueName(addRouteNamePrefix("detectors/validate/type"))
                    .handler(handleRequest)
                    .build()
            );
    }

    private Function<RestRequest, RestResponse> handleRequest = (request) -> {
        try {
            return prepareRequest(request);
        } catch (Exception e) {
            // TODO: handle the AD-specific exceptions separately
            return exceptionalRequest(request, e);
        }
    };

    protected ExtensionRestResponse sendAnomalyDetectorValidationParseResponse(RestRequest request, DetectorValidationIssue issue)
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

    protected ExtensionRestResponse prepareRequest(RestRequest request) throws IOException {
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new IllegalStateException(CommonErrorMessages.DISABLED_ERR_MSG);
        }
        XContentParser parser = request.contentParser();
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

        CompletableFuture<ValidateAnomalyDetectorResponse> futureResponse = new CompletableFuture<>();
        sdkRestClient
            .execute(
                ValidateAnomalyDetectorAction.INSTANCE,
                validateAnomalyDetectorRequest,
                ActionListener.wrap(r -> futureResponse.complete(r), e -> futureResponse.completeExceptionally(e))
            );

        ValidateAnomalyDetectorResponse response = futureResponse
            .orTimeout(AnomalyDetectorSettings.REQUEST_TIMEOUT.get(settings).getMillis(), TimeUnit.MILLISECONDS)
            .join();
        // TODO handle exceptional response
        return validateAnomalyDetectorResponse(request, response);
    }

    private ExtensionRestResponse validateAnomalyDetectorResponse(RestRequest request, ValidateAnomalyDetectorResponse response)
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
