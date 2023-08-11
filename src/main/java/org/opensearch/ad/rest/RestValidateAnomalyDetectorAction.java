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
import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.opensearch.ad.AnomalyDetectorPlugin;
import org.opensearch.ad.common.exception.ADValidationException;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.DetectorValidationIssue;
import org.opensearch.ad.model.ValidationAspect;
import org.opensearch.ad.settings.EnabledSetting;
import org.opensearch.ad.transport.ValidateAnomalyDetectorAction;
import org.opensearch.ad.transport.ValidateAnomalyDetectorRequest;
import org.opensearch.ad.transport.ValidateAnomalyDetectorResponse;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import com.google.common.collect.ImmutableList;

/**
 * This class consists of the REST handler to validate anomaly detector configurations.
 */
public class RestValidateAnomalyDetectorAction extends AbstractAnomalyDetectorAction {
    private static final String VALIDATE_ANOMALY_DETECTOR_ACTION = "validate_anomaly_detector_action";

    public static final Set<String> ALL_VALIDATION_ASPECTS_STRS = Arrays
        .asList(ValidationAspect.values())
        .stream()
        .map(aspect -> aspect.getName())
        .collect(Collectors.toSet());

    public RestValidateAnomalyDetectorAction(Settings settings, ClusterService clusterService) {
        super(settings, clusterService);
    }

    @Override
    public String getName() {
        return VALIDATE_ANOMALY_DETECTOR_ACTION;
    }

    @Override
    public List<Route> routes() {
        return ImmutableList
            .of(
                new Route(
                    RestRequest.Method.POST,
                    String.format(Locale.ROOT, "%s/%s", AnomalyDetectorPlugin.AD_BASE_DETECTORS_URI, VALIDATE)
                ),
                new Route(
                    RestRequest.Method.POST,
                    String.format(Locale.ROOT, "%s/%s/{%s}", AnomalyDetectorPlugin.AD_BASE_DETECTORS_URI, VALIDATE, TYPE)
                )
            );
    }

    protected void sendAnomalyDetectorValidationParseResponse(DetectorValidationIssue issue, RestChannel channel) throws IOException {
        try {
            BytesRestResponse restResponse = new BytesRestResponse(
                RestStatus.OK,
                new ValidateAnomalyDetectorResponse(issue).toXContent(channel.newBuilder())
            );
            channel.sendResponse(restResponse);
        } catch (Exception e) {
            channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, e.getMessage()));
        }
    }

    private Boolean validationTypesAreAccepted(String validationType) {
        Set<String> typesInRequest = new HashSet<>(Arrays.asList(validationType.split(",")));
        return (!Collections.disjoint(typesInRequest, ALL_VALIDATION_ASPECTS_STRS));
    }

    @Override
    protected BaseRestHandler.RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
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

        return channel -> {
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
                    sendAnomalyDetectorValidationParseResponse(issue, channel);
                    return;
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
            client.execute(ValidateAnomalyDetectorAction.INSTANCE, validateAnomalyDetectorRequest, new RestToXContentListener<>(channel));
        };
    }
}
