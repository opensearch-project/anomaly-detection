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

package org.opensearch.timeseries.rest;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.ConfigValidationIssue;
import org.opensearch.timeseries.rest.handler.AbstractTimeSeriesActionHandler;
import org.opensearch.timeseries.transport.ValidateConfigRequest;
import org.opensearch.timeseries.transport.ValidateConfigResponse;

/**
 * This class consists of the REST handler to validate anomaly detector configurations.
 */
public class RestValidateAction {
    private AnalysisType context;
    private Integer maxSingleStreamConfigs;
    private Integer maxHCConfigs;
    private Integer maxFeatures;
    private Integer maxCategoricalFields;
    private TimeValue requestTimeout;

    public RestValidateAction(
        AnalysisType context,
        Integer maxSingleStreamConfigs,
        Integer maxHCConfigs,
        Integer maxFeatures,
        Integer maxCategoricalFields,
        TimeValue requestTimeout
    ) {
        this.context = context;
        this.maxSingleStreamConfigs = maxSingleStreamConfigs;
        this.maxHCConfigs = maxHCConfigs;
        this.maxFeatures = maxFeatures;
        this.maxCategoricalFields = maxCategoricalFields;
        this.requestTimeout = requestTimeout;
    }

    public void sendValidationParseResponse(ConfigValidationIssue issue, RestChannel channel) throws IOException {
        try {
            BytesRestResponse restResponse = new BytesRestResponse(
                RestStatus.OK,
                new ValidateConfigResponse(issue).toXContent(channel.newBuilder())
            );
            channel.sendResponse(restResponse);
        } catch (Exception e) {
            channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, e.getMessage()));
        }
    }

    private Boolean validationTypesAreAccepted(String validationType) {
        Set<String> typesInRequest = new HashSet<>(Arrays.asList(validationType.split(",")));
        return (!Collections.disjoint(typesInRequest, AbstractTimeSeriesActionHandler.ALL_VALIDATION_ASPECTS_STRS));
    }

    public ValidateConfigRequest prepareRequest(RestRequest request, NodeClient client, String typesStr) throws IOException {
        XContentParser parser = request.contentParser();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        // if type param isn't blank and isn't a part of possible validation types throws exception
        if (!StringUtils.isBlank(typesStr)) {
            if (!validationTypesAreAccepted(typesStr)) {
                throw new IllegalStateException(CommonMessages.NOT_EXISTENT_VALIDATION_TYPE);
            }
        }

        Config config = null;

        if (context.isAD()) {
            config = AnomalyDetector.parse(parser);
        } else if (context.isForecast()) {
            config = Forecaster.parse(parser);
        } else {
            throw new UnsupportedOperationException("This method is not supported");
        }

        ValidateConfigRequest validateAnomalyDetectorRequest = new ValidateConfigRequest(
            context,
            config,
            typesStr,
            maxSingleStreamConfigs,
            maxHCConfigs,
            maxFeatures,
            requestTimeout,
            maxCategoricalFields
        );
        return validateAnomalyDetectorRequest;
    }
}
