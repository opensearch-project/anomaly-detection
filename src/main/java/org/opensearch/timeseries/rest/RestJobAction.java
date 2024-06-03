/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.rest;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.util.List;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.timeseries.model.DateRange;

import com.google.common.collect.ImmutableList;

public abstract class RestJobAction extends BaseRestHandler {
    protected DateRange parseInputDateRange(RestRequest request) throws IOException {
        if (!request.hasContent()) {
            return null;
        }
        XContentParser parser = request.contentParser();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        DateRange dateRange = DateRange.parse(parser);
        return dateRange;
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of();
    }
}
