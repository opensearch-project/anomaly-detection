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

package org.opensearch.forecast.rest;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.time.Instant;

import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.timeseries.annotation.Generated;
import org.opensearch.timeseries.util.ParseUtils;

import com.google.common.base.Objects;

/**
 * Input data needed to trigger forecaster.
 */
public class ForecasterExecutionInput implements ToXContentObject {

    private static final String FORECASTER_ID_FIELD = "forecaster_id";
    private static final String PERIOD_START_FIELD = "period_start";
    private static final String PERIOD_END_FIELD = "period_end";
    private static final String FORECASTER_FIELD = "forecaster";
    private Instant periodStart;
    private Instant periodEnd;
    private String forecasterId;
    private Forecaster forecaster;

    public ForecasterExecutionInput(String forecasterId, Instant periodStart, Instant periodEnd, Forecaster forecaster) {
        this.periodStart = periodStart;
        this.periodEnd = periodEnd;
        this.forecasterId = forecasterId;
        this.forecaster = forecaster;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder
            .startObject()
            .field(FORECASTER_ID_FIELD, forecasterId)
            .field(PERIOD_START_FIELD, periodStart.toEpochMilli())
            .field(PERIOD_END_FIELD, periodEnd.toEpochMilli())
            .field(FORECASTER_FIELD, forecaster);
        return xContentBuilder.endObject();
    }

    public static ForecasterExecutionInput parse(XContentParser parser) throws IOException {
        return parse(parser, null);
    }

    public static ForecasterExecutionInput parse(XContentParser parser, String inputConfigId) throws IOException {
        Instant periodStart = null;
        Instant periodEnd = null;
        Forecaster forecaster = null;
        String forecasterId = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case FORECASTER_ID_FIELD:
                    forecasterId = parser.text();
                    break;
                case PERIOD_START_FIELD:
                    periodStart = ParseUtils.toInstant(parser);
                    break;
                case PERIOD_END_FIELD:
                    periodEnd = ParseUtils.toInstant(parser);
                    break;
                case FORECASTER_FIELD:
                    XContentParser.Token token = parser.currentToken();
                    if (parser.currentToken().equals(XContentParser.Token.START_OBJECT)) {
                        forecaster = Forecaster.parse(parser, forecasterId);
                    }
                    break;
                default:
                    break;
            }
        }
        if (!Strings.isNullOrEmpty(inputConfigId)) {
            forecasterId = inputConfigId;
        }
        return new ForecasterExecutionInput(forecasterId, periodStart, periodEnd, forecaster);
    }

    @Generated
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ForecasterExecutionInput that = (ForecasterExecutionInput) o;
        return Objects.equal(periodStart, that.periodStart)
            && Objects.equal(periodEnd, that.periodEnd)
            && Objects.equal(forecasterId, that.forecasterId)
            && Objects.equal(forecaster, that.forecaster);
    }

    @Generated
    @Override
    public int hashCode() {
        return Objects.hashCode(periodStart, periodEnd, forecasterId);
    }

    public Instant getPeriodStart() {
        return periodStart;
    }

    public Instant getPeriodEnd() {
        return periodEnd;
    }

    public String getForecasterId() {
        return forecasterId;
    }

    public void setForecasterId(String forecasterId) {
        this.forecasterId = forecasterId;
    }

    public Forecaster getForecaster() {
        return forecaster;
    }
}
