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

package org.opensearch.forecast.transport;

import java.io.IOException;
import java.util.List;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.forecast.model.Forecaster;

public class PreviewForecasterResponse extends ActionResponse implements ToXContentObject {
    public static final String FORECAST_RESULT_FIELD = "forecast_result";
    public static final String FORECASTER_FIELD = "forecaster";

    private List<ForecastResult> forecastResult;
    private Forecaster forecaster;

    public PreviewForecasterResponse(StreamInput in) throws IOException {
        super(in);
        forecastResult = in.readList(ForecastResult::new);
        forecaster = new Forecaster(in);
    }

    public PreviewForecasterResponse(List<ForecastResult> forecastResult, Forecaster forecaster) {
        this.forecastResult = forecastResult;
        this.forecaster = forecaster;
    }

    public List<ForecastResult> getForecastResult() {
        return forecastResult;
    }

    public Forecaster getForecaster() {
        return forecaster;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(forecastResult);
        forecaster.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field(FORECAST_RESULT_FIELD, forecastResult).field(FORECASTER_FIELD, forecaster).endObject();
    }
}
