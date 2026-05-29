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
import java.time.Instant;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.DocRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.model.Forecaster;

public class PreviewForecasterRequest extends ActionRequest implements DocRequest {
    private Forecaster forecaster;
    private String forecasterId;
    private Instant periodStart;
    private Instant periodEnd;

    public PreviewForecasterRequest(StreamInput in) throws IOException {
        super(in);
        forecaster = in.readOptionalWriteable(Forecaster::new);
        forecasterId = in.readOptionalString();
        periodStart = in.readInstant();
        periodEnd = in.readInstant();
    }

    public PreviewForecasterRequest(Forecaster forecaster, String forecasterId, Instant periodStart, Instant periodEnd) {
        super();
        this.forecaster = forecaster;
        this.forecasterId = forecasterId;
        this.periodStart = periodStart;
        this.periodEnd = periodEnd;
    }

    public Forecaster getForecaster() {
        return forecaster;
    }

    public String getForecasterId() {
        return forecasterId;
    }

    public Instant getPeriodStart() {
        return periodStart;
    }

    public Instant getPeriodEnd() {
        return periodEnd;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(forecaster);
        out.writeOptionalString(forecasterId);
        out.writeInstant(periodStart);
        out.writeInstant(periodEnd);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public String type() {
        return ForecastCommonName.FORECAST_RESOURCE_TYPE;
    }

    @Override
    public String index() {
        return ForecastIndex.CONFIG.getIndexName();
    }

    @Override
    public String id() {
        return forecasterId;
    }
}
