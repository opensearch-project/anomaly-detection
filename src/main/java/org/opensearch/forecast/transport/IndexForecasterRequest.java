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

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.DocRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.rest.RestRequest;

public class IndexForecasterRequest extends ActionRequest implements DocRequest {
    private String forecastID;
    private long seqNo;
    private long primaryTerm;
    private WriteRequest.RefreshPolicy refreshPolicy;
    private Forecaster forecaster;
    private RestRequest.Method method;
    private TimeValue requestTimeout;
    private Integer maxSingleStreamForecasters;
    private Integer maxHCForecasters;
    private Integer maxForecastFeatures;
    private Integer maxCategoricalFields;

    public IndexForecasterRequest(StreamInput in) throws IOException {
        super(in);
        forecastID = in.readString();
        seqNo = in.readLong();
        primaryTerm = in.readLong();
        refreshPolicy = in.readEnum(WriteRequest.RefreshPolicy.class);
        forecaster = new Forecaster(in);
        method = in.readEnum(RestRequest.Method.class);
        requestTimeout = in.readTimeValue();
        maxSingleStreamForecasters = in.readInt();
        maxHCForecasters = in.readInt();
        maxForecastFeatures = in.readInt();
        maxCategoricalFields = in.readInt();
    }

    public IndexForecasterRequest(
        String forecasterID,
        long seqNo,
        long primaryTerm,
        WriteRequest.RefreshPolicy refreshPolicy,
        Forecaster forecaster,
        RestRequest.Method method,
        TimeValue requestTimeout,
        Integer maxSingleEntityAnomalyDetectors,
        Integer maxMultiEntityAnomalyDetectors,
        Integer maxAnomalyFeatures,
        Integer maxCategoricalFields
    ) {
        super();
        this.forecastID = forecasterID;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        this.refreshPolicy = refreshPolicy;
        this.forecaster = forecaster;
        this.method = method;
        this.requestTimeout = requestTimeout;
        this.maxSingleStreamForecasters = maxSingleEntityAnomalyDetectors;
        this.maxHCForecasters = maxMultiEntityAnomalyDetectors;
        this.maxForecastFeatures = maxAnomalyFeatures;
        this.maxCategoricalFields = maxCategoricalFields;
    }

    public String getForecasterID() {
        return forecastID;
    }

    public long getSeqNo() {
        return seqNo;
    }

    public long getPrimaryTerm() {
        return primaryTerm;
    }

    public WriteRequest.RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    public Forecaster getForecaster() {
        return forecaster;
    }

    public RestRequest.Method getMethod() {
        return method;
    }

    public TimeValue getRequestTimeout() {
        return requestTimeout;
    }

    public Integer getMaxSingleStreamForecasters() {
        return maxSingleStreamForecasters;
    }

    public Integer getMaxHCForecasters() {
        return maxHCForecasters;
    }

    public Integer getMaxForecastFeatures() {
        return maxForecastFeatures;
    }

    public Integer getMaxCategoricalFields() {
        return maxCategoricalFields;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(forecastID);
        out.writeLong(seqNo);
        out.writeLong(primaryTerm);
        out.writeEnum(refreshPolicy);
        forecaster.writeTo(out);
        out.writeEnum(method);
        out.writeTimeValue(requestTimeout);
        out.writeInt(maxSingleStreamForecasters);
        out.writeInt(maxHCForecasters);
        out.writeInt(maxForecastFeatures);
        out.writeInt(maxCategoricalFields);
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
        return forecastID;
    }
}
