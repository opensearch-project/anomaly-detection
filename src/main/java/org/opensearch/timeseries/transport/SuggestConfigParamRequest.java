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

package org.opensearch.timeseries.transport;

import java.io.IOException;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.model.Config;

public class SuggestConfigParamRequest extends ActionRequest {

    private final AnalysisType context;
    private final Config config;
    private final String param;
    private final TimeValue requestTimeout;

    public SuggestConfigParamRequest(StreamInput in) throws IOException {
        super(in);
        context = in.readEnum(AnalysisType.class);
        if (context.isAD()) {
            config = new AnomalyDetector(in);
        } else if (context.isForecast()) {
            config = new Forecaster(in);
        } else {
            throw new UnsupportedOperationException("This method is not supported");
        }

        param = in.readString();
        requestTimeout = in.readTimeValue();
    }

    public SuggestConfigParamRequest(AnalysisType context, Config config, String param, TimeValue requestTimeout) {
        this.context = context;
        this.config = config;
        this.param = param;
        this.requestTimeout = requestTimeout;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeEnum(context);
        config.writeTo(out);
        out.writeString(param);
        out.writeTimeValue(requestTimeout);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public Config getConfig() {
        return config;
    }

    public String getParam() {
        return param;
    }

    public TimeValue getRequestTimeout() {
        return requestTimeout;
    }
}
