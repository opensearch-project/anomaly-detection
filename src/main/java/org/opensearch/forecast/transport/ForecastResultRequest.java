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

import static org.opensearch.action.ValidateActions.addValidationError;

import java.io.IOException;
import java.util.Locale;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.forecast.constant.ForecastCommonMessages;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.transport.ResultRequest;

public class ForecastResultRequest extends ResultRequest {

    public ForecastResultRequest(StreamInput in) throws IOException {
        super(in);
        in.readEnum(AnalysisType.class);
    }

    public ForecastResultRequest(String forecastID, long start, long end) {
        super(forecastID, start, end);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (Strings.isEmpty(configId)) {
            validationException = addValidationError(ForecastCommonMessages.FORECASTER_ID_MISSING_MSG, validationException);
        }
        // at least end time should be set
        if (end <= 0) {
            validationException = addValidationError(
                String.format(Locale.ROOT, "%s: start %d, end %d", ForecastCommonMessages.INVALID_TIMESTAMP_ERR_MSG, start, end),
                validationException
            );
        }
        return validationException;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ForecastCommonName.ID_JSON_KEY, configId);
        builder.field(CommonName.START_JSON_KEY, start);
        builder.field(CommonName.END_JSON_KEY, end);
        builder.endObject();
        return builder;
    }
}
