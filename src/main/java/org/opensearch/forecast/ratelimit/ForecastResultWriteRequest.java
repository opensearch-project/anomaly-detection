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

package org.opensearch.forecast.ratelimit;

import java.io.IOException;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.timeseries.ratelimit.RequestPriority;
import org.opensearch.timeseries.ratelimit.ResultWriteRequest;

public class ForecastResultWriteRequest extends ResultWriteRequest<ForecastResult> {

    public ForecastResultWriteRequest(
        long expirationEpochMs,
        String forecasterId,
        RequestPriority priority,
        ForecastResult result,
        String resultIndex,
        String flattenResultIndex
    ) {
        super(expirationEpochMs, forecasterId, priority, result, resultIndex, flattenResultIndex);
    }

    public ForecastResultWriteRequest(StreamInput in) throws IOException {
        super(in, ForecastResult::new);
    }
}
