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
import org.opensearch.forecast.model.ForecastResultBucket;

/**
 * Response for getting the top anomaly results for HC detectors
 */
public class SearchTopForecastResultResponse extends ActionResponse implements ToXContentObject {
    public static final String BUCKETS_FIELD = "buckets";

    private List<ForecastResultBucket> forecastResultBuckets;

    public SearchTopForecastResultResponse(StreamInput in) throws IOException {
        super(in);
        forecastResultBuckets = in.readList(ForecastResultBucket::new);
    }

    public SearchTopForecastResultResponse(List<ForecastResultBucket> forecastResultBuckets) {
        this.forecastResultBuckets = forecastResultBuckets;
    }

    public List<ForecastResultBucket> getForecastResultBuckets() {
        return forecastResultBuckets;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(forecastResultBuckets);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // no need to show bucket with empty value
        return builder.startObject().field(BUCKETS_FIELD, forecastResultBuckets).endObject();
    }
}
