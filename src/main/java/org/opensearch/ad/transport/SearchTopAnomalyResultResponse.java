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

package org.opensearch.ad.transport;

import java.io.IOException;
import java.util.List;

import org.opensearch.ad.model.AnomalyResultBucket;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

/**
 * Response for getting the top anomaly results for HC detectors
 */
public class SearchTopAnomalyResultResponse extends ActionResponse implements ToXContentObject {
    private List<AnomalyResultBucket> anomalyResultBuckets;

    public SearchTopAnomalyResultResponse(StreamInput in) throws IOException {
        super(in);
        anomalyResultBuckets = in.readList(AnomalyResultBucket::new);
    }

    public SearchTopAnomalyResultResponse(List<AnomalyResultBucket> anomalyResultBuckets) {
        this.anomalyResultBuckets = anomalyResultBuckets;
    }

    public List<AnomalyResultBucket> getAnomalyResultBuckets() {
        return anomalyResultBuckets;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(anomalyResultBuckets);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field(AnomalyResultBucket.BUCKETS_FIELD, anomalyResultBuckets).endObject();
    }
}
