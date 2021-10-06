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

package org.opensearch.ad.model;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregation.Bucket;
import org.opensearch.search.aggregations.metrics.InternalMax;

import java.io.IOException;
import java.util.Map;

/**
 * Represents a single bucket when retrieving top anomaly results for HC detectors
 */
public class AnomalyResultBucket implements ToXContentObject, Writeable {
    public static final String BUCKETS_FIELD = "buckets";
    public static final String KEY_FIELD = "key";
    public static final String DOC_COUNT_FIELD = "doc_count";
    public static final String MAX_ANOMALY_GRADE_FIELD = "max_anomaly_grade";

    private final Map<String, Object> key;
    private final int docCount;
    private final double maxAnomalyGrade;

    public AnomalyResultBucket(
            Map<String, Object> key,
            int docCount,
            double maxAnomalyGrade
    ) {
        this.key = key;
        this.docCount = docCount;
        this.maxAnomalyGrade = maxAnomalyGrade;
    }


    public AnomalyResultBucket(StreamInput input) throws IOException {
        this.key = input.readMap();
        this.docCount = input.readInt();
        this.maxAnomalyGrade = input.readDouble();
    }

    public static AnomalyResultBucket createAnomalyResultBucket(Bucket bucket) {
        return new AnomalyResultBucket(
                bucket.getKey(),
                (int) bucket.getDocCount(),
                ((InternalMax) bucket.getAggregations().get(MAX_ANOMALY_GRADE_FIELD)).getValue()
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder
                .startObject()
                .field(KEY_FIELD, key)
                .field(DOC_COUNT_FIELD, docCount)
                .field(MAX_ANOMALY_GRADE_FIELD, maxAnomalyGrade);
        return xContentBuilder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(key);
        out.writeInt(docCount);
        out.writeDouble(maxAnomalyGrade);
    }

    public Map<String, Object> getKey() {
        return key;
    }

    public int getDocCount() {
        return docCount;
    }

    public double getMaxAnomalyGrade() {
        return maxAnomalyGrade;
    }
}
