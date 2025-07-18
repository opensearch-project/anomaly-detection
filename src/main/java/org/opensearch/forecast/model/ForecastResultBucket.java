/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.model;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.timeseries.annotation.Generated;

import com.google.common.base.Objects;

public class ForecastResultBucket implements ToXContentObject, Writeable {
    public static final String BUCKETS_FIELD = "buckets";
    public static final String KEY_FIELD = "key";
    public static final String DOC_COUNT_FIELD = "doc_count";
    public static final String BUCKET_INDEX_FIELD = "bucket_index";

    // e.g., "ip": "1.2.3.4"
    private final Map<String, Object> key;
    private final int docCount;
    private final Map<String, Double> aggregations;
    private final int bucketIndex;

    public ForecastResultBucket(Map<String, Object> key, int docCount, Map<String, Double> aggregations, int bucketIndex) {
        this.key = key;
        this.docCount = docCount;
        this.aggregations = aggregations;
        this.bucketIndex = bucketIndex;
    }

    public ForecastResultBucket(StreamInput input) throws IOException {
        this.key = input.readMap();
        this.docCount = input.readInt();
        this.aggregations = input.readMap(StreamInput::readString, StreamInput::readDouble);
        this.bucketIndex = input.readInt();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder
            .startObject()
            .field(KEY_FIELD, key)
            .field(DOC_COUNT_FIELD, docCount)
            .field(BUCKET_INDEX_FIELD, bucketIndex);

        for (Map.Entry<String, Double> entry : aggregations.entrySet()) {
            xContentBuilder.field(entry.getKey(), entry.getValue());
        }
        return xContentBuilder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(key);
        out.writeInt(docCount);
        out.writeMap(aggregations, StreamOutput::writeString, StreamOutput::writeDouble);
        out.writeInt(bucketIndex);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ForecastResultBucket that = (ForecastResultBucket) o;
        return Objects.equal(key, that.getKey())
            && Objects.equal(docCount, that.getDocCount())
            && Objects.equal(aggregations, that.getAggregations())
            && Objects.equal(bucketIndex, that.getBucketIndex());
    }

    @Generated
    @Override
    public int hashCode() {
        return Objects.hashCode(key, docCount, aggregations, bucketIndex);
    }

    @Generated
    @Override
    public String toString() {
        return new ToStringBuilder(this)
            .append("key", key)
            .append("docCount", docCount)
            .append("aggregations", aggregations)
            .append("bucketIndex", bucketIndex)
            .toString();
    }

    public Map<String, Object> getKey() {
        return key;
    }

    public int getDocCount() {
        return docCount;
    }

    public Map<String, Double> getAggregations() {
        return aggregations;
    }

    public int getBucketIndex() {
        return bucketIndex;
    }
}
