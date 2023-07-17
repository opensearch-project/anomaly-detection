/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.model;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import com.google.common.base.Objects;

/**
 * Data and its Id
 *
 */
public class DataByFeatureId implements ToXContentObject, Writeable {

    public static final String FEATURE_ID_FIELD = "feature_id";
    public static final String DATA_FIELD = "data";

    protected String featureId;
    protected Double data;

    public DataByFeatureId(String featureId, Double data) {
        this.featureId = featureId;
        this.data = data;
    }

    /*
     * Used by the subclass that has its own way of initializing data like
     * reading from StreamInput
     */
    protected DataByFeatureId() {}

    public DataByFeatureId(StreamInput input) throws IOException {
        this.featureId = input.readString();
        this.data = input.readDouble();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject().field(FEATURE_ID_FIELD, featureId).field(DATA_FIELD, data);
        return xContentBuilder.endObject();
    }

    public static DataByFeatureId parse(XContentParser parser) throws IOException {
        String featureId = null;
        Double data = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case FEATURE_ID_FIELD:
                    featureId = parser.text();
                    break;
                case DATA_FIELD:
                    data = parser.doubleValue();
                    break;
                default:
                    // the unknown field and it's children should be ignored
                    parser.skipChildren();
                    break;
            }
        }
        return new DataByFeatureId(featureId, data);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        DataByFeatureId that = (DataByFeatureId) o;
        return Objects.equal(getFeatureId(), that.getFeatureId()) && Objects.equal(getData(), that.getData());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getFeatureId(), getData());
    }

    public String getFeatureId() {
        return featureId;
    }

    public Double getData() {
        return data;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(featureId);
        out.writeDouble(data);
    }

}
