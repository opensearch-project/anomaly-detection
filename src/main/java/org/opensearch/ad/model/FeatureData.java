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

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;

import org.opensearch.ad.annotation.Generated;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import com.google.common.base.Objects;

/**
 * Feature data used by RCF model.
 */
public class FeatureData extends DataByFeatureId {

    public static final String FEATURE_NAME_FIELD = "feature_name";

    private final String featureName;

    public FeatureData(String featureId, String featureName, Double data) {
        super(featureId, data);
        this.featureName = featureName;
    }

    public FeatureData(StreamInput input) throws IOException {
        this.featureId = input.readString();
        this.featureName = input.readString();
        this.data = input.readDouble();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder
            .startObject()
            .field(FEATURE_ID_FIELD, featureId)
            .field(FEATURE_NAME_FIELD, featureName)
            .field(DATA_FIELD, data);
        return xContentBuilder.endObject();
    }

    public static FeatureData parse(XContentParser parser) throws IOException {
        String featureId = null;
        Double data = null;
        String parsedFeatureName = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case FEATURE_ID_FIELD:
                    featureId = parser.text();
                    break;
                case FEATURE_NAME_FIELD:
                    parsedFeatureName = parser.text();
                    break;
                case DATA_FIELD:
                    data = parser.doubleValue();
                    break;
                default:
                    break;
            }
        }
        return new FeatureData(featureId, parsedFeatureName, data);
    }

    @Generated
    @Override
    public boolean equals(Object o) {
        if (super.equals(o)) {
            FeatureData that = (FeatureData) o;
            return Objects.equal(featureName, that.featureName);
        }
        return false;
    }

    @Generated
    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), featureName);
    }

    @Generated
    public String getFeatureName() {
        return featureName;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(featureId);
        out.writeString(featureName);
        out.writeDouble(data);
    }
}
