/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.model;

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
 * Feature imputed and its Id
 *
 */
public class FeatureImputed implements ToXContentObject, Writeable {

    public static final String FEATURE_ID_FIELD = "feature_id";
    public static final String IMPUTED_FIELD = "imputed";

    protected String featureId;
    protected Boolean imputed;

    public FeatureImputed(String featureId, Boolean imputed) {
        this.featureId = featureId;
        this.imputed = imputed;
    }

    public FeatureImputed(StreamInput input) throws IOException {
        this.featureId = input.readString();
        this.imputed = input.readBoolean();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject().field(FEATURE_ID_FIELD, featureId).field(IMPUTED_FIELD, imputed);
        return xContentBuilder.endObject();
    }

    public static FeatureImputed parse(XContentParser parser) throws IOException {
        String featureId = null;
        Boolean imputed = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case FEATURE_ID_FIELD:
                    featureId = parser.text();
                    break;
                case IMPUTED_FIELD:
                    imputed = parser.booleanValue();
                    break;
                default:
                    // the unknown field and it's children should be ignored
                    parser.skipChildren();
                    break;
            }
        }
        return new FeatureImputed(featureId, imputed);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FeatureImputed that = (FeatureImputed) o;
        return Objects.equal(getFeatureId(), that.getFeatureId()) && Objects.equal(isImputed(), that.isImputed());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getFeatureId(), isImputed());
    }

    public String getFeatureId() {
        return featureId;
    }

    public Boolean isImputed() {
        return imputed;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(featureId);
        out.writeBoolean(imputed);
    }

}
