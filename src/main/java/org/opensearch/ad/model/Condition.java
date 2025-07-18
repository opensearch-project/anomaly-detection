/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.model;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.util.Locale;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import com.google.common.base.Objects;

public class Condition implements Writeable, ToXContentObject {
    private static final String FEATURE_NAME_FIELD = "feature_name";
    private static final String THRESHOLD_TYPE_FIELD = "threshold_type";
    private static final String OPERATOR_FIELD = "operator";
    private static final String VALUE_FIELD = "value";

    private String featureName;
    private ThresholdType thresholdType;
    private Operator operator;
    private Double value;

    public Condition(String featureName, ThresholdType thresholdType, Operator operator, Double value) {
        this.featureName = featureName;
        this.thresholdType = thresholdType;
        this.operator = operator;
        this.value = value;
    }

    public Condition(StreamInput input) throws IOException {
        this.featureName = input.readString();
        this.thresholdType = input.readEnum(ThresholdType.class);
        this.operator = input.readEnum(Operator.class);
        this.value = input.readBoolean() ? input.readDouble() : null;
    }

    /**
     * Parse raw json content into rule instance.
     *
     * @param parser json based content parser
     * @return rule instance
     * @throws IOException IOException if content can't be parsed correctly
     */
    public static Condition parse(XContentParser parser) throws IOException {
        String featureName = null;
        ThresholdType thresholdType = null;
        Operator operator = null;
        Double value = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();

            parser.nextToken();
            switch (fieldName) {
                case FEATURE_NAME_FIELD:
                    featureName = parser.text();
                    break;
                case THRESHOLD_TYPE_FIELD:
                    thresholdType = ThresholdType.valueOf(parser.text().toUpperCase(Locale.ROOT));
                    break;
                case OPERATOR_FIELD:
                    if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
                        operator = null; // Set operator to null if the field is missing
                    } else {
                        operator = Operator.valueOf(parser.text().toUpperCase(Locale.ROOT));
                    }
                    break;
                case VALUE_FIELD:
                    if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
                        value = null; // Set value to null if the field is missing
                    } else {
                        value = parser.doubleValue();
                    }
                    break;
                default:
                    break;
            }
        }
        return new Condition(featureName, thresholdType, operator, value);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder
            .startObject()
            .field(FEATURE_NAME_FIELD, featureName)
            .field(THRESHOLD_TYPE_FIELD, thresholdType)
            .field(OPERATOR_FIELD, operator);
        if (value != null) {
            builder.field("value", value);
        }
        return xContentBuilder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(featureName);
        out.writeEnum(thresholdType);
        out.writeEnum(operator);
        out.writeBoolean(value != null);
        if (value != null) {
            out.writeDouble(value);
        }
    }

    public String getFeatureName() {
        return featureName;
    }

    public ThresholdType getThresholdType() {
        return thresholdType;
    }

    public Operator getOperator() {
        return operator;
    }

    public Double getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (getClass() != o.getClass()) {
            return false;
        }
        Condition that = (Condition) o;
        return Objects.equal(featureName, that.featureName)
            && Objects.equal(thresholdType, that.thresholdType)
            && Objects.equal(operator, that.operator)
            && Objects.equal(value, that.value);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + Objects.hashCode(featureName, thresholdType, operator, value);
        return result;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
            .append("featureName", featureName)
            .append("thresholdType", thresholdType)
            .append("operator", operator)
            .append("value", value)
            .toString();
    }
}
