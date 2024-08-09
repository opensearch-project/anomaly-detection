/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.dataprocessor;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.timeseries.model.DataByFeatureId;
import org.opensearch.timeseries.model.Feature;

public class ImputationOption implements Writeable, ToXContent {
    // field name in toXContent
    public static final String METHOD_FIELD = "method";
    public static final String DEFAULT_FILL_FIELD = "defaultFill";

    private final ImputationMethod method;
    private final Map<String, Double> defaultFill;

    public ImputationOption(ImputationMethod method, Map<String, Double> defaultFill) {
        this.method = method;
        this.defaultFill = defaultFill;
    }

    public ImputationOption(ImputationMethod method) {
        this(method, null);
    }

    public ImputationOption(StreamInput in) throws IOException {
        this.method = in.readEnum(ImputationMethod.class);
        if (in.readBoolean()) {
            this.defaultFill = in.readMap(StreamInput::readString, StreamInput::readDouble);
        } else {
            this.defaultFill = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(method);
        if (defaultFill == null || defaultFill.isEmpty()) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeMap(defaultFill, StreamOutput::writeString, StreamOutput::writeDouble);
        }
    }

    public static ImputationOption parse(XContentParser parser) throws IOException {
        ImputationMethod method = ImputationMethod.ZERO;
        Map<String, Double> defaultFill = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();
            switch (fieldName) {
                case METHOD_FIELD:
                    method = ImputationMethod.valueOf(parser.text().toUpperCase(Locale.ROOT));
                    break;
                case DEFAULT_FILL_FIELD:
                    defaultFill = new HashMap<>();
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
                    while ((parser.nextToken()) != XContentParser.Token.END_ARRAY) {

                        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);

                        String featureName = null;
                        Double fillValue = null;
                        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                            String fillFieldName = parser.currentName();
                            parser.nextToken();

                            switch (fillFieldName) {
                                case Feature.FEATURE_NAME_FIELD:
                                    featureName = parser.text();
                                    break;
                                case DataByFeatureId.DATA_FIELD:
                                    fillValue = parser.doubleValue();
                                    break;
                                default:
                                    // the unknown field and it's children should be ignored
                                    parser.skipChildren();
                                    break;
                            }
                        }

                        defaultFill.put(featureName, fillValue);
                    }
                    break;
                default:
                    break;
            }
        }
        return new ImputationOption(method, defaultFill);
    }

    public XContentBuilder toXContent(XContentBuilder builder) throws IOException {
        return toXContent(builder, ToXContent.EMPTY_PARAMS);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject();

        builder.field(METHOD_FIELD, method);

        if (defaultFill != null && !defaultFill.isEmpty()) {
            builder.startArray(DEFAULT_FILL_FIELD);
            for (Map.Entry<String, Double> fill : defaultFill.entrySet()) {
                builder.startObject();
                builder.field(Feature.FEATURE_NAME_FIELD, fill.getKey());
                builder.field(DataByFeatureId.DATA_FIELD, fill.getValue());
                builder.endObject();
            }
            builder.endArray();
        }
        return xContentBuilder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ImputationOption other = (ImputationOption) o;
        return method == other.method && Objects.equals(defaultFill, other.defaultFill);
    }

    @Override
    public int hashCode() {
        return Objects.hash(method, defaultFill);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("method", method).append("defaultFill", defaultFill).toString();
    }

    public ImputationMethod getMethod() {
        return method;
    }

    public Map<String, Double> getDefaultFill() {
        return defaultFill;
    }
}
