/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.dataprocessor;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

public class ImputationOption implements Writeable, ToXContent {
    // field name in toXContent
    public static final String METHOD_FIELD = "method";
    public static final String DEFAULT_FILL_FIELD = "defaultFill";
    public static final String INTEGER_SENSITIVE_FIELD = "integerSensitive";

    private final ImputationMethod method;
    private final Optional<double[]> defaultFill;
    private final boolean integerSentive;

    public ImputationOption(ImputationMethod method, Optional<double[]> defaultFill, boolean integerSentive) {
        this.method = method;
        this.defaultFill = defaultFill;
        this.integerSentive = integerSentive;
    }

    public ImputationOption(ImputationMethod method) {
        this(method, Optional.empty(), false);
    }

    public ImputationOption(StreamInput in) throws IOException {
        this.method = in.readEnum(ImputationMethod.class);
        if (in.readBoolean()) {
            this.defaultFill = Optional.of(in.readDoubleArray());
        } else {
            this.defaultFill = Optional.empty();
        }
        this.integerSentive = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(method);
        if (defaultFill.isEmpty()) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeDoubleArray(defaultFill.get());
        }
        out.writeBoolean(integerSentive);
    }

    public static ImputationOption parse(XContentParser parser) throws IOException {
        ImputationMethod method = ImputationMethod.ZERO;
        List<Double> defaultFill = null;
        Boolean integerSensitive = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();
            switch (fieldName) {
                case METHOD_FIELD:
                    method = ImputationMethod.valueOf(parser.text().toUpperCase(Locale.ROOT));
                    break;
                case DEFAULT_FILL_FIELD:
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
                    defaultFill = new ArrayList<>();
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        defaultFill.add(parser.doubleValue());
                    }
                    break;
                case INTEGER_SENSITIVE_FIELD:
                    integerSensitive = parser.booleanValue();
                    break;
                default:
                    break;
            }
        }
        return new ImputationOption(
            method,
            Optional.ofNullable(defaultFill).map(list -> list.stream().mapToDouble(Double::doubleValue).toArray()),
            integerSensitive
        );
    }

    public XContentBuilder toXContent(XContentBuilder builder) throws IOException {
        return toXContent(builder, ToXContent.EMPTY_PARAMS);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject();

        builder.field(METHOD_FIELD, method);

        if (!defaultFill.isEmpty()) {
            builder.array(DEFAULT_FILL_FIELD, defaultFill.get());
        }
        builder.field(INTEGER_SENSITIVE_FIELD, integerSentive);
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
        return method == other.method
            && (defaultFill.isEmpty() ? other.defaultFill.isEmpty() : Arrays.equals(defaultFill.get(), other.defaultFill.get()))
            && integerSentive == other.integerSentive;
    }

    @Override
    public int hashCode() {
        return Objects.hash(method, (defaultFill.isEmpty() ? 0 : Arrays.hashCode(defaultFill.get())), integerSentive);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
            .append("method", method)
            .append("defaultFill", (defaultFill.isEmpty() ? null : Arrays.toString(defaultFill.get())))
            .append("integerSentive", integerSentive)
            .toString();
    }

    public ImputationMethod getMethod() {
        return method;
    }

    public Optional<double[]> getDefaultFill() {
        return defaultFill;
    }

    public boolean isIntegerSentive() {
        return integerSentive;
    }
}
