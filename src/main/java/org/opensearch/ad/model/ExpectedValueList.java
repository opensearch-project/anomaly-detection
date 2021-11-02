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

import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;

import com.google.common.base.Objects;

public class ExpectedValueList implements ToXContentObject, Writeable {
    public static final String LIKELIHOOD_FIELD = "likelihood";
    public static final String VALUE_LIST_FIELD = "value_list";

    private Double likelihood;
    private List<DataByFeatureId> valueList;

    public ExpectedValueList(Double likelihood, List<DataByFeatureId> valueList) {
        this.likelihood = likelihood;
        this.valueList = valueList;
    }

    public ExpectedValueList(StreamInput input) throws IOException {
        this.likelihood = input.readOptionalDouble();
        this.valueList = input.readList(DataByFeatureId::new);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject();
        if (likelihood != null) {
            xContentBuilder.field(LIKELIHOOD_FIELD, likelihood);
        }
        if (valueList != null) {
            xContentBuilder.field(VALUE_LIST_FIELD, valueList.toArray());
        }
        return xContentBuilder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalDouble(likelihood);
        out.writeList(valueList);
    }

    public static ExpectedValueList parse(XContentParser parser) throws IOException {
        Double likelihood = null;
        List<DataByFeatureId> valueList = new ArrayList<>();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case LIKELIHOOD_FIELD:
                    likelihood = parser.doubleValue();
                    break;
                case VALUE_LIST_FIELD:
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        valueList.add(DataByFeatureId.parse(parser));
                    }
                    break;
                default:
                    // the unknown field and it's children should be ignored
                    parser.skipChildren();
                    break;
            }
        }

        return new ExpectedValueList(likelihood, valueList);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ExpectedValueList that = (ExpectedValueList) o;
        return Double.compare(likelihood, that.likelihood) == 0 && Objects.equal(valueList, that.valueList);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(likelihood, valueList);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("likelihood", likelihood).append("valueList", StringUtils.join(valueList, "|")).toString();
    }

    public Double getLikelihood() {
        return likelihood;
    }

    public List<DataByFeatureId> getValueList() {
        return valueList;
    }
}
