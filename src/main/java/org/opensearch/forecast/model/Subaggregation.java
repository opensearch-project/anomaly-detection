/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.model;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.timeseries.annotation.Generated;
import org.opensearch.timeseries.util.ParseUtils;

import com.google.common.base.Objects;

public class Subaggregation implements Writeable, ToXContentObject {
    private static final String AGGREGATION_QUERY = "aggregation_query";
    private static final String ORDER = "order";

    private final AggregationBuilder aggregation;
    private final Order order;

    public Subaggregation(AggregationBuilder aggregation, Order order) {
        super();
        this.aggregation = aggregation;
        this.order = order;
    }

    public Subaggregation(StreamInput input) throws IOException {
        this.aggregation = input.readNamedWriteable(AggregationBuilder.class);
        this.order = input.readEnum(Order.class);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder
            .startObject()
            .field(ORDER, order.name())
            .field(AGGREGATION_QUERY)
            .startObject()
            .value(aggregation)
            .endObject();
        return xContentBuilder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(aggregation);
        out.writeEnum(order);
    }

    /**
     * Parse raw json content into Subaggregation instance.
     *
     * @param parser json based content parser
     * @return feature instance
     * @throws IOException IOException if content can't be parsed correctly
     */
    public static Subaggregation parse(XContentParser parser) throws IOException {
        Order order = Order.ASC;
        AggregationBuilder aggregation = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();

            parser.nextToken();
            switch (fieldName) {
                case ORDER:
                    order = Order.valueOf(parser.text());
                    break;
                case AGGREGATION_QUERY:
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
                    aggregation = ParseUtils.toAggregationBuilder(parser);
                    break;
                default:
                    break;
            }
        }
        return new Subaggregation(aggregation, order);
    }

    @Generated
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Subaggregation feature = (Subaggregation) o;
        return Objects.equal(order, feature.getOrder()) && Objects.equal(aggregation, feature.getAggregation());
    }

    @Generated
    @Override
    public int hashCode() {
        return Objects.hashCode(aggregation, order);
    }

    public AggregationBuilder getAggregation() {
        return aggregation;
    }

    public Order getOrder() {
        return order;
    }
}
