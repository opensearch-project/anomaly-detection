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

/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.ad.model;

import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;

import org.apache.logging.log4j.util.Strings;
import org.opensearch.ad.annotation.Generated;
import org.opensearch.ad.util.ParseUtils;
import org.opensearch.common.UUIDs;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.search.aggregations.AggregationBuilder;

import com.google.common.base.Objects;

/**
 * Anomaly detector feature
 */
public class Feature implements Writeable, ToXContentObject {

    private static final String FEATURE_ID_FIELD = "feature_id";
    private static final String FEATURE_NAME_FIELD = "feature_name";
    private static final String FEATURE_ENABLED_FIELD = "feature_enabled";
    private static final String AGGREGATION_QUERY = "aggregation_query";

    private final String id;
    private final String name;
    private final Boolean enabled;
    private final AggregationBuilder aggregation;

    /**
     * Constructor function.
     *  @param id      feature id
     * @param name    feature name
     * @param enabled feature enabled or not
     * @param aggregation feature aggregation query
     */
    public Feature(String id, String name, Boolean enabled, AggregationBuilder aggregation) {
        if (Strings.isBlank(name)) {
            throw new IllegalArgumentException("Feature name should be set");
        }
        if (aggregation == null) {
            throw new IllegalArgumentException("Feature aggregation query should be set");
        }
        this.id = id;
        this.name = name;
        this.enabled = enabled;
        this.aggregation = aggregation;
    }

    public Feature(StreamInput input) throws IOException {
        this.id = input.readString();
        this.name = input.readString();
        this.enabled = input.readBoolean();
        this.aggregation = input.readNamedWriteable(AggregationBuilder.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.id);
        out.writeString(this.name);
        out.writeBoolean(this.enabled);
        out.writeNamedWriteable(aggregation);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder
            .startObject()
            .field(FEATURE_ID_FIELD, id)
            .field(FEATURE_NAME_FIELD, name)
            .field(FEATURE_ENABLED_FIELD, enabled)
            .field(AGGREGATION_QUERY)
            .startObject()
            .value(aggregation)
            .endObject();
        return xContentBuilder.endObject();
    }

    /**
     * Parse raw json content into feature instance.
     *
     * @param parser json based content parser
     * @return feature instance
     * @throws IOException IOException if content can't be parsed correctly
     */
    public static Feature parse(XContentParser parser) throws IOException {
        String id = UUIDs.base64UUID();
        String name = null;
        Boolean enabled = null;
        AggregationBuilder aggregation = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();

            parser.nextToken();
            switch (fieldName) {
                case FEATURE_ID_FIELD:
                    id = parser.text();
                    break;
                case FEATURE_NAME_FIELD:
                    name = parser.text();
                    break;
                case FEATURE_ENABLED_FIELD:
                    enabled = parser.booleanValue();
                    break;
                case AGGREGATION_QUERY:
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
                    aggregation = ParseUtils.toAggregationBuilder(parser);
                    break;
                default:
                    break;
            }
        }
        return new Feature(id, name, enabled, aggregation);
    }

    @Generated
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Feature feature = (Feature) o;
        return Objects.equal(getId(), feature.getId())
            && Objects.equal(getName(), feature.getName())
            && Objects.equal(getEnabled(), feature.getEnabled())
            && Objects.equal(getAggregation(), feature.getAggregation());
    }

    @Generated
    @Override
    public int hashCode() {
        return Objects.hashCode(id, name, enabled);
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public Boolean getEnabled() {
        return enabled;
    }

    public AggregationBuilder getAggregation() {
        return aggregation;
    }
}
