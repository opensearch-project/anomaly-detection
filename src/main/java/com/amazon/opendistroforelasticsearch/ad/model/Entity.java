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

package com.amazon.opendistroforelasticsearch.ad.model;

import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;

import com.amazon.opendistroforelasticsearch.ad.annotation.Generated;
import com.google.common.base.Objects;

/**
 * Categorical field name and its value
 * @author kaituo
 *
 */
public class Entity implements ToXContentObject, Writeable {
    public static final String ENTITY_NAME_FIELD = "name";
    public static final String ENTITY_VALUE_FIELD = "value";

    private final String name;
    private final String value;

    public Entity(String name, String value) {
        this.name = name;
        this.value = value;
    }

    public Entity(StreamInput input) throws IOException {
        this.name = input.readString();
        this.value = input.readString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject().field(ENTITY_NAME_FIELD, name).field(ENTITY_VALUE_FIELD, value);
        return xContentBuilder.endObject();
    }

    public static Entity parse(XContentParser parser) throws IOException {
        String parsedValue = null;
        String parsedName = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case ENTITY_NAME_FIELD:
                    parsedName = parser.text();
                    break;
                case ENTITY_VALUE_FIELD:
                    parsedValue = parser.text();
                    break;
                default:
                    break;
            }
        }
        return new Entity(parsedName, parsedValue);
    }

    @Generated
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Entity that = (Entity) o;
        return Objects.equal(getName(), that.getName()) && Objects.equal(getValue(), that.getValue());
    }

    @Generated
    @Override
    public int hashCode() {
        return Objects.hashCode(getName(), getValue());
    }

    @Generated
    public String getName() {
        return name;
    }

    @Generated
    public String getValue() {
        return value;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(value);
    }
}
