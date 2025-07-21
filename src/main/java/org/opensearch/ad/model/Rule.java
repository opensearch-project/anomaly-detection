/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.model;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import com.google.common.base.Objects;

public class Rule implements Writeable, ToXContentObject {
    private static final String ACTION_FIELD = "action";
    private static final String CONDITIONS_FIELD = "conditions";

    private Action action;
    private List<Condition> conditions;

    public Rule(Action action, List<Condition> conditions) {
        this.action = action;
        this.conditions = conditions;
    }

    public Rule(StreamInput input) throws IOException {
        this.action = input.readEnum(Action.class);
        this.conditions = input.readList(Condition::new);
    }

    /**
     * Parse raw json content into rule instance.
     *
     * @param parser json based content parser
     * @return rule instance
     * @throws IOException IOException if content can't be parsed correctly
     */
    public static Rule parse(XContentParser parser) throws IOException {
        Action action = null;
        List<Condition> conditions = new ArrayList<>();

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();

            parser.nextToken();
            switch (fieldName) {
                case ACTION_FIELD:
                    action = Action.valueOf(parser.text().toUpperCase(Locale.ROOT));
                    break;
                case CONDITIONS_FIELD:
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        conditions.add(Condition.parse(parser));
                    }
                    break;
                default:
                    break;
            }
        }
        return new Rule(action, conditions);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject().field(ACTION_FIELD, action).field(CONDITIONS_FIELD, conditions.toArray());
        return xContentBuilder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(action);
        out.writeList(conditions);
    }

    public Action getAction() {
        return action;
    }

    public List<Condition> getConditions() {
        return conditions;
    }

    @Override
    public boolean equals(Object o) {
        if (getClass() != o.getClass()) {
            return false;
        }
        Rule that = (Rule) o;
        return Objects.equal(action, that.action) && Objects.equal(conditions, that.conditions);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + Objects.hashCode(action, conditions);
        return result;
    }

    @Override
    public String toString() {
        return super.toString() + ", " + new ToStringBuilder(this).append("action", action).append("conditions", conditions).toString();
    }
}
