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
import java.time.Instant;

import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.timeseries.annotation.Generated;
import org.opensearch.timeseries.util.ParseUtils;

import com.google.common.base.Objects;

/**
 * Include anomaly detector's state
 */
public class DetectorInternalState implements ToXContentObject, Cloneable {

    public static final String PARSE_FIELD_NAME = "DetectorInternalState";
    public static final NamedXContentRegistry.Entry XCONTENT_REGISTRY = new NamedXContentRegistry.Entry(
        DetectorInternalState.class,
        new ParseField(PARSE_FIELD_NAME),
        it -> parse(it)
    );

    public static final String LAST_UPDATE_TIME_FIELD = "last_update_time";
    public static final String ERROR_FIELD = "error";

    private Instant lastUpdateTime = null;
    private String error = null;

    private DetectorInternalState() {}

    public static class Builder {
        private Instant lastUpdateTime = null;
        private String error = null;

        public Builder() {}

        public Builder lastUpdateTime(Instant lastUpdateTime) {
            this.lastUpdateTime = lastUpdateTime;
            return this;
        }

        public Builder error(String error) {
            this.error = error;
            return this;
        }

        public DetectorInternalState build() {
            DetectorInternalState state = new DetectorInternalState();
            state.lastUpdateTime = this.lastUpdateTime;
            state.error = this.error;

            return state;
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject();

        if (lastUpdateTime != null) {
            xContentBuilder.field(LAST_UPDATE_TIME_FIELD, lastUpdateTime.toEpochMilli());
        }
        if (error != null) {
            xContentBuilder.field(ERROR_FIELD, error);
        }
        return xContentBuilder.endObject();
    }

    public static DetectorInternalState parse(XContentParser parser) throws IOException {
        Instant lastUpdateTime = null;
        String error = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case LAST_UPDATE_TIME_FIELD:
                    lastUpdateTime = ParseUtils.toInstant(parser);
                    break;
                case ERROR_FIELD:
                    error = parser.text();
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }
        return new DetectorInternalState.Builder().lastUpdateTime(lastUpdateTime).error(error).build();
    }

    @Generated
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        DetectorInternalState that = (DetectorInternalState) o;
        return Objects.equal(getLastUpdateTime(), that.getLastUpdateTime()) && Objects.equal(getError(), that.getError());
    }

    @Generated
    @Override
    public int hashCode() {
        return Objects.hashCode(lastUpdateTime, error);
    }

    @Override
    public Object clone() {
        DetectorInternalState state = null;
        try {
            state = (DetectorInternalState) super.clone();
        } catch (CloneNotSupportedException e) {
            state = new DetectorInternalState.Builder().lastUpdateTime(lastUpdateTime).error(error).build();
        }
        return state;
    }

    public Instant getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(Instant lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }
}
