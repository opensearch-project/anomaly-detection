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

package org.opensearch.timeseries.model;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.time.Instant;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.timeseries.annotation.Generated;
import org.opensearch.timeseries.util.ParseUtils;

import com.google.common.base.Objects;

public class DateRange implements ToXContentObject, Writeable {

    public static final String START_TIME_FIELD = "start_time";
    public static final String END_TIME_FIELD = "end_time";

    private final Instant startTime;
    private final Instant endTime;

    public DateRange(Instant startTime, Instant endTime) {
        this.startTime = startTime;
        this.endTime = endTime;
        validate();
    }

    public DateRange(StreamInput in) throws IOException {
        this.startTime = in.readInstant();
        this.endTime = in.readInstant();
        validate();
    }

    private void validate() {
        if (startTime == null) {
            throw new IllegalArgumentException("Detection data range's start time must not be null");
        }
        if (endTime == null) {
            throw new IllegalArgumentException("Detection data range's end time must not be null");
        }
        if (startTime.isAfter(endTime)) {
            throw new IllegalArgumentException("Detection data range's end time must be after start time");
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject();
        xContentBuilder.field(START_TIME_FIELD, startTime.toEpochMilli());
        xContentBuilder.field(END_TIME_FIELD, endTime.toEpochMilli());
        return xContentBuilder.endObject();
    }

    public static DateRange parse(XContentParser parser) throws IOException {
        Instant startTime = null;
        Instant endTime = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case START_TIME_FIELD:
                    startTime = ParseUtils.toInstant(parser);
                    break;
                case END_TIME_FIELD:
                    endTime = ParseUtils.toInstant(parser);
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }
        return new DateRange(startTime, endTime);
    }

    @Generated
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        DateRange that = (DateRange) o;
        return Objects.equal(getStartTime(), that.getStartTime()) && Objects.equal(getEndTime(), that.getEndTime());
    }

    @Generated
    @Override
    public int hashCode() {
        return Objects.hashCode(getStartTime(), getEndTime());
    }

    @Generated
    @Override
    public String toString() {
        return new ToStringBuilder(this).append("startTime", startTime).append("endTime", endTime).toString();
    }

    public Instant getStartTime() {
        return startTime;
    }

    public Instant getEndTime() {
        return endTime;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInstant(startTime);
        out.writeInstant(endTime);
    }
}
