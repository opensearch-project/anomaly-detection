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

package org.opensearch.timeseries.transport;

import java.io.IOException;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.model.Mergeable;

public class SuggestConfigParamResponse extends ActionResponse implements ToXContentObject, Mergeable {
    public static final String INTERVAL_FIELD = "interval";
    public static final String HORIZON_FIELD = "horizon";
    public static final String HISTORY_FIELD = "history";

    private IntervalTimeConfiguration interval;
    private Integer horizon;
    private Integer history;

    public IntervalTimeConfiguration getInterval() {
        return interval;
    }

    public Integer getHorizon() {
        return horizon;
    }

    public Integer getHistory() {
        return history;
    }

    public SuggestConfigParamResponse(IntervalTimeConfiguration interval, Integer horizon, Integer history) {
        this.interval = interval;
        this.horizon = horizon;
        this.history = history;
    }

    public SuggestConfigParamResponse(StreamInput in) throws IOException {
        super(in);
        if (in.readBoolean()) {
            this.interval = IntervalTimeConfiguration.readFrom(in);
        } else {
            this.interval = null;
        }
        this.horizon = in.readOptionalInt();
        this.history = in.readOptionalInt();
    }

    public static class Builder {
        protected IntervalTimeConfiguration interval = null;
        protected Integer horizon = null;
        protected Integer history = null;

        public Builder() {}

        public Builder interval(IntervalTimeConfiguration interval) {
            this.interval = interval;
            return this;
        }

        public Builder horizon(Integer horizon) {
            this.horizon = horizon;
            return this;
        }

        public Builder history(Integer history) {
            this.history = history;
            return this;
        }

        public SuggestConfigParamResponse build() {
            return new SuggestConfigParamResponse(interval, horizon, history);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (interval != null) {
            out.writeBoolean(true);
            interval.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        out.writeOptionalInt(horizon);
        out.writeOptionalInt(history);
    }

    public XContentBuilder toXContent(XContentBuilder builder) throws IOException {
        return toXContent(builder, ToXContent.EMPTY_PARAMS);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject();
        if (interval != null) {
            xContentBuilder.field(INTERVAL_FIELD, interval);
        }
        if (horizon != null) {
            xContentBuilder.field(HORIZON_FIELD, horizon);
        }
        if (history != null) {
            xContentBuilder.field(HISTORY_FIELD, history);
        }

        return xContentBuilder.endObject();
    }

    @Override
    public void merge(Mergeable other) {
        if (this == other || other == null || getClass() != other.getClass()) {
            return;
        }
        SuggestConfigParamResponse otherProfile = (SuggestConfigParamResponse) other;
        if (otherProfile.getInterval() != null) {
            this.interval = otherProfile.getInterval();
        }
        if (otherProfile.getHorizon() != null) {
            this.horizon = otherProfile.getHorizon();
        }
        if (otherProfile.getHistory() != null) {
            this.history = otherProfile.getHistory();
        }
    }
}
