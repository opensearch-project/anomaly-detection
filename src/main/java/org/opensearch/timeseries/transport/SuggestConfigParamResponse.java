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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
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
    public static final String WINDOW_DELAY_FIELD = "windowDelay";

    private IntervalTimeConfiguration interval;
    private Integer horizon;
    private Integer history;
    private IntervalTimeConfiguration windowDelay;

    public IntervalTimeConfiguration getInterval() {
        return interval;
    }

    public Integer getHorizon() {
        return horizon;
    }

    public Integer getHistory() {
        return history;
    }

    public IntervalTimeConfiguration getWindowDelay() {
        return windowDelay;
    }

    public SuggestConfigParamResponse(
        IntervalTimeConfiguration interval,
        Integer horizon,
        Integer history,
        IntervalTimeConfiguration windowDelay
    ) {
        this.interval = interval;
        this.horizon = horizon;
        this.history = history;
        this.windowDelay = windowDelay;
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
        if (in.readBoolean()) {
            this.windowDelay = IntervalTimeConfiguration.readFrom(in);
        } else {
            this.windowDelay = null;
        }
    }

    public static class Builder {
        protected IntervalTimeConfiguration interval = null;
        protected Integer horizon = null;
        protected Integer history = null;
        protected IntervalTimeConfiguration windowDelay = null;

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

        public Builder windowDelay(IntervalTimeConfiguration windowDelay) {
            this.windowDelay = windowDelay;
            return this;
        }

        public SuggestConfigParamResponse build() {
            return new SuggestConfigParamResponse(interval, horizon, history, windowDelay);
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
        if (windowDelay != null) {
            out.writeBoolean(true);
            windowDelay.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
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
        if (windowDelay != null) {
            xContentBuilder.field(WINDOW_DELAY_FIELD, windowDelay);
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
        if (otherProfile.getWindowDelay() != null) {
            this.windowDelay = otherProfile.getWindowDelay();
        }
    }

    public static SuggestConfigParamResponse fromActionResponse(
        final ActionResponse actionResponse,
        NamedWriteableRegistry namedWriteableRegistry
    ) {
        if (actionResponse instanceof SuggestConfigParamResponse) {
            return (SuggestConfigParamResponse) actionResponse;
        }

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); OutputStreamStreamOutput osso = new OutputStreamStreamOutput(baos)) {
            actionResponse.writeTo(osso);
            try (
                StreamInput input = new InputStreamStreamInput(new ByteArrayInputStream(baos.toByteArray()));
                NamedWriteableAwareStreamInput namedInput = new NamedWriteableAwareStreamInput(input, namedWriteableRegistry)
            ) {
                return new SuggestConfigParamResponse(namedInput);
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("failed to parse ActionResponse into SuggestConfigParamResponse", e);
        }
    }
}
