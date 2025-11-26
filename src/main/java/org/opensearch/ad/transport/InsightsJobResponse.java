/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ad.transport;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

public class InsightsJobResponse extends ActionResponse implements ToXContentObject {

    private final String message;
    private final List<String> results;
    private final long totalHits;

    // Status fields
    private final String jobName;
    private final Boolean isEnabled;
    private final Instant enabledTime;
    private final Instant disabledTime;
    private final Instant lastUpdateTime;
    private final String scheduleJson; // Schedule as JSON string for easier serialization

    public InsightsJobResponse(String message) {
        this.message = message;
        this.results = new ArrayList<>();
        this.totalHits = 0;
        this.jobName = null;
        this.isEnabled = null;
        this.enabledTime = null;
        this.disabledTime = null;
        this.lastUpdateTime = null;
        this.scheduleJson = null;
    }

    public InsightsJobResponse(List<String> results, long totalHits) {
        this.message = null;
        this.results = results;
        this.totalHits = totalHits;
        this.jobName = null;
        this.isEnabled = null;
        this.enabledTime = null;
        this.disabledTime = null;
        this.lastUpdateTime = null;
        this.scheduleJson = null;
    }

    // Constructor for status response
    public InsightsJobResponse(
        String jobName,
        Boolean isEnabled,
        Instant enabledTime,
        Instant disabledTime,
        Instant lastUpdateTime,
        org.opensearch.jobscheduler.spi.schedule.Schedule schedule
    ) {
        this.message = null;
        this.results = new ArrayList<>();
        this.totalHits = 0;
        this.jobName = jobName;
        this.isEnabled = isEnabled;
        this.enabledTime = enabledTime;
        this.disabledTime = disabledTime;
        this.lastUpdateTime = lastUpdateTime;
        // Convert Schedule to JSON string for serialization
        String tempScheduleJson = null;
        if (schedule != null) {
            try {
                XContentBuilder builder = XContentFactory.jsonBuilder();
                schedule.toXContent(builder, ToXContentObject.EMPTY_PARAMS);
                tempScheduleJson = org.opensearch.core.common.bytes.BytesReference.bytes(builder).utf8ToString();
            } catch (IOException e) {
                // Leave as null
            }
        }
        this.scheduleJson = tempScheduleJson;
    }

    public InsightsJobResponse(StreamInput in) throws IOException {
        super(in);
        this.message = in.readOptionalString();
        this.results = in.readStringList();
        this.totalHits = in.readLong();
        this.jobName = in.readOptionalString();
        this.isEnabled = in.readOptionalBoolean();
        this.enabledTime = in.readOptionalInstant();
        this.disabledTime = in.readOptionalInstant();
        this.lastUpdateTime = in.readOptionalInstant();
        this.scheduleJson = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(message);
        out.writeStringCollection(results);
        out.writeLong(totalHits);
        out.writeOptionalString(jobName);
        out.writeOptionalBoolean(isEnabled);
        out.writeOptionalInstant(enabledTime);
        out.writeOptionalInstant(disabledTime);
        out.writeOptionalInstant(lastUpdateTime);
        out.writeOptionalString(scheduleJson);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (message != null) {
            builder.field("message", message);
        } else if (jobName != null) {
            // Status response
            builder.field("job_name", jobName);
            builder.field("enabled", isEnabled != null ? isEnabled : false);
            if (enabledTime != null) {
                builder.field("enabled_time", enabledTime.toEpochMilli());
            }
            if (disabledTime != null) {
                builder.field("disabled_time", disabledTime.toEpochMilli());
            }
            if (lastUpdateTime != null) {
                builder.field("last_update_time", lastUpdateTime.toEpochMilli());
            }
            if (scheduleJson != null) {
                // Parse and include the schedule JSON
                try (
                    XContentParser parser = XContentType.JSON
                        .xContent()
                        .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, scheduleJson)
                ) {
                    parser.nextToken();
                    builder.field("schedule").copyCurrentStructure(parser);
                }
            }
        } else {
            // Results response
            builder.field("total_hits", totalHits);
            builder.startArray("results");
            for (String result : results) {
                try (
                    XContentParser parser = XContentType.JSON
                        .xContent()
                        .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, result)
                ) {
                    parser.nextToken();
                    builder.copyCurrentStructure(parser);
                }
            }
            builder.endArray();
        }

        builder.endObject();
        return builder;
    }

    public String getMessage() {
        return message;
    }

    public List<String> getResults() {
        return results;
    }

    public long getTotalHits() {
        return totalHits;
    }
}
