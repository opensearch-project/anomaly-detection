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

import org.opensearch.commons.authuser.User;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.spi.schedule.CronSchedule;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.jobscheduler.spi.schedule.Schedule;
import org.opensearch.jobscheduler.spi.schedule.ScheduleParser;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.ParseUtils;

import com.google.common.base.Objects;

/**
 * Anomaly detector job.
 */
public class Job implements Writeable, ToXContentObject, ScheduledJobParameter {
    enum ScheduleType {
        CRON,
        INTERVAL
    }

    public static final String PARSE_FIELD_NAME = "TimeSeriesJob";
    public static final NamedXContentRegistry.Entry XCONTENT_REGISTRY = new NamedXContentRegistry.Entry(
        Job.class,
        new ParseField(PARSE_FIELD_NAME),
        it -> parse(it)
    );

    public static final String NAME_FIELD = "name";
    public static final String LAST_UPDATE_TIME_FIELD = "last_update_time";
    public static final String LOCK_DURATION_SECONDS = "lock_duration_seconds";

    public static final String SCHEDULE_FIELD = "schedule";
    public static final String WINDOW_DELAY_FIELD = "window_delay";
    public static final String IS_ENABLED_FIELD = "enabled";
    public static final String ENABLED_TIME_FIELD = "enabled_time";
    public static final String DISABLED_TIME_FIELD = "disabled_time";
    public static final String USER_FIELD = "user";
    private static final String RESULT_INDEX_FIELD = "result_index";
    private static final String TYPE_FIELD = "type";

    // name is config id
    private final String name;
    private final Schedule schedule;
    private final TimeConfiguration windowDelay;
    private final Boolean isEnabled;
    private final Instant enabledTime;
    private final Instant disabledTime;
    private final Instant lastUpdateTime;
    private final Long lockDurationSeconds;
    private final User user;
    private String resultIndex;
    private AnalysisType analysisType;

    public Job(
        String name,
        Schedule schedule,
        TimeConfiguration windowDelay,
        Boolean isEnabled,
        Instant enabledTime,
        Instant disabledTime,
        Instant lastUpdateTime,
        Long lockDurationSeconds,
        User user,
        String resultIndex,
        AnalysisType type
    ) {
        this.name = name;
        this.schedule = schedule;
        this.windowDelay = windowDelay;
        this.isEnabled = isEnabled;
        this.enabledTime = enabledTime;
        this.disabledTime = disabledTime;
        this.lastUpdateTime = lastUpdateTime;
        this.lockDurationSeconds = lockDurationSeconds;
        this.user = user;
        this.resultIndex = resultIndex;
        this.analysisType = type;
    }

    public Job(StreamInput input) throws IOException {
        name = input.readString();
        if (input.readEnum(Job.ScheduleType.class) == ScheduleType.CRON) {
            schedule = new CronSchedule(input);
        } else {
            schedule = new IntervalSchedule(input);
        }
        windowDelay = IntervalTimeConfiguration.readFrom(input);
        isEnabled = input.readBoolean();
        enabledTime = input.readInstant();
        disabledTime = input.readInstant();
        lastUpdateTime = input.readInstant();
        lockDurationSeconds = input.readLong();
        if (input.readBoolean()) {
            user = new User(input);
        } else {
            user = null;
        }
        resultIndex = input.readOptionalString();
        this.analysisType = input.readEnum(AnalysisType.class);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder
            .startObject()
            .field(NAME_FIELD, name)
            .field(SCHEDULE_FIELD, schedule)
            .field(WINDOW_DELAY_FIELD, windowDelay)
            .field(IS_ENABLED_FIELD, isEnabled)
            .field(ENABLED_TIME_FIELD, enabledTime.toEpochMilli())
            .field(LAST_UPDATE_TIME_FIELD, lastUpdateTime.toEpochMilli())
            .field(LOCK_DURATION_SECONDS, lockDurationSeconds)
            .field(TYPE_FIELD, analysisType);
        if (disabledTime != null) {
            xContentBuilder.field(DISABLED_TIME_FIELD, disabledTime.toEpochMilli());
        }
        if (user != null) {
            xContentBuilder.field(USER_FIELD, user);
        }
        if (resultIndex != null) {
            xContentBuilder.field(RESULT_INDEX_FIELD, resultIndex);
        }
        return xContentBuilder.endObject();
    }

    @Override
    public void writeTo(StreamOutput output) throws IOException {
        output.writeString(name);
        if (schedule instanceof CronSchedule) {
            output.writeEnum(ScheduleType.CRON);
        } else {
            output.writeEnum(ScheduleType.INTERVAL);
        }
        schedule.writeTo(output);
        windowDelay.writeTo(output);
        output.writeBoolean(isEnabled);
        output.writeInstant(enabledTime);
        output.writeInstant(disabledTime);
        output.writeInstant(lastUpdateTime);
        output.writeLong(lockDurationSeconds);
        if (user != null) {
            output.writeBoolean(true); // user exists
            user.writeTo(output);
        } else {
            output.writeBoolean(false); // user does not exist
        }
        output.writeOptionalString(resultIndex);
        output.writeEnum(analysisType);
    }

    public static Job parse(XContentParser parser) throws IOException {
        String name = null;
        Schedule schedule = null;
        TimeConfiguration windowDelay = null;
        // we cannot set it to null as isEnabled() would do the unboxing and results in null pointer exception
        Boolean isEnabled = Boolean.FALSE;
        Instant enabledTime = null;
        Instant disabledTime = null;
        Instant lastUpdateTime = null;
        Long lockDurationSeconds = TimeSeriesSettings.DEFAULT_JOB_LOC_DURATION_SECONDS;
        User user = null;
        String resultIndex = null;
        String analysisType = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case NAME_FIELD:
                    name = parser.text();
                    break;
                case SCHEDULE_FIELD:
                    schedule = ScheduleParser.parse(parser);
                    break;
                case WINDOW_DELAY_FIELD:
                    windowDelay = TimeConfiguration.parse(parser);
                    break;
                case IS_ENABLED_FIELD:
                    isEnabled = parser.booleanValue();
                    break;
                case ENABLED_TIME_FIELD:
                    enabledTime = ParseUtils.toInstant(parser);
                    break;
                case DISABLED_TIME_FIELD:
                    disabledTime = ParseUtils.toInstant(parser);
                    break;
                case LAST_UPDATE_TIME_FIELD:
                    lastUpdateTime = ParseUtils.toInstant(parser);
                    break;
                case LOCK_DURATION_SECONDS:
                    lockDurationSeconds = parser.longValue();
                    break;
                case USER_FIELD:
                    user = User.parse(parser);
                    break;
                case RESULT_INDEX_FIELD:
                    resultIndex = parser.text();
                    break;
                case TYPE_FIELD:
                    analysisType = parser.text();
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }
        return new Job(
            name,
            schedule,
            windowDelay,
            isEnabled,
            enabledTime,
            disabledTime,
            lastUpdateTime,
            lockDurationSeconds,
            user,
            resultIndex,
            (Strings.isEmpty(analysisType) || AnalysisType.AD == AnalysisType.valueOf(analysisType))
                ? AnalysisType.AD
                : AnalysisType.FORECAST
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Job that = (Job) o;
        return Objects.equal(getName(), that.getName())
            && Objects.equal(getSchedule(), that.getSchedule())
            && Objects.equal(isEnabled(), that.isEnabled())
            && Objects.equal(getEnabledTime(), that.getEnabledTime())
            && Objects.equal(getDisabledTime(), that.getDisabledTime())
            && Objects.equal(getLastUpdateTime(), that.getLastUpdateTime())
            && Objects.equal(getLockDurationSeconds(), that.getLockDurationSeconds())
            && Objects.equal(getCustomResultIndexOrAlias(), that.getCustomResultIndexOrAlias())
            && Objects.equal(getAnalysisType(), that.getAnalysisType());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name, schedule, isEnabled, enabledTime, lastUpdateTime, analysisType);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Schedule getSchedule() {
        return schedule;
    }

    public TimeConfiguration getWindowDelay() {
        return windowDelay;
    }

    @Override
    public boolean isEnabled() {
        return isEnabled;
    }

    @Override
    public Instant getEnabledTime() {
        return enabledTime;
    }

    public Instant getDisabledTime() {
        return disabledTime;
    }

    @Override
    public Instant getLastUpdateTime() {
        return lastUpdateTime;
    }

    @Override
    public Long getLockDurationSeconds() {
        return lockDurationSeconds;
    }

    public User getUser() {
        return user;
    }

    public String getCustomResultIndexOrAlias() {
        return resultIndex;
    }

    public AnalysisType getAnalysisType() {
        return analysisType;
    }
}
