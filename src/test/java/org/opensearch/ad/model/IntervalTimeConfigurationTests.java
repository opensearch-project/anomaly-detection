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

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Locale;

import org.opensearch.ad.TestHelpers;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.model.TimeConfiguration;

public class IntervalTimeConfigurationTests extends OpenSearchTestCase {

    public void testParseIntervalSchedule() throws IOException {
        TimeConfiguration schedule = TestHelpers.randomIntervalTimeConfiguration();
        String scheduleString = TestHelpers.xContentBuilderToString(schedule.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        scheduleString = scheduleString
            .replaceFirst(
                "\"interval",
                String.format(Locale.ROOT, "\"%s\":\"%s\",\"interval", randomAlphaOfLength(5), randomAlphaOfLength(5))
            );
        TimeConfiguration parsedSchedule = TimeConfiguration.parse(TestHelpers.parser(scheduleString));
        assertEquals("Parsing interval schedule doesn't work", schedule, parsedSchedule);
    }

    public void testParseWrongScheduleType() throws Exception {
        TimeConfiguration schedule = TestHelpers.randomIntervalTimeConfiguration();
        String scheduleString = TestHelpers.xContentBuilderToString(schedule.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        String finalScheduleString = scheduleString.replaceFirst("period", randomAlphaOfLength(5));
        TestHelpers
            .assertFailWith(
                IllegalArgumentException.class,
                "Find no schedule definition",
                () -> TimeConfiguration.parse(TestHelpers.parser(finalScheduleString))
            );
    }

    public void testWrongInterval() throws Exception {
        TestHelpers
            .assertFailWith(
                IllegalArgumentException.class,
                "should be non-negative",
                () -> new IntervalTimeConfiguration(randomLongBetween(-100, -1), ChronoUnit.MINUTES)
            );
    }

    public void testWrongUnit() throws Exception {
        TestHelpers
            .assertFailWith(
                IllegalArgumentException.class,
                "is not supported",
                () -> new IntervalTimeConfiguration(randomLongBetween(1, 100), ChronoUnit.MILLIS)
            );
    }

    public void testToDuration() {
        IntervalTimeConfiguration timeConfig = new IntervalTimeConfiguration(/*interval*/1, ChronoUnit.MINUTES);
        assertEquals(Duration.ofMillis(60_000L), timeConfig.toDuration());
    }
}
