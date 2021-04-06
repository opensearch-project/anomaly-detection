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

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Locale;

import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.test.OpenSearchTestCase;

import com.amazon.opendistroforelasticsearch.ad.TestHelpers;

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
