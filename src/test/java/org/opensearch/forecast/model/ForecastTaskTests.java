/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.model;

import java.io.IOException;

import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.TestHelpers;

public class ForecastTaskTests extends OpenSearchTestCase {
    public void testParse() throws IOException {
        ForecastTask originalTask = TestHelpers.ForecastTaskBuilder.newInstance().build();
        String forecastTaskString = TestHelpers
            .xContentBuilderToString(originalTask.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        ForecastTask parsedForecastTask = ForecastTask.parse(TestHelpers.parser(forecastTaskString));
        assertEquals("Parsing forecast task doesn't work", originalTask, parsedForecastTask);
    }

    public void testParseEmptyForecaster() throws IOException {
        ForecastTask originalTask = TestHelpers.ForecastTaskBuilder.newInstance().setForecaster(null).build();
        String forecastTaskString = TestHelpers
            .xContentBuilderToString(originalTask.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        ForecastTask parsedForecastTask = ForecastTask.parse(TestHelpers.parser(forecastTaskString));
        assertEquals("Parsing forecast task doesn't work", originalTask, parsedForecastTask);
    }

    public void testParseEmptyForecasterRange() throws IOException {
        ForecastTask originalTask = TestHelpers.ForecastTaskBuilder.newInstance().setForecaster(null).setDateRange(null).build();
        String forecastTaskString = TestHelpers
            .xContentBuilderToString(originalTask.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        ForecastTask parsedForecastTask = ForecastTask.parse(TestHelpers.parser(forecastTaskString));
        assertEquals("Parsing forecast task doesn't work", originalTask, parsedForecastTask);
    }
}
