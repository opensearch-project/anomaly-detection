/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.model;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.junit.Before;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.FeatureData;

public class ForecastResultTests extends OpenSearchTestCase {
    List<ForecastResult> result;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        // Arrange
        String forecasterId = "testId";
        long intervalMillis = 1000;
        Double dataQuality = 0.9;
        List<FeatureData> featureData = new ArrayList<>();
        featureData.add(new FeatureData("f1", "f1", 1.0d));
        featureData.add(new FeatureData("f2", "f2", 2.0d));
        long currentTimeMillis = System.currentTimeMillis();
        Instant instantFromMillis = Instant.ofEpochMilli(currentTimeMillis);
        Instant dataStartTime = instantFromMillis;
        Instant dataEndTime = dataStartTime.plusSeconds(10);
        Instant executionStartTime = instantFromMillis;
        Instant executionEndTime = executionStartTime.plusSeconds(10);
        String error = null;
        Optional<Entity> entity = Optional.empty();
        User user = new User("testUser", Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
        Integer schemaVersion = 1;
        String modelId = "testModelId";
        float[] forecastsValues = new float[] { 1.0f, 2.0f, 3.0f, 4.0f };
        float[] forecastsUppers = new float[] { 1.5f, 2.5f, 3.5f, 4.5f };
        float[] forecastsLowers = new float[] { 0.5f, 1.5f, 2.5f, 3.5f };
        String taskId = "testTaskId";

        // Act
        result = ForecastResult
            .fromRawRCFCasterResult(
                forecasterId,
                intervalMillis,
                dataQuality,
                featureData,
                dataStartTime,
                dataEndTime,
                executionStartTime,
                executionEndTime,
                error,
                entity,
                user,
                schemaVersion,
                modelId,
                forecastsValues,
                forecastsUppers,
                forecastsLowers,
                taskId
            );
    }

    public void testFromRawRCFCasterResult() {
        // Assert
        assertEquals(5, result.size());
        assertEquals("f1", result.get(1).getFeatureId());
        assertEquals(1.0f, result.get(1).getForecastValue(), 0.01);
        assertEquals("f2", result.get(2).getFeatureId());
        assertEquals(2.0f, result.get(2).getForecastValue(), 0.01);

        assertTrue(
            "actual: " + result.toString(),
            result
                .toString()
                .contains(
                    "featureId=f2,dataQuality=0.9,forecastValue=2.0,lowerBound=1.5,upperBound=2.5,confidenceIntervalWidth=1.0,forecastDataStartTime="
                )
        );
    }

    public void testParseAnomalyDetector() throws IOException {
        for (int i = 0; i < 5; i++) {
            String forecastResultString = TestHelpers
                .xContentBuilderToString(result.get(i).toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
            ForecastResult parsedForecastResult = ForecastResult.parse(TestHelpers.parser(forecastResultString));
            assertEquals("Parsing forecast result doesn't work", result.get(i), parsedForecastResult);
            assertTrue("Parsing forecast result doesn't work", result.get(i).hashCode() == parsedForecastResult.hashCode());
        }
    }
}
