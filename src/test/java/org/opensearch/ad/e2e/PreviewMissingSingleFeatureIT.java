/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.e2e;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.timeseries.AbstractSyntheticDataTest;
import org.opensearch.timeseries.dataprocessor.ImputationMethod;

public class PreviewMissingSingleFeatureIT extends AbstractMissingSingleFeatureTestCase {
    public static final Logger LOG = (Logger) LogManager.getLogger(MissingSingleFeatureIT.class);

    @SuppressWarnings("unchecked")
    public void testSingleStream() throws Exception {

        int numberOfEntities = 1;

        AbstractSyntheticDataTest.MISSING_MODE mode = AbstractSyntheticDataTest.MISSING_MODE.MISSING_TIMESTAMP;
        ImputationMethod method = ImputationMethod.ZERO;

        AbstractSyntheticDataTest.GenData dataGenerated = genData(trainTestSplit, numberOfEntities, mode);

        ingestUniformSingleFeatureData(
            -1, // ingest all
            dataGenerated.data
        );

        Duration windowDelay = getWindowDelay(dataGenerated.testStartTime);
        String detector = genDetector(trainTestSplit, windowDelay.toMinutes(), false, method, dataGenerated.testStartTime);

        Instant begin = Instant.ofEpochMilli(dataGenerated.data.get(0).get("timestamp").getAsLong());
        Instant end = Instant.ofEpochMilli(dataGenerated.data.get(dataGenerated.data.size() - 1).get("timestamp").getAsLong());
        Map<String, Object> result = preview(detector, begin, end, client());
        // We return java.lang.IllegalArgumentException: Insufficient data for preview results. Minimum required: 400
        // But we return empty results instead. Read comments in AnomalyDetectorRunner.onFailure.
        List<Object> results = (List<Object>) XContentMapValues.extractValue(result, "anomaly_result");
        assertTrue(results.size() == 0);

    }

    @SuppressWarnings("unchecked")
    public void testHC() throws Exception {

        int numberOfEntities = 2;

        AbstractSyntheticDataTest.MISSING_MODE mode = AbstractSyntheticDataTest.MISSING_MODE.MISSING_TIMESTAMP;
        ImputationMethod method = ImputationMethod.ZERO;

        AbstractSyntheticDataTest.GenData dataGenerated = genData(trainTestSplit, numberOfEntities, mode);

        ingestUniformSingleFeatureData(
            -1, // ingest all
            dataGenerated.data
        );

        Duration windowDelay = getWindowDelay(dataGenerated.testStartTime);
        String detector = genDetector(trainTestSplit, windowDelay.toMinutes(), true, method, dataGenerated.testStartTime);

        Instant begin = Instant.ofEpochMilli(dataGenerated.data.get(0).get("timestamp").getAsLong());
        Instant end = Instant.ofEpochMilli(dataGenerated.data.get(dataGenerated.data.size() - 1).get("timestamp").getAsLong());
        Map<String, Object> result = preview(detector, begin, end, client());
        // We return java.lang.IllegalArgumentException: Insufficient data for preview results. Minimum required: 400
        // But we return empty results instead. Read comments in AnomalyDetectorRunner.onFailure.
        List<Object> results = (List<Object>) XContentMapValues.extractValue(result, "anomaly_result");
        assertTrue(results.size() == 0);

    }
}
