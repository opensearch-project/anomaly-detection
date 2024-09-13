/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.e2e;

import java.util.List;
import java.util.Map;

import org.opensearch.common.xcontent.support.XContentMapValues;

public class PreviewRuleIT extends AbstractRuleTestCase {
    @SuppressWarnings("unchecked")
    public void testRule() throws Exception {
        // TODO: this test case will run for a much longer time and timeout with security enabled
        if (!isHttps()) {
            disableResourceNotFoundFaultTolerence();

            String datasetName = "rule";
            int intervalMinutes = 10;
            int numberOfEntities = 2;
            int trainTestSplit = 100;

            TrainResult trainResult = ingestTrainData(
                datasetName,
                intervalMinutes,
                numberOfEntities,
                trainTestSplit,
                true,
                // ingest just enough for finish the test
                (trainTestSplit + 1) * numberOfEntities
            );

            String detector = genDetector(datasetName, intervalMinutes, trainTestSplit, trainResult, true);
            Map<String, Object> result = preview(detector, trainResult.firstDataTime, trainResult.finalDataTime, client());
            List<Object> results = (List<Object>) XContentMapValues.extractValue(result, "anomaly_result");
            assertTrue(results.size() > 100);
            Map<String, Object> firstResult = (Map<String, Object>) results.get(0);
            assertTrue((Double) XContentMapValues.extractValue(firstResult, "anomaly_grade") >= 0);
            List<Object> feature = (List<Object>) XContentMapValues.extractValue(firstResult, "feature_data");
            Map<String, Object> firstFeatureValue = (Map<String, Object>) feature.get(0);
            assertTrue((Double) XContentMapValues.extractValue(firstFeatureValue, "data") != null);
        }
    }
}
