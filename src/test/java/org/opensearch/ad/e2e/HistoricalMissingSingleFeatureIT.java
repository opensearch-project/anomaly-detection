/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.e2e;

import org.opensearch.timeseries.AbstractSyntheticDataTest;
import org.opensearch.timeseries.dataprocessor.ImputationMethod;

public class HistoricalMissingSingleFeatureIT extends AbstractMissingSingleFeatureTestCase {
    public void testSingleStream() throws Exception {
        int numberOfEntities = 1;

        AbstractSyntheticDataTest.MISSING_MODE mode = AbstractSyntheticDataTest.MISSING_MODE.CONTINUOUS_IMPUTE;
        ImputationMethod method = ImputationMethod.ZERO;

        AbstractSyntheticDataTest.GenData dataGenerated = genData(trainTestSplit, numberOfEntities, mode);

        ingestUniformSingleFeatureData(
            -1, // ingest all
            dataGenerated.data
        );

        TrainResult trainResult = createAndStartHistoricalDetector(
            numberOfEntities,
            trainTestSplit,
            dataGenerated.data,
            method,
            false,
            dataGenerated.testStartTime
        );

        // we allowed 25 (continuousImputeEndIndex - continuousImputeStartIndex + 1) continuous missing timestamps in shouldSkipDataPoint
        runTest(
            dataGenerated.testStartTime,
            dataGenerated,
            trainResult.windowDelay,
            trainResult.detectorId,
            numberOfEntities,
            mode,
            method,
            continuousImputeEndIndex - continuousImputeStartIndex + 1,
            false
        );
    }

    public void testHCFixed() throws Exception {
        int numberOfEntities = 1;

        AbstractSyntheticDataTest.MISSING_MODE mode = AbstractSyntheticDataTest.MISSING_MODE.CONTINUOUS_IMPUTE;
        ImputationMethod method = ImputationMethod.FIXED_VALUES;

        AbstractSyntheticDataTest.GenData dataGenerated = genData(trainTestSplit, numberOfEntities, mode);

        ingestUniformSingleFeatureData(
            -1, // ingest all
            dataGenerated.data
        );

        TrainResult trainResult = createAndStartHistoricalDetector(
            numberOfEntities,
            trainTestSplit,
            dataGenerated.data,
            method,
            true,
            dataGenerated.testStartTime
        );

        // we allowed 25 (continuousImputeEndIndex - continuousImputeStartIndex + 1) continuous missing timestamps in shouldSkipDataPoint
        runTest(
            dataGenerated.testStartTime,
            dataGenerated,
            trainResult.windowDelay,
            trainResult.detectorId,
            numberOfEntities,
            mode,
            method,
            continuousImputeEndIndex - continuousImputeStartIndex + 1,
            false
        );
    }

    public void testHCPrevious() throws Exception {
        int numberOfEntities = 1;

        AbstractSyntheticDataTest.MISSING_MODE mode = AbstractSyntheticDataTest.MISSING_MODE.CONTINUOUS_IMPUTE;
        ImputationMethod method = ImputationMethod.PREVIOUS;

        AbstractSyntheticDataTest.GenData dataGenerated = genData(trainTestSplit, numberOfEntities, mode);

        ingestUniformSingleFeatureData(
            -1, // ingest all
            dataGenerated.data
        );

        TrainResult trainResult = createAndStartHistoricalDetector(
            numberOfEntities,
            trainTestSplit,
            dataGenerated.data,
            method,
            true,
            dataGenerated.testStartTime
        );

        // we allowed 25 (continuousImputeEndIndex - continuousImputeStartIndex + 1) continuous missing timestamps in shouldSkipDataPoint
        runTest(
            dataGenerated.testStartTime,
            dataGenerated,
            trainResult.windowDelay,
            trainResult.detectorId,
            numberOfEntities,
            mode,
            method,
            continuousImputeEndIndex - continuousImputeStartIndex + 1,
            false
        );
    }
}
