/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.e2e;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.opensearch.timeseries.AbstractSyntheticDataTest;
import org.opensearch.timeseries.dataprocessor.ImputationMethod;

public class MissingSingleFeatureIT extends AbstractMissingSingleFeatureTestCase {
    public static final Logger LOG = (Logger) LogManager.getLogger(MissingSingleFeatureIT.class);

    public void testSingleStream() throws Exception {
        int numberOfEntities = 1;

        AbstractSyntheticDataTest.MISSING_MODE mode = AbstractSyntheticDataTest.MISSING_MODE.MISSING_TIMESTAMP;
        ImputationMethod method = ImputationMethod.ZERO;

        AbstractSyntheticDataTest.GenData dataGenerated = genData(trainTestSplit, numberOfEntities, mode);

        ingestUniformSingleFeatureData(
            -1, // ingest all
            dataGenerated.data
        );

        TrainResult trainResult = createAndStartRealTimeDetector(
            numberOfEntities,
            trainTestSplit,
            dataGenerated.data,
            method,
            false,
            dataGenerated.testStartTime
        );

        runTest(
            dataGenerated.testStartTime,
            dataGenerated,
            trainResult.windowDelay,
            trainResult.detectorId,
            numberOfEntities,
            mode,
            method,
            3,
            true
        );
    }

    public void testHCMissingTimeStamp() throws Exception {
        int numberOfEntities = 2;

        AbstractSyntheticDataTest.MISSING_MODE mode = AbstractSyntheticDataTest.MISSING_MODE.MISSING_TIMESTAMP;
        ImputationMethod method = ImputationMethod.PREVIOUS;

        AbstractSyntheticDataTest.GenData dataGenerated = genData(trainTestSplit, numberOfEntities, mode);

        ingestUniformSingleFeatureData(
            -1, // ingest all
            dataGenerated.data
        );

        TrainResult trainResult = createAndStartRealTimeDetector(
            numberOfEntities,
            trainTestSplit,
            dataGenerated.data,
            method,
            true,
            dataGenerated.testStartTime
        );

        runTest(
            dataGenerated.testStartTime,
            dataGenerated,
            trainResult.windowDelay,
            trainResult.detectorId,
            numberOfEntities,
            mode,
            method,
            3,
            true
        );
    }

    public void testHCMissingEntity() throws Exception {
        int numberOfEntities = 2;

        AbstractSyntheticDataTest.MISSING_MODE mode = AbstractSyntheticDataTest.MISSING_MODE.MISSING_ENTITY;
        ImputationMethod method = ImputationMethod.FIXED_VALUES;

        AbstractSyntheticDataTest.GenData dataGenerated = genData(trainTestSplit, numberOfEntities, mode);

        ingestUniformSingleFeatureData(
            -1, // ingest all
            dataGenerated.data
        );

        TrainResult trainResult = createAndStartRealTimeDetector(
            numberOfEntities,
            trainTestSplit,
            dataGenerated.data,
            method,
            true,
            dataGenerated.testStartTime
        );

        runTest(
            dataGenerated.testStartTime,
            dataGenerated,
            trainResult.windowDelay,
            trainResult.detectorId,
            numberOfEntities,
            mode,
            method,
            3,
            true
        );
    }
}
