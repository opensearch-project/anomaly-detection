/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.ad.transport;

import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.BATCH_TASK_PIECE_INTERVAL_SECONDS;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.junit.Before;
import org.opensearch.common.settings.Settings;

import com.amazon.opendistroforelasticsearch.ad.HistoricalDetectorIntegTestCase;

public class ADTaskProfileTransportActionTests extends HistoricalDetectorIntegTestCase {

    private Instant startTime;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        startTime = Instant.now().minus(10, ChronoUnit.DAYS);
        ingestTestData(testIndex, startTime, detectionIntervalInMinutes, "error", 2000);
        createDetectorIndex();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings
            .builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(BATCH_TASK_PIECE_INTERVAL_SECONDS.getKey(), 1)
            .put(MAX_BATCH_TASK_PER_NODE.getKey(), 1)
            .build();
    }

    public void testProfile() {

    }
}
