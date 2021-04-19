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

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.junit.Before;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.test.OpenSearchIntegTestCase;

import com.amazon.opendistroforelasticsearch.ad.HistoricalDetectorIntegTestCase;
import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.Feature;
import com.google.common.collect.ImmutableList;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 2)
public class DeleteAnomalyDetectorTransportActionTests extends HistoricalDetectorIntegTestCase {
    private Instant startTime;
    private Instant endTime;
    private String type = "error";

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        startTime = Instant.now().minus(10, ChronoUnit.DAYS);
        endTime = Instant.now();
        ingestTestData(testIndex, startTime, detectionIntervalInMinutes, type, 2000);
        createDetectorIndex();
    }

    public void testDeleteAnomalyDetectorWithoutFeature() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(null);
        testDeleteDetector(detector);
    }

    public void testDeleteAnomalyDetectorWithoutEnabledFeature() throws IOException {
        Feature feature = TestHelpers.randomFeature(false);
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableList.of(feature));
        testDeleteDetector(detector);
    }

    public void testDeleteAnomalyDetectorWithEnabledFeature() throws IOException {
        Feature feature = TestHelpers.randomFeature(true);
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableList.of(feature));
        testDeleteDetector(detector);
    }

    private void testDeleteDetector(AnomalyDetector detector) throws IOException {
        String detectorId = createDetector(detector);
        DeleteAnomalyDetectorRequest request = new DeleteAnomalyDetectorRequest(detectorId);
        DeleteResponse deleteResponse = client().execute(DeleteAnomalyDetectorAction.INSTANCE, request).actionGet(10000);
        System.out.println(deleteResponse);
        assertEquals("deleted", deleteResponse.getResult().getLowercase());
    }
}
