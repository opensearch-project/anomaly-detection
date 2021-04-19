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
import java.util.Locale;

import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.test.OpenSearchTestCase;

import com.amazon.opendistroforelasticsearch.ad.TestHelpers;

public class AnomalyDetectorJobTests extends OpenSearchTestCase {

    public void testParseAnomalyDetectorJob() throws IOException {
        AnomalyDetectorJob anomalyDetectorJob = TestHelpers.randomAnomalyDetectorJob();
        String anomalyDetectorJobString = TestHelpers
            .xContentBuilderToString(anomalyDetectorJob.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        anomalyDetectorJobString = anomalyDetectorJobString
            .replaceFirst("\\{", String.format(Locale.ROOT, "{\"%s\":\"%s\",", randomAlphaOfLength(5), randomAlphaOfLength(5)));

        AnomalyDetectorJob parsedAnomalyDetectorJob = AnomalyDetectorJob.parse(TestHelpers.parser(anomalyDetectorJobString));
        assertEquals("Parsing anomaly detect result doesn't work", anomalyDetectorJob, parsedAnomalyDetectorJob);
    }
}
