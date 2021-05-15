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

package org.opensearch.ad.model;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Locale;

import org.opensearch.ad.TestHelpers;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.test.OpenSearchTestCase;

public class AnomalyDetectorExecutionInputTests extends OpenSearchTestCase {

    public void testParseAnomalyDetectorExecutionInput() throws IOException {
        AnomalyDetectorExecutionInput detectorExecutionInput = TestHelpers.randomAnomalyDetectorExecutionInput();
        String detectInputString = TestHelpers
            .xContentBuilderToString(detectorExecutionInput.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        detectInputString = detectInputString
            .replaceFirst("\\{", String.format(Locale.ROOT, "{\"%s\":\"%s\",", randomAlphaOfLength(5), randomAlphaOfLength(5)));
        AnomalyDetectorExecutionInput parsedAnomalyDetectorExecutionInput = AnomalyDetectorExecutionInput
            .parse(TestHelpers.parser(detectInputString), detectorExecutionInput.getDetectorId());
        assertEquals("Parsing anomaly detect execution input doesn't work", detectorExecutionInput, parsedAnomalyDetectorExecutionInput);
    }

    public void testNullPeriodStart() throws Exception {
        TestHelpers
            .assertFailWith(
                IllegalArgumentException.class,
                () -> new AnomalyDetectorExecutionInput(randomAlphaOfLength(5), null, Instant.now(), null)
            );
    }

    public void testNullPeriodEnd() throws Exception {
        TestHelpers
            .assertFailWith(
                IllegalArgumentException.class,
                () -> new AnomalyDetectorExecutionInput(randomAlphaOfLength(5), Instant.now(), null, null)
            );
    }

    public void testWrongPeriod() throws Exception {
        TestHelpers
            .assertFailWith(
                IllegalArgumentException.class,
                () -> new AnomalyDetectorExecutionInput(
                    randomAlphaOfLength(5),
                    Instant.now(),
                    Instant.now().minus(5, ChronoUnit.MINUTES),
                    null
                )
            );
    }
}
