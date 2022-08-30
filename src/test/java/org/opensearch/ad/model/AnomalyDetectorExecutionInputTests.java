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

package org.opensearch.ad.model;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.opensearch.ad.TestHelpers;
import org.opensearch.test.OpenSearchTestCase;

public class AnomalyDetectorExecutionInputTests extends OpenSearchTestCase {

    /*public void testParseAnomalyDetectorExecutionInput() throws IOException {
        AnomalyDetectorExecutionInput detectorExecutionInput = TestHelpers.randomAnomalyDetectorExecutionInput();
        String detectInputString = TestHelpers
            .xContentBuilderToString(detectorExecutionInput.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        detectInputString = detectInputString
            .replaceFirst("\\{", String.format(Locale.ROOT, "{\"%s\":\"%s\",", randomAlphaOfLength(5), randomAlphaOfLength(5)));
        AnomalyDetectorExecutionInput parsedAnomalyDetectorExecutionInput = AnomalyDetectorExecutionInput
            .parse(TestHelpers.parser(detectInputString), detectorExecutionInput.getDetectorId());
        assertEquals("Parsing anomaly detect execution input doesn't work", detectorExecutionInput, parsedAnomalyDetectorExecutionInput);
    }*/

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
