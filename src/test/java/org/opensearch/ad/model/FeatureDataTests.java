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

import java.io.IOException;
import java.util.Locale;

import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.TestHelpers;

public class FeatureDataTests extends OpenSearchTestCase {

    public void testParseAnomalyDetector() throws IOException {
        FeatureData featureData = TestHelpers.randomFeatureData();
        String featureDataString = TestHelpers
            .xContentBuilderToString(featureData.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        featureDataString = featureDataString
            .replaceFirst("\\{", String.format(Locale.ROOT, "{\"%s\":\"%s\",", randomAlphaOfLength(5), randomAlphaOfLength(5)));
        FeatureData parsedFeatureData = FeatureData.parse(TestHelpers.parser(featureDataString));
        assertEquals("Parsing feature data doesn't work", featureData, parsedFeatureData);
    }
}
