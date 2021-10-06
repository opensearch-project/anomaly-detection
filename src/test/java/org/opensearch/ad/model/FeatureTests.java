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

import org.opensearch.ad.TestHelpers;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.test.OpenSearchTestCase;

public class FeatureTests extends OpenSearchTestCase {

    public void testParseFeature() throws IOException {
        Feature feature = TestHelpers.randomFeature();
        String featureString = TestHelpers.xContentBuilderToString(feature.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        featureString = featureString
            .replaceFirst("\\{", String.format(Locale.ROOT, "{\"%s\":\"%s\",", randomAlphaOfLength(5), randomAlphaOfLength(5)));
        Feature parsedFeature = Feature.parse(TestHelpers.parser(featureString));
        assertEquals("Parsing feature doesn't work", feature, parsedFeature);
    }

    public void testNullName() throws Exception {
        TestHelpers
            .assertFailWith(
                IllegalArgumentException.class,
                () -> new Feature(randomAlphaOfLength(5), null, true, TestHelpers.randomAggregation())
            );
    }

}
