/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.model;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

import java.io.IOException;

import org.hamcrest.MatcherAssert;
import org.junit.Before;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.TestHelpers;

public class FeatureImputedTests extends OpenSearchTestCase {

    private FeatureImputed featureImputed;
    private String featureId = "feature_1";
    private Boolean imputed = true;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        featureImputed = new FeatureImputed(featureId, imputed);
    }

    public void testParseFeatureImputed() throws IOException {
        String jsonString = TestHelpers.xContentBuilderToString(featureImputed.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));

        // Parse the JSON content
        XContentParser parser = JsonXContent.jsonXContent
            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, jsonString);
        parser.nextToken(); // move to the first token

        // Call the parse method
        FeatureImputed parsedFeatureImputed = FeatureImputed.parse(parser);

        // Verify the parsed object
        assertNotNull("Parsed FeatureImputed should not be null", parsedFeatureImputed);
        assertEquals("Feature ID should match", featureId, parsedFeatureImputed.getFeatureId());
        assertEquals("Imputed value should match", imputed, parsedFeatureImputed.isImputed());
    }

    public void testWriteToAndReadFrom() throws IOException {
        // Serialize the object
        BytesStreamOutput output = new BytesStreamOutput();
        featureImputed.writeTo(output);

        // Deserialize the object
        StreamInput streamInput = output.bytes().streamInput();
        FeatureImputed readFeatureImputed = new FeatureImputed(streamInput);

        // Verify the deserialized object
        MatcherAssert.assertThat("Feature ID should match", readFeatureImputed.getFeatureId(), equalTo(featureId));
        MatcherAssert.assertThat("Imputed value should match", readFeatureImputed.isImputed(), equalTo(imputed));

        // verify equals/hashCode
        MatcherAssert.assertThat("FeatureImputed should match", featureImputed, equalTo(featureImputed));
        MatcherAssert.assertThat("FeatureImputed should match", featureImputed, not(equalTo(streamInput)));
        MatcherAssert.assertThat("FeatureImputed should match", readFeatureImputed, equalTo(featureImputed));
        MatcherAssert.assertThat("FeatureImputed should match", readFeatureImputed.hashCode(), equalTo(featureImputed.hashCode()));
    }
}
