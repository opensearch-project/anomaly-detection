/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.dataprocessor;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.BeforeClass;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ImputationOptionTests extends OpenSearchTestCase {
    private static ObjectMapper mapper;
    private static Map<String, Double> map;
    private static String xContent;

    @BeforeClass
    public static void setUpOnce() {
        mapper = new ObjectMapper();
        double[] defaultFill = { 1.0, 2.0, 3.0 };
        map = new HashMap<>();
        map.put("a", defaultFill[0]);
        map.put("b", defaultFill[1]);
        map.put("c", defaultFill[2]);

        xContent = "{"
            + "\"method\":\"FIXED_VALUES\","
            + "\"default_fill\":[{\"feature_name\":\"a\", \"data\":1.0},{\"feature_name\":\"b\", \"data\":2.0},{\"feature_name\":\"c\", \"data\":3.0}]}";
    }

    private Map<String, Double> randomMap(double[] defaultFill) {
        Map<String, Double> map = new HashMap<>();

        for (int i = 0; i < defaultFill.length; i++) {
            String randomKey = UUID.randomUUID().toString(); // generate a random UUID string as the key
            map.put(randomKey, defaultFill[i]);
        }

        return map;
    }

    public void testStreamInputAndOutput() throws IOException {
        // Prepare the data to be read by the StreamInput object.
        ImputationMethod method = ImputationMethod.PREVIOUS;
        double[] defaultFill = { 1.0, 2.0, 3.0 };
        Map<String, Double> map1 = randomMap(defaultFill);

        ImputationOption option = new ImputationOption(method, map1);

        // Write the ImputationOption to the StreamOutput.
        BytesStreamOutput out = new BytesStreamOutput();
        option.writeTo(out);

        StreamInput in = out.bytes().streamInput();

        // Create an ImputationOption using the mocked StreamInput.
        ImputationOption inOption = new ImputationOption(in);

        // Check that the created ImputationOption has the correct values.
        assertEquals(method, inOption.getMethod());
        assertEquals(map1, inOption.getDefaultFill());
    }

    public void testToXContent() throws IOException {

        ImputationOption imputationOption = new ImputationOption(ImputationMethod.FIXED_VALUES, map);

        XContentBuilder builder = imputationOption.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS);
        String actualJson = BytesReference.bytes(builder).utf8ToString();

        JsonNode expectedTree = mapper.readTree(xContent);
        JsonNode actualTree = mapper.readTree(actualJson);

        assertEquals(expectedTree, actualTree);
    }

    public void testParse() throws IOException {

        ImputationOption imputationOption = new ImputationOption(ImputationMethod.FIXED_VALUES, map);

        try (
            XContentParser parser = JsonXContent.jsonXContent
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, xContent)
        ) {
            // advance to first token
            XContentParser.Token token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new IOException("Expected data to start with an Object");
            }

            ImputationOption parsedOption = ImputationOption.parse(parser);

            assertEquals(imputationOption.getMethod(), parsedOption.getMethod());
            assertTrue(imputationOption.getDefaultFill().size() > 0);
            assertTrue(parsedOption.getDefaultFill().size() > 0);

            // The assertEquals method checks if the two maps are equal. The Map interface's equals method ensures that
            // the maps are considered equal if they contain the same key-value pairs, regardless of the order in which
            // they were inserted.
            assertEquals(imputationOption.getDefaultFill(), parsedOption.getDefaultFill());
        }
    }

    public void testEqualsAndHashCode() {
        double[] defaultFill1 = { 1.0, 2.0, 3.0 };

        Map<String, Double> map1 = randomMap(defaultFill1);

        ImputationOption option1 = new ImputationOption(ImputationMethod.FIXED_VALUES, map1);
        ImputationOption option2 = new ImputationOption(ImputationMethod.FIXED_VALUES, map1);
        ImputationOption option3 = new ImputationOption(ImputationMethod.PREVIOUS);

        // Test reflexivity
        assertTrue(option1.equals(option1));

        // Test symmetry
        assertTrue(option1.equals(option2));
        assertTrue(option2.equals(option1));

        // Test transitivity
        ImputationOption option2Clone = new ImputationOption(ImputationMethod.FIXED_VALUES, map1);
        assertTrue(option1.equals(option2));
        assertTrue(option2.equals(option2Clone));
        assertTrue(option1.equals(option2Clone));

        // Test consistency: ultiple invocations of a.equals(b) consistently return true or consistently return false.
        assertTrue(option1.equals(option2));
        assertTrue(option1.equals(option2));

        // Test non-nullity
        assertFalse(option1.equals(null));

        // Test hashCode consistency
        assertEquals(option1.hashCode(), option1.hashCode());

        // Test hashCode equality
        assertTrue(option1.equals(option2));
        assertEquals(option1.hashCode(), option2.hashCode());

        // Test inequality
        assertFalse(option1.equals(option3));
        assertNotEquals(option1.hashCode(), option3.hashCode());
    }
}
