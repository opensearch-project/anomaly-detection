/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.dataprocessor;

import java.io.IOException;
import java.util.Optional;

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

public class ImputationOptionTests extends OpenSearchTestCase {

    public void testStreamInputAndOutput() throws IOException {
        // Prepare the data to be read by the StreamInput object.
        ImputationMethod method = ImputationMethod.PREVIOUS;
        double[] defaultFill = { 1.0, 2.0, 3.0 };

        ImputationOption option = new ImputationOption(method, Optional.of(defaultFill), false);

        // Write the ImputationOption to the StreamOutput.
        BytesStreamOutput out = new BytesStreamOutput();
        option.writeTo(out);

        StreamInput in = out.bytes().streamInput();

        // Create an ImputationOption using the mocked StreamInput.
        ImputationOption inOption = new ImputationOption(in);

        // Check that the created ImputationOption has the correct values.
        assertEquals(method, inOption.getMethod());
        assertArrayEquals(defaultFill, inOption.getDefaultFill().get(), 1e-6);
    }

    public void testToXContent() throws IOException {
        double[] defaultFill = { 1.0, 2.0, 3.0 };
        ImputationOption imputationOption = new ImputationOption(ImputationMethod.FIXED_VALUES, Optional.of(defaultFill), false);

        String xContent = "{" + "\"method\":\"FIXED_VALUES\"," + "\"defaultFill\":[1.0,2.0,3.0],\"integerSensitive\":false" + "}";

        XContentBuilder builder = imputationOption.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS);
        String actualJson = BytesReference.bytes(builder).utf8ToString();

        assertEquals(xContent, actualJson);
    }

    public void testParse() throws IOException {
        String xContent = "{" + "\"method\":\"FIXED_VALUES\"," + "\"defaultFill\":[1.0,2.0,3.0],\"integerSensitive\":false" + "}";

        double[] defaultFill = { 1.0, 2.0, 3.0 };
        ImputationOption imputationOption = new ImputationOption(ImputationMethod.FIXED_VALUES, Optional.of(defaultFill), false);

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
            assertTrue(imputationOption.getDefaultFill().isPresent());
            assertTrue(parsedOption.getDefaultFill().isPresent());
            assertEquals(imputationOption.getDefaultFill().get().length, parsedOption.getDefaultFill().get().length);
            for (int i = 0; i < imputationOption.getDefaultFill().get().length; i++) {
                assertEquals(imputationOption.getDefaultFill().get()[i], parsedOption.getDefaultFill().get()[i], 0);
            }
        }
    }

    public void testEqualsAndHashCode() {
        double[] defaultFill1 = { 1.0, 2.0, 3.0 };
        double[] defaultFill2 = { 4.0, 5.0, 6.0 };

        ImputationOption option1 = new ImputationOption(ImputationMethod.FIXED_VALUES, Optional.of(defaultFill1), false);
        ImputationOption option2 = new ImputationOption(ImputationMethod.FIXED_VALUES, Optional.of(defaultFill1), false);
        ImputationOption option3 = new ImputationOption(ImputationMethod.LINEAR, Optional.of(defaultFill2), false);

        // Test reflexivity
        assertTrue(option1.equals(option1));

        // Test symmetry
        assertTrue(option1.equals(option2));
        assertTrue(option2.equals(option1));

        // Test transitivity
        ImputationOption option2Clone = new ImputationOption(ImputationMethod.FIXED_VALUES, Optional.of(defaultFill1), false);
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
