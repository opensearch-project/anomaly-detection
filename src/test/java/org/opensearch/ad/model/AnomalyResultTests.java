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

import java.util.Collection;

import org.opensearch.ad.AnomalyDetectorPlugin;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

public class AnomalyResultTests extends OpenSearchSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class, AnomalyDetectorPlugin.class);
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return getInstanceFromNode(NamedWriteableRegistry.class);
    }

    /*public void testParseAnomalyDetector() throws IOException {
        AnomalyResult detectResult = TestHelpers.randomAnomalyDetectResult(0.8, randomAlphaOfLength(5), null);
        String detectResultString = TestHelpers
            .xContentBuilderToString(detectResult.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        detectResultString = detectResultString
            .replaceFirst("\\{", String.format(Locale.ROOT, "{\"%s\":\"%s\",", randomAlphaOfLength(5), randomAlphaOfLength(5)));
        AnomalyResult parsedDetectResult = AnomalyResult.parse(TestHelpers.parser(detectResultString));
        assertEquals("Parsing anomaly detect result doesn't work", detectResult, parsedDetectResult);
    }*/

    /*public void testParseAnomalyDetectorWithoutUser() throws IOException {
        AnomalyResult detectResult = TestHelpers.randomAnomalyDetectResult(0.8, randomAlphaOfLength(5), randomAlphaOfLength(5), false);
        String detectResultString = TestHelpers
            .xContentBuilderToString(detectResult.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        detectResultString = detectResultString
            .replaceFirst("\\{", String.format(Locale.ROOT, "{\"%s\":\"%s\",", randomAlphaOfLength(5), randomAlphaOfLength(5)));
        AnomalyResult parsedDetectResult = AnomalyResult.parse(TestHelpers.parser(detectResultString));
        assertEquals("Parsing anomaly detect result doesn't work", detectResult, parsedDetectResult);
    }*/

    /*public void testParseAnomalyDetectorWithoutNormalResult() throws IOException {
        AnomalyResult detectResult = TestHelpers.randomHCADAnomalyDetectResult(randomDouble(), randomDouble(), null);
    
        String detectResultString = TestHelpers
            .xContentBuilderToString(detectResult.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        detectResultString = detectResultString
            .replaceFirst("\\{", String.format(Locale.ROOT, "{\"%s\":\"%s\",", randomAlphaOfLength(5), randomAlphaOfLength(5)));
        AnomalyResult parsedDetectResult = AnomalyResult.parse(TestHelpers.parser(detectResultString));
        assertTrue(
            Objects.equal(detectResult.getDetectorId(), parsedDetectResult.getDetectorId())
                && Objects.equal(detectResult.getTaskId(), parsedDetectResult.getTaskId())
                && Objects.equal(detectResult.getAnomalyScore(), parsedDetectResult.getAnomalyScore())
                && Objects.equal(detectResult.getAnomalyGrade(), parsedDetectResult.getAnomalyGrade())
                && Objects.equal(detectResult.getConfidence(), parsedDetectResult.getConfidence())
                && Objects.equal(detectResult.getDataStartTime(), parsedDetectResult.getDataStartTime())
                && Objects.equal(detectResult.getDataEndTime(), parsedDetectResult.getDataEndTime())
                && Objects.equal(detectResult.getExecutionStartTime(), parsedDetectResult.getExecutionStartTime())
                && Objects.equal(detectResult.getExecutionEndTime(), parsedDetectResult.getExecutionEndTime())
                && Objects.equal(detectResult.getError(), parsedDetectResult.getError())
                && Objects.equal(detectResult.getEntity(), parsedDetectResult.getEntity())
                && Objects.equal(detectResult.getFeatureData(), parsedDetectResult.getFeatureData())
        );
    }*/

    /*public void testParseAnomalyDetectorWithNanAnomalyResult() throws IOException {
        AnomalyResult detectResult = TestHelpers.randomHCADAnomalyDetectResult(Double.NaN, Double.NaN, null);
        String detectResultString = TestHelpers
            .xContentBuilderToString(detectResult.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        detectResultString = detectResultString
            .replaceFirst("\\{", String.format(Locale.ROOT, "{\"%s\":\"%s\",", randomAlphaOfLength(5), randomAlphaOfLength(5)));
        AnomalyResult parsedDetectResult = AnomalyResult.parse(TestHelpers.parser(detectResultString));
        assertNull(parsedDetectResult.getAnomalyGrade());
        assertNull(parsedDetectResult.getAnomalyScore());
        assertTrue(
            Objects.equal(detectResult.getDetectorId(), parsedDetectResult.getDetectorId())
                && Objects.equal(detectResult.getTaskId(), parsedDetectResult.getTaskId())
                && Objects.equal(detectResult.getFeatureData(), parsedDetectResult.getFeatureData())
                && Objects.equal(detectResult.getDataStartTime(), parsedDetectResult.getDataStartTime())
                && Objects.equal(detectResult.getDataEndTime(), parsedDetectResult.getDataEndTime())
                && Objects.equal(detectResult.getExecutionStartTime(), parsedDetectResult.getExecutionStartTime())
                && Objects.equal(detectResult.getExecutionEndTime(), parsedDetectResult.getExecutionEndTime())
                && Objects.equal(detectResult.getError(), parsedDetectResult.getError())
                && Objects.equal(detectResult.getEntity(), parsedDetectResult.getEntity())
                && Objects.equal(detectResult.getConfidence(), parsedDetectResult.getConfidence())
        );
    }*/

    /*public void testParseAnomalyDetectorWithTaskId() throws IOException {
        AnomalyResult detectResult = TestHelpers.randomAnomalyDetectResult(0.8, randomAlphaOfLength(5), randomAlphaOfLength(5));
        String detectResultString = TestHelpers
            .xContentBuilderToString(detectResult.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        detectResultString = detectResultString
            .replaceFirst("\\{", String.format(Locale.ROOT, "{\"%s\":\"%s\",", randomAlphaOfLength(5), randomAlphaOfLength(5)));
        AnomalyResult parsedDetectResult = AnomalyResult.parse(TestHelpers.parser(detectResultString));
        assertEquals("Parsing anomaly detect result doesn't work", detectResult, parsedDetectResult);
    }*/

    /*public void testParseAnomalyDetectorWithEntity() throws IOException {
        AnomalyResult detectResult = TestHelpers.randomHCADAnomalyDetectResult(0.8, 0.5);
        String detectResultString = TestHelpers
            .xContentBuilderToString(detectResult.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        detectResultString = detectResultString
            .replaceFirst("\\{", String.format(Locale.ROOT, "{\"%s\":\"%s\",", randomAlphaOfLength(5), randomAlphaOfLength(5)));
        AnomalyResult parsedDetectResult = AnomalyResult.parse(TestHelpers.parser(detectResultString));
        assertEquals("Parsing anomaly detect result doesn't work", detectResult, parsedDetectResult);
    }*/

    /*public void testSerializeAnomalyResult() throws IOException {
        AnomalyResult detectResult = TestHelpers.randomAnomalyDetectResult(0.8, randomAlphaOfLength(5), randomAlphaOfLength(5));
        BytesStreamOutput output = new BytesStreamOutput();
        detectResult.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        AnomalyResult parsedDetectResult = new AnomalyResult(input);
        assertTrue(parsedDetectResult.equals(detectResult));
    }*/

    /*public void testSerializeAnomalyResultWithoutUser() throws IOException {
        AnomalyResult detectResult = TestHelpers.randomAnomalyDetectResult(0.8, randomAlphaOfLength(5), randomAlphaOfLength(5), false);
        BytesStreamOutput output = new BytesStreamOutput();
        detectResult.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        AnomalyResult parsedDetectResult = new AnomalyResult(input);
        assertTrue(parsedDetectResult.equals(detectResult));
    }*/

    /*public void testSerializeAnomalyResultWithEntity() throws IOException {
        AnomalyResult detectResult = TestHelpers.randomHCADAnomalyDetectResult(0.8, 0.5);
        BytesStreamOutput output = new BytesStreamOutput();
        detectResult.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        AnomalyResult parsedDetectResult = new AnomalyResult(input);
        assertTrue(parsedDetectResult.equals(detectResult));
    }*/
}
