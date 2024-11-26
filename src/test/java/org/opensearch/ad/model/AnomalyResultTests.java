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
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.FeatureData;

import com.google.common.base.Objects;

public class AnomalyResultTests extends OpenSearchSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class, TimeSeriesAnalyticsPlugin.class);
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return getInstanceFromNode(NamedWriteableRegistry.class);
    }

    public void testParseAnomalyDetector() throws IOException {
        AnomalyResult detectResult = TestHelpers.randomAnomalyDetectResult(0.8, randomAlphaOfLength(5), null);
        String detectResultString = TestHelpers
            .xContentBuilderToString(detectResult.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        detectResultString = detectResultString
            .replaceFirst("\\{", String.format(Locale.ROOT, "{\"%s\":\"%s\",", randomAlphaOfLength(5), randomAlphaOfLength(5)));
        AnomalyResult parsedDetectResult = AnomalyResult.parse(TestHelpers.parser(detectResultString));
        assertEquals("Parsing anomaly detect result doesn't work", detectResult, parsedDetectResult);
    }

    public void testParseAnomalyDetectorWithoutUser() throws IOException {
        AnomalyResult detectResult = TestHelpers.randomAnomalyDetectResult(0.8, randomAlphaOfLength(5), randomAlphaOfLength(5), false);
        String detectResultString = TestHelpers
            .xContentBuilderToString(detectResult.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        detectResultString = detectResultString
            .replaceFirst("\\{", String.format(Locale.ROOT, "{\"%s\":\"%s\",", randomAlphaOfLength(5), randomAlphaOfLength(5)));
        AnomalyResult parsedDetectResult = AnomalyResult.parse(TestHelpers.parser(detectResultString));
        assertEquals("Parsing anomaly detect result doesn't work", detectResult, parsedDetectResult);
    }

    public void testParseAnomalyDetectorWithoutNormalResult() throws IOException {
        AnomalyResult detectResult = TestHelpers.randomHCADAnomalyDetectResult(randomDouble(), randomDouble(), null);

        String detectResultString = TestHelpers
            .xContentBuilderToString(detectResult.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        detectResultString = detectResultString
            .replaceFirst("\\{", String.format(Locale.ROOT, "{\"%s\":\"%s\",", randomAlphaOfLength(5), randomAlphaOfLength(5)));
        AnomalyResult parsedDetectResult = AnomalyResult.parse(TestHelpers.parser(detectResultString));
        assertTrue(
            Objects.equal(detectResult.getConfigId(), parsedDetectResult.getConfigId())
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
    }

    public void testParseAnomalyDetectorWithNanAnomalyResult() throws IOException {
        AnomalyResult detectResult = TestHelpers.randomHCADAnomalyDetectResult(Double.NaN, Double.NaN, null);
        String detectResultString = TestHelpers
            .xContentBuilderToString(detectResult.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        detectResultString = detectResultString
            .replaceFirst("\\{", String.format(Locale.ROOT, "{\"%s\":\"%s\",", randomAlphaOfLength(5), randomAlphaOfLength(5)));
        AnomalyResult parsedDetectResult = AnomalyResult.parse(TestHelpers.parser(detectResultString));
        assertNull(parsedDetectResult.getAnomalyGrade());
        assertNull(parsedDetectResult.getAnomalyScore());
        assertTrue(
            Objects.equal(detectResult.getConfigId(), parsedDetectResult.getConfigId())
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
    }

    public void testParseAnomalyDetectorWithTaskId() throws IOException {
        AnomalyResult detectResult = TestHelpers.randomAnomalyDetectResult(0.8, randomAlphaOfLength(5), randomAlphaOfLength(5));
        String detectResultString = TestHelpers
            .xContentBuilderToString(detectResult.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        detectResultString = detectResultString
            .replaceFirst("\\{", String.format(Locale.ROOT, "{\"%s\":\"%s\",", randomAlphaOfLength(5), randomAlphaOfLength(5)));
        AnomalyResult parsedDetectResult = AnomalyResult.parse(TestHelpers.parser(detectResultString));
        assertEquals("Parsing anomaly detect result doesn't work", detectResult, parsedDetectResult);
    }

    public void testParseAnomalyDetectorWithEntity() throws IOException {
        AnomalyResult detectResult = TestHelpers.randomHCADAnomalyDetectResult(0.8, 0.5);
        String detectResultString = TestHelpers
            .xContentBuilderToString(detectResult.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        detectResultString = detectResultString
            .replaceFirst("\\{", String.format(Locale.ROOT, "{\"%s\":\"%s\",", randomAlphaOfLength(5), randomAlphaOfLength(5)));
        AnomalyResult parsedDetectResult = AnomalyResult.parse(TestHelpers.parser(detectResultString));
        assertEquals("Parsing anomaly detect result doesn't work", detectResult, parsedDetectResult);
    }

    public void testSerializeAnomalyResult() throws IOException {
        AnomalyResult detectResult = TestHelpers.randomAnomalyDetectResult(0.8, randomAlphaOfLength(5), randomAlphaOfLength(5));
        BytesStreamOutput output = new BytesStreamOutput();
        detectResult.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        AnomalyResult parsedDetectResult = new AnomalyResult(input);
        assertTrue(parsedDetectResult.equals(detectResult));
    }

    public void testSerializeAnomalyResultWithoutUser() throws IOException {
        AnomalyResult detectResult = TestHelpers.randomAnomalyDetectResult(0.8, randomAlphaOfLength(5), randomAlphaOfLength(5), false);
        BytesStreamOutput output = new BytesStreamOutput();
        detectResult.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        AnomalyResult parsedDetectResult = new AnomalyResult(input);
        assertTrue(parsedDetectResult.equals(detectResult));
    }

    public void testSerializeAnomalyResultWithEntity() throws IOException {
        AnomalyResult detectResult = TestHelpers.randomHCADAnomalyDetectResult(0.8, 0.5);
        BytesStreamOutput output = new BytesStreamOutput();
        detectResult.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        AnomalyResult parsedDetectResult = new AnomalyResult(input);
        assertTrue(parsedDetectResult.equals(detectResult));
    }

    public void testFromRawTRCFResultWithHighConfidence() {
        // Set up test parameters
        String detectorId = "test-detector-id";
        long intervalMillis = 60000; // Example interval
        String taskId = "test-task-id";
        Double rcfScore = 0.5;
        Double grade = 0.0; // Non-anomalous
        Double confidence = 1.03; // Confidence greater than 1
        List<FeatureData> featureData = Collections.emptyList(); // Assuming empty for simplicity
        Instant dataStartTime = Instant.now();
        Instant dataEndTime = dataStartTime.plusMillis(intervalMillis);
        Instant executionStartTime = Instant.now();
        Instant executionEndTime = executionStartTime.plusMillis(500);
        String error = null;
        Optional<Entity> entity = Optional.empty();
        User user = null; // Replace with actual user if needed
        Integer schemaVersion = 1;
        String modelId = "test-model-id";
        double[] relevantAttribution = null;
        Integer relativeIndex = null;
        double[] pastValues = null;
        double[][] expectedValuesList = null;
        double[] likelihoodOfValues = null;
        Double threshold = null;
        double[] currentData = null;
        boolean[] featureImputed = null;

        // Invoke the method under test
        AnomalyResult result = AnomalyResult
            .fromRawTRCFResult(
                detectorId,
                intervalMillis,
                taskId,
                rcfScore,
                grade,
                confidence,
                featureData,
                dataStartTime,
                dataEndTime,
                executionStartTime,
                executionEndTime,
                error,
                entity,
                user,
                schemaVersion,
                modelId,
                relevantAttribution,
                relativeIndex,
                pastValues,
                expectedValuesList,
                likelihoodOfValues,
                threshold,
                currentData,
                featureImputed,
                Collections.emptyList()
            );

        // Assert that the confidence is capped at 1.0
        assertEquals("Confidence should be capped at 1.0", 1.0, result.getConfidence(), 0.00001);
    }

    public void testFromRawTRCFResultWithRules() {
        // Set up test parameters
        String detectorId = "test-detector-id";
        long intervalMillis = 60000; // Example interval
        String taskId = "test-task-id";
        Double rcfScore = 0.5;
        Double grade = 0.0; // Non-anomalous
        Double confidence = 1.03; // Confidence greater than 1
        List<FeatureData> featureData = Collections.emptyList(); // Assuming empty for simplicity
        Instant dataStartTime = Instant.now();
        Instant dataEndTime = dataStartTime.plusMillis(intervalMillis);
        Instant executionStartTime = Instant.now();
        Instant executionEndTime = executionStartTime.plusMillis(500);
        String error = null;
        Optional<Entity> entity = Optional.empty();
        User user = null; // Replace with actual user if needed
        Integer schemaVersion = 1;
        String modelId = "test-model-id";
        double[] relevantAttribution = null;
        Integer relativeIndex = null;
        double[] pastValues = null;
        double[][] expectedValuesList = null;
        double[] likelihoodOfValues = null;
        Double threshold = null;
        double[] currentData = null;
        boolean[] featureImputed = null;

        Condition condition = new Condition(
            "testFeature", // featureName not in features
            ThresholdType.ACTUAL_IS_BELOW_EXPECTED,
            null,
            null
        );
        Rule rule = new Rule(Action.IGNORE_ANOMALY, Arrays.asList(condition));
        List<Rule> rules = Arrays.asList(rule);

        // Invoke the method under test
        AnomalyResult result = AnomalyResult
            .fromRawTRCFResult(
                detectorId,
                intervalMillis,
                taskId,
                rcfScore,
                grade,
                confidence,
                featureData,
                dataStartTime,
                dataEndTime,
                executionStartTime,
                executionEndTime,
                error,
                entity,
                user,
                schemaVersion,
                modelId,
                relevantAttribution,
                relativeIndex,
                pastValues,
                expectedValuesList,
                likelihoodOfValues,
                threshold,
                currentData,
                featureImputed,
                rules
            );

        // Assert that the confidence is capped at 1.0
        assertEquals("Confidence should be capped at 1.0", 1.0, result.getConfidence(), 0.00001);
    }
}
