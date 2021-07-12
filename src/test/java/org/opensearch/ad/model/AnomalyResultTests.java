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
import java.util.Collection;
import java.util.Locale;

import org.opensearch.ad.AnomalyDetectorPlugin;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.constant.CommonValue;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

public class AnomalyResultTests extends OpenSearchSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class, AnomalyDetectorPlugin.class);
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
        AnomalyResult detectResult = new AnomalyResult(
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            null,
            null,
            null,
            null,
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            null,
            null,
            randomAlphaOfLength(5),
            null,
            TestHelpers.randomUser(),
            CommonValue.NO_SCHEMA_VERSION,
            null
        );
        String detectResultString = TestHelpers
            .xContentBuilderToString(detectResult.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        detectResultString = detectResultString
            .replaceFirst("\\{", String.format(Locale.ROOT, "{\"%s\":\"%s\",", randomAlphaOfLength(5), randomAlphaOfLength(5)));
        AnomalyResult parsedDetectResult = AnomalyResult.parse(TestHelpers.parser(detectResultString));
        assertTrue(parsedDetectResult.getFeatureData().size() == 0);
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
        );
    }

    public void testParseAnomalyDetectorWithNanAnomalyResult() throws IOException {
        AnomalyResult detectResult = new AnomalyResult(
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            Double.NaN,
            Double.NaN,
            Double.NaN,
            ImmutableList.of(),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            null,
            null,
            randomAlphaOfLength(5),
            null,
            null,
            CommonValue.NO_SCHEMA_VERSION,
            null
        );
        String detectResultString = TestHelpers
            .xContentBuilderToString(detectResult.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        detectResultString = detectResultString
            .replaceFirst("\\{", String.format(Locale.ROOT, "{\"%s\":\"%s\",", randomAlphaOfLength(5), randomAlphaOfLength(5)));
        AnomalyResult parsedDetectResult = AnomalyResult.parse(TestHelpers.parser(detectResultString));
        assertNull(parsedDetectResult.getAnomalyGrade());
        assertNull(parsedDetectResult.getAnomalyScore());
        assertNull(parsedDetectResult.getConfidence());
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
}
