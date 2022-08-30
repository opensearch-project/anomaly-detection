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
import java.time.temporal.ChronoUnit;
import java.util.Collection;

import org.junit.Ignore;
import org.opensearch.ad.AnomalyDetectorPlugin;
import org.opensearch.ad.TestHelpers;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

@Ignore
public class ADTaskTests extends OpenSearchSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class, AnomalyDetectorPlugin.class);
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return getInstanceFromNode(NamedWriteableRegistry.class);
    }

    public void testAdTaskSerialization() throws IOException {
        ADTask adTask = TestHelpers.randomAdTask(randomAlphaOfLength(5), ADTaskState.STOPPED, Instant.now(), randomAlphaOfLength(5), true);
        BytesStreamOutput output = new BytesStreamOutput();
        adTask.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        ADTask parsedADTask = new ADTask(input);
        assertEquals("AD task serialization doesn't work", adTask, parsedADTask);
    }

    public void testAdTaskSerializationWithNullDetector() throws IOException {
        ADTask adTask = TestHelpers.randomAdTask(randomAlphaOfLength(5), ADTaskState.STOPPED, Instant.now(), randomAlphaOfLength(5), false);
        BytesStreamOutput output = new BytesStreamOutput();
        adTask.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        ADTask parsedADTask = new ADTask(input);
        assertEquals("AD task serialization doesn't work", adTask, parsedADTask);
    }

    public void testParseADTask() throws IOException {
        ADTask adTask = TestHelpers
            .randomAdTask(null, ADTaskState.STOPPED, Instant.now().truncatedTo(ChronoUnit.SECONDS), randomAlphaOfLength(5), true);
        String taskId = randomAlphaOfLength(5);
        adTask.setTaskId(taskId);
        String adTaskString = TestHelpers.xContentBuilderToString(adTask.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        ADTask parsedADTask = ADTask.parse(TestHelpers.parser(adTaskString), adTask.getTaskId());
        assertEquals("Parsing AD task doesn't work", adTask, parsedADTask);
    }

    public void testParseADTaskWithoutTaskId() throws IOException {
        String taskId = null;
        ADTask adTask = TestHelpers
            .randomAdTask(taskId, ADTaskState.STOPPED, Instant.now().truncatedTo(ChronoUnit.SECONDS), randomAlphaOfLength(5), true);
        String adTaskString = TestHelpers.xContentBuilderToString(adTask.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        ADTask parsedADTask = ADTask.parse(TestHelpers.parser(adTaskString));
        assertEquals("Parsing AD task doesn't work", adTask, parsedADTask);
    }

    public void testParseADTaskWithNullDetector() throws IOException {
        String taskId = randomAlphaOfLength(5);
        ADTask adTask = TestHelpers
            .randomAdTask(taskId, ADTaskState.STOPPED, Instant.now().truncatedTo(ChronoUnit.SECONDS), randomAlphaOfLength(5), false);
        String adTaskString = TestHelpers.xContentBuilderToString(adTask.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        ADTask parsedADTask = ADTask.parse(TestHelpers.parser(adTaskString), taskId);
        assertEquals("Parsing AD task doesn't work", adTask, parsedADTask);
    }

    public void testParseNullableFields() throws IOException {
        ADTask adTask = ADTask.builder().build();
        String adTaskString = TestHelpers.xContentBuilderToString(adTask.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        ADTask parsedADTask = ADTask.parse(TestHelpers.parser(adTaskString));
        assertEquals("Parsing AD task doesn't work", adTask, parsedADTask);
    }

}
