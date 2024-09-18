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

package org.opensearch.ad.transport;

import static org.opensearch.ad.model.ADTaskAction.CLEAN_CACHE;
import static org.opensearch.ad.model.ADTaskAction.CLEAN_STALE_RUNNING_ENTITIES;
import static org.opensearch.ad.model.ADTaskAction.START;
import static org.opensearch.timeseries.TestHelpers.randomIntervalTimeConfiguration;
import static org.opensearch.timeseries.TestHelpers.randomQuery;
import static org.opensearch.timeseries.TestHelpers.randomUser;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.Locale;

import org.opensearch.Version;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.ad.mock.transport.MockADTaskAction_1_0;
import org.opensearch.ad.mock.transport.MockForwardADTaskRequest_1_0;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.common.exception.VersionException;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

import com.google.common.collect.ImmutableList;

public class ForwardADTaskRequestTests extends OpenSearchSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class, TimeSeriesAnalyticsPlugin.class);
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return getInstanceFromNode(NamedWriteableRegistry.class);
    }

    public void testNullVersion() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableList.of());
        expectThrows(VersionException.class, () -> new ForwardADTaskRequest(detector, null, null, null, null, null));
    }

    public void testNullDetectorIdAndTaskAction() throws IOException {
        AnomalyDetector detector = new AnomalyDetector(
            null,
            randomLong(),
            randomAlphaOfLength(20),
            randomAlphaOfLength(30),
            randomAlphaOfLength(5),
            ImmutableList.of(randomAlphaOfLength(10).toLowerCase(Locale.ROOT)),
            ImmutableList.of(),
            randomQuery(),
            randomIntervalTimeConfiguration(),
            randomIntervalTimeConfiguration(),
            randomIntBetween(1, TimeSeriesSettings.MAX_SHINGLE_SIZE),
            null,
            randomInt(),
            Instant.now(),
            null,
            randomUser(),
            null,
            TestHelpers.randomImputationOption(null),
            randomIntBetween(1, 10000),
            randomInt(TimeSeriesSettings.MAX_SHINGLE_SIZE / 2),
            randomIntBetween(1, 1000),
            null,
            null,
            null,
            null,
            null,
            Instant.now()
        );
        ForwardADTaskRequest request = new ForwardADTaskRequest(detector, null, null, null, null, Version.V_2_1_0);
        ActionRequestValidationException validate = request.validate();
        assertEquals("Validation Failed: 1: AD ID is missing;2: AD task action is missing;", validate.getMessage());
    }

    public void testEmptyStaleEntities() throws IOException {
        ADTask adTask = TestHelpers.randomAdTask();
        ForwardADTaskRequest request = new ForwardADTaskRequest(adTask, CLEAN_STALE_RUNNING_ENTITIES, null);
        ActionRequestValidationException validate = request.validate();
        assertEquals("Validation Failed: 1: Empty stale running entities;", validate.getMessage());
    }

    public void testSerializeRequest() throws IOException {
        ADTask adTask = TestHelpers.randomAdTask();
        ForwardADTaskRequest request = new ForwardADTaskRequest(adTask, CLEAN_STALE_RUNNING_ENTITIES, null);
        BytesStreamOutput output = new BytesStreamOutput();
        request.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        ForwardADTaskRequest parsedInput = new ForwardADTaskRequest(input);
        assertEquals(request, parsedInput);
    }

    public void testParseRequestFromOldNodeWithNewCode() throws IOException {
        ADTask adTask = TestHelpers.randomAdTask();
        MockForwardADTaskRequest_1_0 oldRequest = new MockForwardADTaskRequest_1_0(
            adTask.getDetector(),
            adTask.getUser(),
            MockADTaskAction_1_0.START
        );
        BytesStreamOutput output = new BytesStreamOutput();
        oldRequest.writeTo(output);

        // Parse old forward AD task request of 1.0, will reject it directly,
        // so if old node is coordinating node, it can't use new node as worker node to run task.
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        expectThrows(VersionException.class, () -> new ForwardADTaskRequest(input));
    }

    public void testParseRequestFromNewNodeWithOldCode_StartAction() throws IOException {
        ADTask adTask = TestHelpers.randomAdTask();
        ForwardADTaskRequest request = new ForwardADTaskRequest(adTask, START, null);
        BytesStreamOutput output = new BytesStreamOutput();
        request.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        MockForwardADTaskRequest_1_0 parsedInput = new MockForwardADTaskRequest_1_0(input);
        // START action should be parsed as START action on old node
        // If coordinating node is new node, it will just use new node as worker node to run task.
        // So it's impossible that new node will send START action to old node. Add this test case
        // just to show the request parsing logic works.
        assertEquals(MockADTaskAction_1_0.START, parsedInput.getAdTaskAction());
        assertEquals(request.getDetector(), parsedInput.getDetector());
    }

    public void testParseRequestFromNewNodeWithOldCode_CleanCacheAction() throws IOException {
        ADTask adTask = TestHelpers.randomAdTask();
        ForwardADTaskRequest request = new ForwardADTaskRequest(adTask, CLEAN_CACHE, null);
        BytesStreamOutput output = new BytesStreamOutput();
        request.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        MockForwardADTaskRequest_1_0 parsedInput = new MockForwardADTaskRequest_1_0(input);
        // CLEAN_CACHE action should be parsed as STOP action on old node
        // In old version on or before AD1.0, worker node will send STOP action to clean cache
        // on coordinating node when task done on worker node.
        // In mixed cluster, new node will reject START action if it's from old node.
        // So no new node will run as worker node for old coordinating node.
        // Add this test case just to show the request task action parsing logic works.
        assertEquals(MockADTaskAction_1_0.STOP, parsedInput.getAdTaskAction());
        assertEquals(request.getDetector(), parsedInput.getDetector());
    }
}
