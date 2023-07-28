/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.model;

import java.io.IOException;
import java.util.Collection;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;

public class ForecastTaskSerializationTests extends OpenSearchSingleNodeTestCase {
    private BytesStreamOutput output;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class, TimeSeriesAnalyticsPlugin.class);
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return getInstanceFromNode(NamedWriteableRegistry.class);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        output = new BytesStreamOutput();
    }

    public void testConstructor_allFieldsPresent() throws IOException {
        // Set up a StreamInput that contains all fields
        ForecastTask originalTask = TestHelpers.ForecastTaskBuilder.newInstance().build();

        originalTask.writeTo(output);
        // required by AggregationBuilder in Feature's constructor for named writeable
        NamedWriteableAwareStreamInput streamInput = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());

        ForecastTask readTask = new ForecastTask(streamInput);

        assertEquals("task123", readTask.getTaskId());
        assertEquals("FORECAST_HISTORICAL_HC_ENTITY", readTask.getTaskType());
        assertTrue(readTask.isEntityTask());
        assertEquals("config123", readTask.getConfigId());
        assertEquals(originalTask.getForecaster(), readTask.getForecaster());
        assertEquals("Running", readTask.getState());
        assertEquals(Float.valueOf(0.5f), readTask.getTaskProgress());
        assertEquals(Float.valueOf(0.1f), readTask.getInitProgress());
        assertEquals(originalTask.getCurrentPiece(), readTask.getCurrentPiece());
        assertEquals(originalTask.getExecutionStartTime(), readTask.getExecutionStartTime());
        assertEquals(originalTask.getExecutionEndTime(), readTask.getExecutionEndTime());
        assertEquals(Boolean.TRUE, readTask.isLatest());
        assertEquals("No errors", readTask.getError());
        assertEquals("checkpoint1", readTask.getCheckpointId());
        assertEquals(originalTask.getLastUpdateTime(), readTask.getLastUpdateTime());
        assertEquals("user1", readTask.getStartedBy());
        assertEquals("user2", readTask.getStoppedBy());
        assertEquals("node1", readTask.getCoordinatingNode());
        assertEquals("node2", readTask.getWorkerNode());
        assertEquals(originalTask.getUser(), readTask.getUser());
        assertEquals(originalTask.getDateRange(), readTask.getDateRange());
        assertEquals(originalTask.getEntity(), readTask.getEntity());
        // since entity attributes are random, we cannot have a fixed model id to verify
        assertTrue(readTask.getEntityModelId().startsWith("config123_entity_"));
        assertEquals("parentTask1", readTask.getParentTaskId());
        assertEquals(Integer.valueOf(10), readTask.getEstimatedMinutesLeft());
    }

    public void testConstructor_missingOptionalFields() throws IOException {
        // Set up a StreamInput that contains all fields
        ForecastTask originalTask = TestHelpers.ForecastTaskBuilder
            .newInstance()
            .setForecaster(null)
            .setUser(null)
            .setDateRange(null)
            .setEntity(null)
            .build();

        originalTask.writeTo(output);
        // required by AggregationBuilder in Feature's constructor for named writeable
        NamedWriteableAwareStreamInput streamInput = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());

        ForecastTask readTask = new ForecastTask(streamInput);

        assertEquals("task123", readTask.getTaskId());
        assertEquals("FORECAST_HISTORICAL_HC_ENTITY", readTask.getTaskType());
        assertTrue(readTask.isEntityTask());
        assertEquals("config123", readTask.getConfigId());
        assertEquals(null, readTask.getForecaster());
        assertEquals("Running", readTask.getState());
        assertEquals(Float.valueOf(0.5f), readTask.getTaskProgress());
        assertEquals(Float.valueOf(0.1f), readTask.getInitProgress());
        assertEquals(originalTask.getCurrentPiece(), readTask.getCurrentPiece());
        assertEquals(originalTask.getExecutionStartTime(), readTask.getExecutionStartTime());
        assertEquals(originalTask.getExecutionEndTime(), readTask.getExecutionEndTime());
        assertEquals(Boolean.TRUE, readTask.isLatest());
        assertEquals("No errors", readTask.getError());
        assertEquals("checkpoint1", readTask.getCheckpointId());
        assertEquals(originalTask.getLastUpdateTime(), readTask.getLastUpdateTime());
        assertEquals("user1", readTask.getStartedBy());
        assertEquals("user2", readTask.getStoppedBy());
        assertEquals("node1", readTask.getCoordinatingNode());
        assertEquals("node2", readTask.getWorkerNode());
        assertEquals(null, readTask.getUser());
        assertEquals(null, readTask.getDateRange());
        assertEquals(null, readTask.getEntity());
        assertEquals(null, readTask.getEntityModelId());
        assertEquals("parentTask1", readTask.getParentTaskId());
        assertEquals(Integer.valueOf(10), readTask.getEstimatedMinutesLeft());
    }

}
