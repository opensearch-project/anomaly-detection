/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.model;

import java.util.List;

import org.opensearch.test.OpenSearchTestCase;

/** Unit tests for {@link TaskState}. */
public class TaskStateTests extends OpenSearchTestCase {

    /* -------------------------------------------------------------
       Description mapping
       ------------------------------------------------------------- */
    public void testDescriptionMapping() {
        assertEquals("Created", TaskState.CREATED.getDescription());
        assertEquals("Running", TaskState.RUNNING.getDescription());
        assertEquals("Error", TaskState.ERROR.getDescription());

        // forecast-specific states
        assertEquals("Awaiting data to init", TaskState.AWAITING_DATA_TO_INIT.getDescription());
        assertEquals("forecast failure", TaskState.FORECAST_FAILURE.getDescription());
    }

    /* -------------------------------------------------------------
       NOT_ENDED_STATES content
       ------------------------------------------------------------- */
    public void testNotEndedStatesConstant() {
        List<String> list = TaskState.NOT_ENDED_STATES;

        // expected size and membership
        assertEquals(4, list.size());
        assertTrue(list.contains(TaskState.CREATED.name()));
        assertTrue(list.contains(TaskState.INIT.name()));
        assertTrue(list.contains(TaskState.RUNNING.name()));
        assertTrue(list.contains(TaskState.INIT_TEST.name()));

        // an obviously ended state must be absent
        assertFalse(list.contains(TaskState.FINISHED.name()));
    }

    /* -------------------------------------------------------------
       isAwaitState
       ------------------------------------------------------------- */
    public void testIsAwaitState() {
        assertTrue(TaskState.isAwaitState(TaskState.AWAITING_DATA_TO_INIT.name()));
        assertTrue(TaskState.isAwaitState(TaskState.AWAITING_DATA_TO_RESTART.name()));

        // negative cases
        assertFalse(TaskState.isAwaitState(TaskState.CREATED.name()));
        assertFalse(TaskState.isAwaitState("non-existent"));
    }

    /* -------------------------------------------------------------
       isForecastErrorState
       ------------------------------------------------------------- */
    public void testIsForecastErrorState() {
        assertTrue(TaskState.isForecastErrorState(TaskState.INIT_ERROR.name()));
        assertTrue(TaskState.isForecastErrorState(TaskState.FORECAST_FAILURE.name()));

        // negative cases
        assertFalse(TaskState.isForecastErrorState(TaskState.RUNNING.name()));
        assertFalse(TaskState.isForecastErrorState("someOtherState"));
    }
}
