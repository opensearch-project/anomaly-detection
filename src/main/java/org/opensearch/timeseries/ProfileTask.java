/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries;

import org.opensearch.core.action.ActionListener;
import org.opensearch.timeseries.model.TimeSeriesTask;

/**
 * Break the cross dependency between TaskManager and ProfileRunner. Instead of
 * depending on each other, they depend on the interface.
 *
 */
public interface ProfileTask<TaskClass extends TimeSeriesTask, TaskProfileType extends TaskProfile<TaskClass>> {
    void getTaskProfile(TaskClass configLevelTask, ActionListener<TaskProfileType> listener);
}
