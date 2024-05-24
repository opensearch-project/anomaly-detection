/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries;

import org.opensearch.core.action.ActionListener;
import org.opensearch.timeseries.model.TimeSeriesTask;

/**
 * break the cross dependency between ProfileRunner and TaskManager. Instead, both of them depend on TaskProfileRunner.
 */
public interface TaskProfileRunner<TaskClass extends TimeSeriesTask, TaskProfileType extends TaskProfile<TaskClass>> {
    void getTaskProfile(TaskClass configLevelTask, ActionListener<TaskProfileType> listener);
}
