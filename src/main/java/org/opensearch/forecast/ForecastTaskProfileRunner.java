/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast;

import org.opensearch.core.action.ActionListener;
import org.opensearch.forecast.model.ForecastTask;
import org.opensearch.forecast.model.ForecastTaskProfile;
import org.opensearch.timeseries.TaskProfileRunner;

public class ForecastTaskProfileRunner implements TaskProfileRunner<ForecastTask, ForecastTaskProfile> {

    @Override
    public void getTaskProfile(ForecastTask configLevelTask, ActionListener<ForecastTaskProfile> listener) {
        // return null in other fields since forecasting have no in-memory task profiles as AD
        listener
            .onResponse(
                new ForecastTaskProfile(
                    configLevelTask,
                    null,
                    null,
                    null,
                    configLevelTask == null ? null : configLevelTask.getTaskId(),
                    null
                )
            );
    }

}
