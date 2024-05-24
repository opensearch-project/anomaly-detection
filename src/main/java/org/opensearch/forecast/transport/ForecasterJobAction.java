/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.transport;

import org.opensearch.action.ActionType;
import org.opensearch.forecast.constant.ForecastCommonValue;
import org.opensearch.timeseries.transport.JobResponse;

public class ForecasterJobAction extends ActionType<JobResponse> {
    // External Action which used for public facing RestAPIs.
    public static final String NAME = ForecastCommonValue.EXTERNAL_ACTION_PREFIX + "forecaster/jobmanagement";
    public static final ForecasterJobAction INSTANCE = new ForecasterJobAction();

    private ForecasterJobAction() {
        super(NAME, JobResponse::new);
    }
}
