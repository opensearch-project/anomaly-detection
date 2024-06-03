/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.transport;

import org.opensearch.action.ActionType;
import org.opensearch.forecast.constant.ForecastCommonValue;
import org.opensearch.timeseries.transport.ValidateConfigResponse;

public class ValidateForecasterAction extends ActionType<ValidateConfigResponse> {
    public static final String NAME = ForecastCommonValue.EXTERNAL_ACTION_PREFIX + "forecaster/validate";
    public static final ValidateForecasterAction INSTANCE = new ValidateForecasterAction();

    private ValidateForecasterAction() {
        super(NAME, ValidateConfigResponse::new);
    }
}
