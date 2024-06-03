/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.transport;

import org.opensearch.action.ActionType;
import org.opensearch.forecast.constant.ForecastCommonValue;
import org.opensearch.timeseries.transport.SuggestConfigParamResponse;

public class SuggestForecasterParamAction extends ActionType<SuggestConfigParamResponse> {
    public static final String NAME = ForecastCommonValue.EXTERNAL_ACTION_PREFIX + "forecaster/suggest";
    public static final SuggestForecasterParamAction INSTANCE = new SuggestForecasterParamAction();

    private SuggestForecasterParamAction() {
        super(NAME, SuggestConfigParamResponse::new);
    }
}
