/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.transport;

import org.opensearch.action.ActionType;
import org.opensearch.forecast.constant.ForecastCommonValue;

public class ForecastRunOnceAction extends ActionType<ForecastResultResponse> {
    // External Action which used for public facing RestAPIs.
    public static final String NAME = ForecastCommonValue.EXTERNAL_ACTION_PREFIX + "forecaster/runOnce";
    public static final ForecastRunOnceAction INSTANCE = new ForecastRunOnceAction();

    private ForecastRunOnceAction() {
        super(NAME, ForecastResultResponse::new);
    }
}
