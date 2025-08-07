/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.transport;

import org.opensearch.action.ActionType;
import org.opensearch.forecast.constant.ForecastCommonValue;

public class ForecastRunOnceProfileAction extends ActionType<ForecastRunOnceProfileResponse> {
    // External Action which used for public facing RestAPIs.
    public static final String NAME = ForecastCommonValue.EXTERNAL_ACTION_PREFIX + "forecaster/runOnceProfile";
    public static final ForecastRunOnceProfileAction INSTANCE = new ForecastRunOnceProfileAction();

    private ForecastRunOnceProfileAction() {
        super(NAME, ForecastRunOnceProfileResponse::new);
    }
}
