/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.forecast.transport;

import org.opensearch.action.ActionType;
import org.opensearch.forecast.constant.ForecastCommonValue;
import org.opensearch.timeseries.transport.StopConfigResponse;

public class StopForecasterAction extends ActionType<StopConfigResponse> {
    // Internal Action which is not used for public facing RestAPIs.
    public static final String NAME = ForecastCommonValue.INTERNAL_ACTION_PREFIX + "forecaster/stop";
    public static final StopForecasterAction INSTANCE = new StopForecasterAction();

    private StopForecasterAction() {
        super(NAME, StopConfigResponse::new);
    }

}
