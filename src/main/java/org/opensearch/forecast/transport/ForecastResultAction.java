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

public class ForecastResultAction extends ActionType<ForecastResultResponse> {
    // External Action which used for public facing RestAPIs or actions we need to assume cx's role.
    public static final String NAME = ForecastCommonValue.EXTERNAL_ACTION_PREFIX + "forecaster/run";
    public static final ForecastResultAction INSTANCE = new ForecastResultAction();

    private ForecastResultAction() {
        super(NAME, ForecastResultResponse::new);
    }
}
