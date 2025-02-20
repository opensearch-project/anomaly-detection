/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.transport;

import org.opensearch.action.ActionType;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.forecast.constant.ForecastCommonValue;

public class ForecastImputeMissingValueAction extends ActionType<AcknowledgedResponse> {
    // Internal Action which is not used for public facing RestAPIs.
    public static final String NAME = ForecastCommonValue.INTERNAL_ACTION_PREFIX + "impute";
    public static final ForecastImputeMissingValueAction INSTANCE = new ForecastImputeMissingValueAction();

    public ForecastImputeMissingValueAction() {
        super(NAME, AcknowledgedResponse::new);
    }
}
