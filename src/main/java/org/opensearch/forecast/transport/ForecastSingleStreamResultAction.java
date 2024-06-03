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
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.forecast.constant.ForecastCommonValue;

public class ForecastSingleStreamResultAction extends ActionType<AcknowledgedResponse> {
    // Internal Action which is not used for public facing RestAPIs.
    public static final String NAME = ForecastCommonValue.INTERNAL_ACTION_PREFIX + "singlestream/result";
    public static final ForecastSingleStreamResultAction INSTANCE = new ForecastSingleStreamResultAction();

    private ForecastSingleStreamResultAction() {
        super(NAME, AcknowledgedResponse::new);
    }

}
