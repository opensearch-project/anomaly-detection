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
import org.opensearch.common.settings.Settings;
import org.opensearch.forecast.constant.ForecastCommonValue;
import org.opensearch.timeseries.transport.ResultBulkResponse;
import org.opensearch.transport.TransportRequestOptions;

public class ForecastResultBulkAction extends ActionType<ResultBulkResponse> {

    // Internal Action which is not used for public facing RestAPIs.
    public static final String NAME = ForecastCommonValue.INTERNAL_ACTION_PREFIX + "write/bulk";
    public static final ForecastResultBulkAction INSTANCE = new ForecastResultBulkAction();

    private ForecastResultBulkAction() {
        super(NAME, ResultBulkResponse::new);
    }

    @Override
    public TransportRequestOptions transportOptions(Settings settings) {
        return TransportRequestOptions.builder().withType(TransportRequestOptions.Type.BULK).build();
    }
}
