/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.transport;

import org.opensearch.action.ActionType;
import org.opensearch.ad.constant.ADCommonValue;
import org.opensearch.timeseries.transport.SuggestConfigParamResponse;

public class SuggestAnomalyDetectorParamAction extends ActionType<SuggestConfigParamResponse> {
    public static final String NAME = ADCommonValue.EXTERNAL_ACTION_PREFIX + "detector/suggest";
    public static final SuggestAnomalyDetectorParamAction INSTANCE = new SuggestAnomalyDetectorParamAction();

    private SuggestAnomalyDetectorParamAction() {
        super(NAME, SuggestConfigParamResponse::new);
    }
}
