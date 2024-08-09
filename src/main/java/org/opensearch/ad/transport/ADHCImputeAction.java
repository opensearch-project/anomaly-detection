/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.transport;

import org.opensearch.action.ActionType;
import org.opensearch.ad.constant.ADCommonValue;

public class ADHCImputeAction extends ActionType<ADHCImputeNodesResponse> {
    // Internal Action which is not used for public facing RestAPIs.
    public static final String NAME = ADCommonValue.INTERNAL_ACTION_PREFIX + "impute/hc";
    public static final ADHCImputeAction INSTANCE = new ADHCImputeAction();

    private ADHCImputeAction() {
        super(NAME, ADHCImputeNodesResponse::new);
    }
}
