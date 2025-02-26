/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.transport;

import org.opensearch.action.ActionType;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.ad.constant.ADCommonValue;

public class ADSingleStreamResultAction extends ActionType<AcknowledgedResponse> {
    // Internal Action which is not used for public facing RestAPIs.
    public static final String NAME = ADCommonValue.INTERNAL_ACTION_PREFIX + "singlestream/result";
    public static final ADSingleStreamResultAction INSTANCE = new ADSingleStreamResultAction();

    private ADSingleStreamResultAction() {
        super(NAME, AcknowledgedResponse::new);
    }

}
