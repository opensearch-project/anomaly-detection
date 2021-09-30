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

package org.opensearch.ad.transport;

import org.opensearch.action.ActionType;
import org.opensearch.ad.constant.CommonValue;

public class RCFResultAction extends ActionType<RCFResultResponse> {
    // Internal Action which is not used for public facing RestAPIs.
    public static final String NAME = CommonValue.INTERNAL_ACTION_PREFIX + "rcf/result";
    public static final RCFResultAction INSTANCE = new RCFResultAction();

    private RCFResultAction() {
        super(NAME, RCFResultResponse::new);
    }

}
