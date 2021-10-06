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

import static org.opensearch.ad.constant.CommonName.AD_TASK_REMOTE;

import org.opensearch.action.ActionType;
import org.opensearch.ad.constant.CommonValue;

public class ADBatchTaskRemoteExecutionAction extends ActionType<ADBatchAnomalyResultResponse> {
    public static final String NAME = CommonValue.INTERNAL_ACTION_PREFIX + "detector/" + AD_TASK_REMOTE;
    public static final ADBatchTaskRemoteExecutionAction INSTANCE = new ADBatchTaskRemoteExecutionAction();

    private ADBatchTaskRemoteExecutionAction() {
        super(NAME, ADBatchAnomalyResultResponse::new);
    }

}
