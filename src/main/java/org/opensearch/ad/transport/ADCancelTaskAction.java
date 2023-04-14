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

import static org.opensearch.ad.constant.ADCommonName.CANCEL_TASK;

import org.opensearch.action.ActionType;
import org.opensearch.ad.constant.CommonValue;

public class ADCancelTaskAction extends ActionType<ADCancelTaskResponse> {

    public static final String NAME = CommonValue.INTERNAL_ACTION_PREFIX + "detectors/" + CANCEL_TASK;
    public static final ADCancelTaskAction INSTANCE = new ADCancelTaskAction();

    private ADCancelTaskAction() {
        super(NAME, ADCancelTaskResponse::new);
    }

}
