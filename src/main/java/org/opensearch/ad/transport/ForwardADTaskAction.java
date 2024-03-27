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

import static org.opensearch.ad.constant.ADCommonName.AD_TASK;

import org.opensearch.action.ActionType;
import org.opensearch.ad.constant.CommonValue;
import org.opensearch.timeseries.transport.JobResponse;

public class ForwardADTaskAction extends ActionType<JobResponse> {
    // Internal Action which is not used for public facing RestAPIs.
    public static final String NAME = CommonValue.INTERNAL_ACTION_PREFIX + "detector/" + AD_TASK + "/forward";
    public static final ForwardADTaskAction INSTANCE = new ForwardADTaskAction();

    private ForwardADTaskAction() {
        super(NAME, JobResponse::new);
    }
}
