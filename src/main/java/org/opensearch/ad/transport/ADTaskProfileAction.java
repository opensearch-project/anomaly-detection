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
import org.opensearch.ad.constant.ADCommonValue;

public class ADTaskProfileAction extends ActionType<ADTaskProfileResponse> {

    public static final String NAME = ADCommonValue.INTERNAL_ACTION_PREFIX + "detectors/profile/" + AD_TASK;
    public static final ADTaskProfileAction INSTANCE = new ADTaskProfileAction();

    private ADTaskProfileAction() {
        super(NAME, ADTaskProfileResponse::new);
    }

}
