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
import org.opensearch.ad.constant.ADCommonValue;
import org.opensearch.timeseries.transport.EntityProfileResponse;

public class ADEntityProfileAction extends ActionType<EntityProfileResponse> {
    // Internal Action which is not used for public facing RestAPIs.
    public static final String NAME = ADCommonValue.INTERNAL_ACTION_PREFIX + "detectors/profile/entity";
    public static final ADEntityProfileAction INSTANCE = new ADEntityProfileAction();

    private ADEntityProfileAction() {
        super(NAME, EntityProfileResponse::new);
    }

}
