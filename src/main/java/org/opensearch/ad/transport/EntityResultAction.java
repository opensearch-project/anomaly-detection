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
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.ad.constant.CommonValue;

public class EntityResultAction extends ActionType<AcknowledgedResponse> {
    // Internal Action which is not used for public facing RestAPIs.
    public static final String NAME = CommonValue.INTERNAL_ACTION_PREFIX + "entity/result";
    public static final EntityResultAction INSTANCE = new EntityResultAction();

    private EntityResultAction() {
        super(NAME, AcknowledgedResponse::new);
    }

}
