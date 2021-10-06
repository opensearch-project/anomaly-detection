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

package org.opensearch.ad.mock.transport;

import org.opensearch.action.ActionType;
import org.opensearch.ad.constant.CommonValue;
import org.opensearch.ad.transport.AnomalyDetectorJobResponse;

public class MockAnomalyDetectorJobAction extends ActionType<AnomalyDetectorJobResponse> {
    // External Action which used for public facing RestAPIs.
    public static final String NAME = CommonValue.EXTERNAL_ACTION_PREFIX + "detector/mockjobmanagement";
    public static final MockAnomalyDetectorJobAction INSTANCE = new MockAnomalyDetectorJobAction();

    private MockAnomalyDetectorJobAction() {
        super(NAME, AnomalyDetectorJobResponse::new);
    }

}
