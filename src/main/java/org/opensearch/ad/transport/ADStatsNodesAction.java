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
import org.opensearch.timeseries.transport.StatsNodesResponse;

/**
 * ADStatsNodesAction class
 */
public class ADStatsNodesAction extends ActionType<StatsNodesResponse> {

    // Internal Action which is not used for public facing RestAPIs.
    public static final String NAME = ADCommonValue.INTERNAL_ACTION_PREFIX + "stats/nodes";
    public static final ADStatsNodesAction INSTANCE = new ADStatsNodesAction();

    /**
     * Constructor
     */
    private ADStatsNodesAction() {
        super(NAME, StatsNodesResponse::new);
    }

}
