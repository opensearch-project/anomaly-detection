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

public class SearchTopAnomalyResultAction extends ActionType<SearchTopAnomalyResultResponse> {
    // External Action which used for public facing RestAPIs.
    public static final String NAME = CommonValue.EXTERNAL_ACTION_PREFIX + "result/topAnomalies";
    public static final SearchTopAnomalyResultAction INSTANCE = new SearchTopAnomalyResultAction();

    private SearchTopAnomalyResultAction() {
        super(NAME, SearchTopAnomalyResultResponse::new);
    }
}
