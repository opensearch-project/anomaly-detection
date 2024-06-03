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
import org.opensearch.action.search.SearchResponse;
import org.opensearch.ad.constant.ADCommonValue;

public class SearchAnomalyDetectorAction extends ActionType<SearchResponse> {
    // External Action which used for public facing RestAPIs.
    public static final String NAME = ADCommonValue.EXTERNAL_ACTION_PREFIX + "detector/search";
    public static final SearchAnomalyDetectorAction INSTANCE = new SearchAnomalyDetectorAction();

    private SearchAnomalyDetectorAction() {
        super(NAME, SearchResponse::new);
    }
}
