/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.rest;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.action.ActionType;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.ad.constant.ADCommonMessages;
import org.opensearch.ad.settings.ADEnabledSetting;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.timeseries.rest.AbstractSearchAction;

public abstract class AbstractADSearchAction<T extends ToXContentObject> extends AbstractSearchAction<T> {

    public AbstractADSearchAction(
        List<String> urlPaths,
        List<Pair<String, String>> deprecatedPaths,
        String index,
        Class<T> clazz,
        ActionType<SearchResponse> actionType
    ) {
        super(urlPaths, deprecatedPaths, index, clazz, actionType, ADEnabledSetting::isADEnabled, ADCommonMessages.DISABLED_ERR_MSG);
    }
}
