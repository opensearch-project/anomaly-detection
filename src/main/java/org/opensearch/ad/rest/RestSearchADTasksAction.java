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

package org.opensearch.ad.rest;

import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.ad.AnomalyDetectorExtension;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.transport.SearchADTasksAction;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.SDKClient.SDKRestClient;

import com.google.common.collect.ImmutableList;

/**
 * This class consists of the REST handler to search AD tasks.
 */
public class RestSearchADTasksAction extends AbstractSearchAction<ADTask> {

    private static final String LEGACY_URL_PATH = AnomalyDetectorExtension.LEGACY_OPENDISTRO_AD_BASE_URI + "/tasks/_search";
    private static final String URL_PATH = AnomalyDetectorExtension.AD_BASE_DETECTORS_URI + "/tasks/_search";
    private final String SEARCH_ANOMALY_DETECTION_TASKS = "search_anomaly_detection_tasks";

    public RestSearchADTasksAction(ExtensionsRunner extensionsRunner, SDKRestClient client) {
        super(
            ImmutableList.of(),
            ImmutableList.of(Pair.of(URL_PATH, LEGACY_URL_PATH)),
            CommonName.DETECTION_STATE_INDEX,
            ADTask.class,
            SearchADTasksAction.INSTANCE,
            client,
            extensionsRunner
        );
    }

    public String getName() {
        return SEARCH_ANOMALY_DETECTION_TASKS;
    }

}
