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

public class IndexAnomalyDetectorAction extends ActionType<IndexAnomalyDetectorResponse> {
    // External Action which used for public facing RestAPIs.
    public static final String NAME = CommonValue.EXTERNAL_ACTION_PREFIX + "detector/write";
    public static final IndexAnomalyDetectorAction INSTANCE = new IndexAnomalyDetectorAction();

    private IndexAnomalyDetectorAction() {
        super(NAME, IndexAnomalyDetectorResponse::new);
    }

}
