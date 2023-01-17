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

public class PreviewAnomalyDetectorAction extends ActionType<PreviewAnomalyDetectorResponse> {
    // External Action which used for public facing RestAPIs.
    public static final String NAME = ADCommonValue.EXTERNAL_ACTION_PREFIX + "detector/preview";
    public static final PreviewAnomalyDetectorAction INSTANCE = new PreviewAnomalyDetectorAction();

    private PreviewAnomalyDetectorAction() {
        super(NAME, PreviewAnomalyDetectorResponse::new);
    }
}
