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

package com.amazon.opendistroforelasticsearch.ad.transport;

import org.opensearch.action.ActionType;

import com.amazon.opendistroforelasticsearch.ad.constant.CommonValue;

public class ValidateAnomalyDetectorAction extends ActionType<ValidateAnomalyDetectorResponse> {

    public static final String NAME = CommonValue.EXTERNAL_ACTION_PREFIX + "detector/validate";
    public static final ValidateAnomalyDetectorAction INSTANCE = new ValidateAnomalyDetectorAction();

    private ValidateAnomalyDetectorAction() {
        super(NAME, ValidateAnomalyDetectorResponse::new);
    }
}
