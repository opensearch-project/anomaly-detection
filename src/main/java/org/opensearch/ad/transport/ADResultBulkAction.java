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

/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.ad.transport;

import org.opensearch.action.ActionType;
import org.opensearch.ad.constant.CommonValue;
import org.opensearch.common.settings.Settings;
import org.opensearch.transport.TransportRequestOptions;

public class ADResultBulkAction extends ActionType<ADResultBulkResponse> {

    // Internal Action which is not used for public facing RestAPIs.
    public static final String NAME = CommonValue.INTERNAL_ACTION_PREFIX + "write/bulk";
    public static final ADResultBulkAction INSTANCE = new ADResultBulkAction();

    private ADResultBulkAction() {
        super(NAME, ADResultBulkResponse::new);
    }

    @Override
    public TransportRequestOptions transportOptions(Settings settings) {
        return TransportRequestOptions.builder().withType(TransportRequestOptions.Type.BULK).build();
    }
}
