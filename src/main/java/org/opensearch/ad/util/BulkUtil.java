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

package org.opensearch.ad.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;

public class BulkUtil {
    private static final Logger logger = LogManager.getLogger(BulkUtil.class);

    public static List<DocWriteRequest<?>> getIndexRequestToRetry(BulkRequest bulkRequest, BulkResponse bulkResponse) {
        List<DocWriteRequest<?>> res = new ArrayList<>();

        Set<String> failedId = new HashSet<>();
        for (BulkItemResponse response : bulkResponse.getItems()) {
            if (response.isFailed()) {
                failedId.add(response.getId());
            }
        }

        for (DocWriteRequest<?> request : bulkRequest.requests()) {
            if (failedId.contains(request.id())) {
                res.add(request);
            }
        }
        return res;
    }
}
