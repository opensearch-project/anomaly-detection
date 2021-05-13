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

package org.opensearch.ad.stats.suppliers;

import java.util.function.Supplier;

import org.opensearch.ad.util.IndexUtils;

/**
 * IndexStatusSupplier provides the status of an index as the value
 */
public class IndexStatusSupplier implements Supplier<String> {
    private IndexUtils indexUtils;
    private String indexName;

    public static final String UNABLE_TO_RETRIEVE_HEALTH_MESSAGE = "unable to retrieve health";

    /**
     * Constructor
     *
     * @param indexUtils Utility for getting information about indices
     * @param indexName Name of index to extract stats from
     */
    public IndexStatusSupplier(IndexUtils indexUtils, String indexName) {
        this.indexUtils = indexUtils;
        this.indexName = indexName;
    }

    @Override
    public String get() {
        try {
            return indexUtils.getIndexHealthStatus(indexName);
        } catch (IllegalArgumentException e) {
            return UNABLE_TO_RETRIEVE_HEALTH_MESSAGE;
        }

    }
}
