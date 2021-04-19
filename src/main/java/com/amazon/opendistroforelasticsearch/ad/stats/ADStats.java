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

package com.amazon.opendistroforelasticsearch.ad.stats;

import java.util.HashMap;
import java.util.Map;

import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager;
import com.amazon.opendistroforelasticsearch.ad.util.IndexUtils;

/**
 * This class is the main entry-point for access to the stats that the AD plugin keeps track of.
 */
public class ADStats {

    private IndexUtils indexUtils;
    private ModelManager modelManager;
    private Map<String, ADStat<?>> stats;

    /**
     * Constructor
     *
     * @param indexUtils utility to get information about indices
     * @param modelManager used to get information about which models are hosted on a particular node
     * @param stats Map of the stats that are to be kept
     */
    public ADStats(IndexUtils indexUtils, ModelManager modelManager, Map<String, ADStat<?>> stats) {
        this.indexUtils = indexUtils;
        this.modelManager = modelManager;
        this.stats = stats;
    }

    /**
     * Get the stats
     *
     * @return all of the stats
     */
    public Map<String, ADStat<?>> getStats() {
        return stats;
    }

    /**
     * Get individual stat by stat name
     *
     * @param key Name of stat
     * @return ADStat
     * @throws IllegalArgumentException thrown on illegal statName
     */
    public ADStat<?> getStat(String key) throws IllegalArgumentException {
        if (!stats.keySet().contains(key)) {
            throw new IllegalArgumentException("Stat=\"" + key + "\" does not exist");
        }
        return stats.get(key);
    }

    /**
     * Get a map of the stats that are kept at the node level
     *
     * @return Map of stats kept at the node level
     */
    public Map<String, ADStat<?>> getNodeStats() {
        return getClusterOrNodeStats(false);
    }

    /**
     * Get a map of the stats that are kept at the cluster level
     *
     * @return Map of stats kept at the cluster level
     */
    public Map<String, ADStat<?>> getClusterStats() {
        return getClusterOrNodeStats(true);
    }

    private Map<String, ADStat<?>> getClusterOrNodeStats(Boolean getClusterStats) {
        Map<String, ADStat<?>> statsMap = new HashMap<>();

        for (Map.Entry<String, ADStat<?>> entry : stats.entrySet()) {
            if (entry.getValue().isClusterLevel() == getClusterStats) {
                statsMap.put(entry.getKey(), entry.getValue());
            }
        }
        return statsMap;
    }
}
