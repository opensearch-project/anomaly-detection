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

package org.opensearch.ad.stats;

import java.util.HashMap;
import java.util.Map;

/**
 * This class is the main entry-point for access to the stats that the AD plugin keeps track of.
 */
public class ADStats {

    private Map<String, ADStat<?>> stats;

    /**
     * Constructor
     *
     * @param stats Map of the stats that are to be kept
     */
    public ADStats(Map<String, ADStat<?>> stats) {
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
