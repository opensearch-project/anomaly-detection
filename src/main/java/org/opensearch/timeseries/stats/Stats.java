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

package org.opensearch.timeseries.stats;

import java.util.HashMap;
import java.util.Map;

public class Stats {
    private Map<String, TimeSeriesStat<?>> stats;

    /**
     * Constructor
     *
     * @param stats Map of the stats that are to be kept
     */
    public Stats(Map<String, TimeSeriesStat<?>> stats) {
        this.stats = stats;
    }

    /**
     * Get the stats
     *
     * @return all of the stats
     */
    public Map<String, TimeSeriesStat<?>> getStats() {
        return stats;
    }

    /**
     * Get individual stat by stat name
     *
     * @param key Name of stat
     * @return TimeSeriesStat
     * @throws IllegalArgumentException thrown on illegal statName
     */
    public TimeSeriesStat<?> getStat(String key) throws IllegalArgumentException {
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
    public Map<String, TimeSeriesStat<?>> getNodeStats() {
        return getClusterOrNodeStats(false);
    }

    /**
     * Get a map of the stats that are kept at the cluster level
     *
     * @return Map of stats kept at the cluster level
     */
    public Map<String, TimeSeriesStat<?>> getClusterStats() {
        return getClusterOrNodeStats(true);
    }

    private Map<String, TimeSeriesStat<?>> getClusterOrNodeStats(Boolean getClusterStats) {
        Map<String, TimeSeriesStat<?>> statsMap = new HashMap<>();

        for (Map.Entry<String, TimeSeriesStat<?>> entry : stats.entrySet()) {
            if (entry.getValue().isClusterLevel() == getClusterStats) {
                statsMap.put(entry.getKey(), entry.getValue());
            }
        }
        return statsMap;
    }
}
