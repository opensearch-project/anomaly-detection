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

package org.opensearch.timeseries.cluster;

import org.opensearch.Version;

/**
 * This class records time series plugin version of nodes and whether node is eligible data node to run time series analysis.
 */
public class TimeSeriesNodeInfo {
    // time series plugin version
    private Version adVersion;
    // Is node eligible to run AD.
    private boolean isEligibleDataNode;

    public TimeSeriesNodeInfo(Version version, boolean isEligibleDataNode) {
        this.adVersion = version;
        this.isEligibleDataNode = isEligibleDataNode;
    }

    public Version getVersion() {
        return adVersion;
    }

    public boolean isEligibleDataNode() {
        return isEligibleDataNode;
    }

    @Override
    public String toString() {
        return "ADNodeInfo{" + "version=" + adVersion + ", isEligibleDataNode=" + isEligibleDataNode + '}';
    }
}
