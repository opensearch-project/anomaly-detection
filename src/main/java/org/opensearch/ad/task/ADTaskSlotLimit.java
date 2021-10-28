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

package org.opensearch.ad.task;

public class ADTaskSlotLimit {
    // Task slots assigned to detector
    private Integer detectorTaskSlots;
    // How many task lanes this detector can start at most
    private Integer detectorTaskLaneLimit;

    public ADTaskSlotLimit(Integer detectorTaskSlots, Integer detectorTaskLaneLimit) {
        this.detectorTaskSlots = detectorTaskSlots;
        this.detectorTaskLaneLimit = detectorTaskLaneLimit;
    }

    public Integer getDetectorTaskSlots() {
        return detectorTaskSlots;
    }

    public void setDetectorTaskSlots(Integer detectorTaskSlots) {
        this.detectorTaskSlots = detectorTaskSlots;
    }

    public void setDetectorTaskLaneLimit(Integer detectorTaskLaneLimit) {
        this.detectorTaskLaneLimit = detectorTaskLaneLimit;
    }
}
