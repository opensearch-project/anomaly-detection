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

import org.opensearch.ad.model.ADTaskState;

/**
 * Cache HC batch task running state on worker node.
 */
public class ADHCBatchTaskRunState {

    private String detectorTaskState;
    // record if HC detector historical analysis cancelled/stopped. Every entity task should
    // recheck this field and stop if it's true.
    private boolean isHistoricalAnalysisCancelled;
    private String cancelReason;
    private String cancelledBy;

    public ADHCBatchTaskRunState() {
        this.detectorTaskState = ADTaskState.INIT.name();
    }

    public String getDetectorTaskState() {
        return detectorTaskState;
    }

    public void setDetectorTaskState(String detectorTaskState) {
        this.detectorTaskState = detectorTaskState;
    }

    public boolean getHistoricalAnalysisCancelled() {
        return isHistoricalAnalysisCancelled;
    }

    public void setHistoricalAnalysisCancelled(boolean historicalAnalysisCancelled) {
        isHistoricalAnalysisCancelled = historicalAnalysisCancelled;
    }

    public String getCancelReason() {
        return cancelReason;
    }

    public void setCancelReason(String cancelReason) {
        this.cancelReason = cancelReason;
    }

    public String getCancelledBy() {
        return cancelledBy;
    }

    public void setCancelledBy(String cancelledBy) {
        this.cancelledBy = cancelledBy;
    }
}
