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

/**
 * AD realtime task cache which will hold these data
 * 1. task state
 * 2. init progress
 * 3. error
 */
public class ADRealtimeTaskCache {

    // task state
    private String state;

    // init progress
    private Float initProgress;

    // error
    private String error;

    public ADRealtimeTaskCache(String state, Float initProgress, String error) {
        this.state = state;
        this.initProgress = initProgress;
        this.error = error;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public Float getInitProgress() {
        return initProgress;
    }

    public void setInitProgress(Float initProgress) {
        this.initProgress = initProgress;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }
}
