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

package org.opensearch.timeseries.task;

import java.time.Instant;

/**
 * realtime task cache which will hold these data
 * 1. task state
 * 2. init progress
 * 3. error
 * 4. last job run time
 * 5. analysis interval
 */
public class RealtimeTaskCache {

    // task state
    private String state;

    // init progress
    private Float initProgress;

    // error
    private String error;

    // track last job run time, will clean up cache if no access after 2 intervals
    private long lastJobRunTime;

    // interval in milliseconds.
    private long intervalInMillis;

    public RealtimeTaskCache(String state, Float initProgress, String error, long detectorIntervalInMillis) {
        this.state = state;
        this.initProgress = initProgress;
        this.error = error;
        this.lastJobRunTime = Instant.now().toEpochMilli();
        this.intervalInMillis = detectorIntervalInMillis;
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

    public void setLastJobRunTime(long lastJobRunTime) {
        this.lastJobRunTime = lastJobRunTime;
    }

    public boolean expired() {
        return lastJobRunTime + 2 * intervalInMillis < Instant.now().toEpochMilli();
    }
}
