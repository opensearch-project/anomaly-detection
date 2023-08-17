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

    // detector interval in milliseconds.
    private long detectorIntervalInMillis;

    // we query result index to check if there are any result generated for detector to tell whether it passed initialization of not.
    // To avoid repeated query when there is no data, record whether we have done that or not.
    private boolean queriedResultIndex;

    public RealtimeTaskCache(String state, Float initProgress, String error, long detectorIntervalInMillis) {
        this.state = state;
        this.initProgress = initProgress;
        this.error = error;
        this.lastJobRunTime = Instant.now().toEpochMilli();
        this.detectorIntervalInMillis = detectorIntervalInMillis;
        this.queriedResultIndex = false;
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

    public boolean hasQueriedResultIndex() {
        return queriedResultIndex;
    }

    public void setQueriedResultIndex(boolean queriedResultIndex) {
        this.queriedResultIndex = queriedResultIndex;
    }

    public boolean expired() {
        return lastJobRunTime + 2 * detectorIntervalInMillis < Instant.now().toEpochMilli();
    }
}
