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

package org.opensearch.ad.ratelimit;

public abstract class QueuedRequest {
    protected long expirationEpochMs;
    protected String detectorId;
    protected RequestPriority priority;

    /**
     *
     * @param expirationEpochMs Request expiry time in milliseconds
     * @param detectorId Detector Id
     * @param priority how urgent the request is
     */
    protected QueuedRequest(long expirationEpochMs, String detectorId, RequestPriority priority) {
        this.expirationEpochMs = expirationEpochMs;
        this.detectorId = detectorId;
        this.priority = priority;
    }

    protected QueuedRequest() {}

    public long getExpirationEpochMs() {
        return expirationEpochMs;
    }

    /**
     * A queue consists of various segments with different priority.  A queued
     * request belongs one segment. The subtype will define the id.
     * @return Segment Id
     */
    public RequestPriority getPriority() {
        return priority;
    }

    public void setPriority(RequestPriority priority) {
        this.priority = priority;
    }

    public String getId() {
        return detectorId;
    }

    public void setDetectorId(String detectorId) {
        this.detectorId = detectorId;
    }
}
