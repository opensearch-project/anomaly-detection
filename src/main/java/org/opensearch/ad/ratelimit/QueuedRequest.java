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
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package org.opensearch.ad.ratelimit;

public abstract class QueuedRequest {
    protected long expirationEpochMs;
    protected String detectorId;
    protected SegmentPriority priority;

    /**
     *
     * @param expirationEpochMs Request expiry time in milliseconds
     * @param detectorId A queue consists of various segments with different priority.
     *  A queued request belongs one segment.
     * @param priority how urgent the request is
     */
    protected QueuedRequest(long expirationEpochMs, String detectorId, SegmentPriority priority) {
        this.expirationEpochMs = expirationEpochMs;
        this.detectorId = detectorId;
        this.priority = priority;
    }

    public long getExpirationEpochMs() {
        return expirationEpochMs;
    }

    /**
     * A queue consists of various segments with different priority.  A queued
     * request belongs one segment. The subtype will define the id.
     * @return Segment Id
     */
    public SegmentPriority getPriority() {
        return priority;
    }

    public void setPriority(SegmentPriority priority) {
        this.priority = priority;
    }

    public String getDetectorId() {
        return detectorId;
    }

    public void setDetectorId(String detectorId) {
        this.detectorId = detectorId;
    }
}
