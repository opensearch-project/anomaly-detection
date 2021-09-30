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

package org.opensearch.ad;

/**
 * Represent a state organized via detectorId.  When deleting a detector's state,
 * we can remove it from the state.
 *
 *
 */
public interface CleanState {
    /**
     * Remove state associated with a detector Id
     * @param detectorId Detector Id
     */
    void clear(String detectorId);
}
