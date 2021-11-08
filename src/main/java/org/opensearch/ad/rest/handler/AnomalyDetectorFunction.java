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

package org.opensearch.ad.rest.handler;

@FunctionalInterface
public interface AnomalyDetectorFunction {

    /**
     * Performs this operation.
     *
     * Notes: don't forget to send back responses via channel if you process response with this method.
     */
    void execute();
}
