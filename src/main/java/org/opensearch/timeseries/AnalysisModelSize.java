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

package org.opensearch.timeseries;

import java.util.Map;

public interface AnalysisModelSize {
    /**
     * Gets all of a detector's model sizes hosted on a node
     *
     * @param id Analysis Id
     * @return a map of model id to its memory size
     */
    Map<String, Long> getModelSize(String id);
}
