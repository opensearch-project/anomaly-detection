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

import java.io.IOException;

import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.timeseries.ratelimit.RequestPriority;
import org.opensearch.timeseries.ratelimit.ResultWriteRequest;

public class ADResultWriteRequest extends ResultWriteRequest<AnomalyResult> {

    public ADResultWriteRequest(
        long expirationEpochMs,
        String detectorId,
        RequestPriority priority,
        AnomalyResult result,
        String resultIndex,
        String flattenResultIndex
    ) {
        super(expirationEpochMs, detectorId, priority, result, resultIndex, flattenResultIndex);
    }

    public ADResultWriteRequest(StreamInput in) throws IOException {
        super(in, AnomalyResult::new);
    }
}
