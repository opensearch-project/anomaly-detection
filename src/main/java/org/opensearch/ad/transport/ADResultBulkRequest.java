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

package org.opensearch.ad.transport;

import java.io.IOException;

import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.ratelimit.ADResultWriteRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.timeseries.transport.ResultBulkRequest;

public class ADResultBulkRequest extends ResultBulkRequest<AnomalyResult, ADResultWriteRequest> {

    public ADResultBulkRequest() {
        super();
    }

    public ADResultBulkRequest(StreamInput in) throws IOException {
        super(in, ADResultWriteRequest::new);
    }
}
