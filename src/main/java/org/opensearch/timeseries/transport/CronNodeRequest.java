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

package org.opensearch.timeseries.transport;

import java.io.IOException;

import org.opensearch.action.support.nodes.BaseNodeRequest;
import org.opensearch.core.common.io.stream.StreamInput;

/**
 *  Delete model represents the request to an individual node
 */
public class CronNodeRequest extends BaseNodeRequest {

    public CronNodeRequest() {}

    public CronNodeRequest(StreamInput in) throws IOException {
        super(in);
    }
}
