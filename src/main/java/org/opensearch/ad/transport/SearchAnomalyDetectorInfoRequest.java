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

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

public class SearchAnomalyDetectorInfoRequest extends ActionRequest {

    private String name;
    private String rawPath;

    public SearchAnomalyDetectorInfoRequest(StreamInput in) throws IOException {
        super(in);
        name = in.readOptionalString();
        rawPath = in.readString();
    }

    public SearchAnomalyDetectorInfoRequest(String name, String rawPath) throws IOException {
        super();
        this.name = name;
        this.rawPath = rawPath;
    }

    public String getName() {
        return name;
    }

    public String getRawPath() {
        return rawPath;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(name);
        out.writeString(rawPath);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
