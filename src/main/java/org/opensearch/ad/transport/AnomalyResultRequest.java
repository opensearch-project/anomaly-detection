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

import static org.opensearch.action.ValidateActions.addValidationError;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Locale;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.ad.constant.ADCommonMessages;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.transport.ResultRequest;

public class AnomalyResultRequest extends ResultRequest {
    public AnomalyResultRequest(StreamInput in) throws IOException {
        super(in);
    }

    public AnomalyResultRequest(String adID, long start, long end) {
        super(adID, ADIndex.CONFIG.getIndexName(), start, end);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (Strings.isEmpty(configId)) {
            validationException = addValidationError(ADCommonMessages.AD_ID_MISSING_MSG, validationException);
        }
        if (start <= 0 || end <= 0 || start > end) {
            validationException = addValidationError(
                String.format(Locale.ROOT, "%s: start %d, end %d", CommonMessages.INVALID_TIMESTAMP_ERR_MSG, start, end),
                validationException
            );
        }
        return validationException;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ADCommonName.ID_JSON_KEY, configId);
        builder.field(CommonName.START_JSON_KEY, start);
        builder.field(CommonName.END_JSON_KEY, end);
        builder.endObject();
        return builder;
    }

    public static AnomalyResultRequest fromActionRequest(final ActionRequest actionRequest) {
        if (actionRequest instanceof AnomalyResultRequest) {
            return (AnomalyResultRequest) actionRequest;
        }

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); OutputStreamStreamOutput osso = new OutputStreamStreamOutput(baos)) {
            actionRequest.writeTo(osso);
            try (StreamInput input = new InputStreamStreamInput(new ByteArrayInputStream(baos.toByteArray()))) {
                return new AnomalyResultRequest(input);
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("failed to parse ActionRequest into AnomalyResultRequest", e);
        }
    }
}
