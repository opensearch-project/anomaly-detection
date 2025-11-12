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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.model.Config;

public class ValidateConfigRequest extends ActionRequest {

    private final AnalysisType context;
    private final Config config;
    private final String validationType;
    private final Integer maxSingleStreamConfigs;
    private final Integer maxHCConfigs;
    private final Integer maxFeatures;
    private final TimeValue requestTimeout;
    // added during refactoring for forecasting. It is fine we add a new field
    // since the request is handled by the same node.
    private final Integer maxCategoricalFields;

    public ValidateConfigRequest(StreamInput in) throws IOException {
        super(in);
        context = in.readEnum(AnalysisType.class);
        if (context.isAD()) {
            config = new AnomalyDetector(in);
        } else if (context.isForecast()) {
            config = new Forecaster(in);
        } else {
            throw new UnsupportedOperationException("This method is not supported");
        }

        validationType = in.readString();
        maxSingleStreamConfigs = in.readOptionalInt();
        maxHCConfigs = in.readOptionalInt();
        maxFeatures = in.readOptionalInt();
        requestTimeout = in.readTimeValue();
        maxCategoricalFields = in.readOptionalInt();
    }

    public ValidateConfigRequest(
        AnalysisType context,
        Config config,
        String validationType,
        Integer maxSingleStreamConfigs,
        Integer maxHCConfigs,
        Integer maxFeatures,
        TimeValue requestTimeout,
        Integer maxCategoricalFields
    ) {
        this.context = context;
        this.config = config;
        this.validationType = validationType;
        this.maxSingleStreamConfigs = maxSingleStreamConfigs;
        this.maxHCConfigs = maxHCConfigs;
        this.maxFeatures = maxFeatures;
        this.requestTimeout = requestTimeout;
        this.maxCategoricalFields = maxCategoricalFields;
    }

    public ValidateConfigRequest(AnalysisType context, Config config, String validationType) {
        this(context, config, validationType, null, null, null, TimeValue.timeValueSeconds(60), null);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeEnum(context);
        config.writeTo(out);
        out.writeString(validationType);
        out.writeOptionalInt(maxSingleStreamConfigs);
        out.writeOptionalInt(maxHCConfigs);
        out.writeOptionalInt(maxFeatures);
        out.writeTimeValue(requestTimeout);
        out.writeOptionalInt(maxCategoricalFields);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public Config getConfig() {
        return config;
    }

    public String getValidationType() {
        return validationType;
    }

    public Integer getMaxSingleEntityAnomalyDetectors() {
        return maxSingleStreamConfigs;
    }

    public Integer getMaxMultiEntityAnomalyDetectors() {
        return maxHCConfigs;
    }

    public Integer getMaxAnomalyFeatures() {
        return maxFeatures;
    }

    public TimeValue getRequestTimeout() {
        return requestTimeout;
    }

    public Integer getMaxCategoricalFields() {
        return maxCategoricalFields;
    }

    public static ValidateConfigRequest fromActionRequest(
        final ActionRequest actionRequest,
        NamedWriteableRegistry namedWriteableRegistry
    ) {
        if (actionRequest instanceof ValidateConfigRequest) {
            return (ValidateConfigRequest) actionRequest;
        }
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); OutputStreamStreamOutput osso = new OutputStreamStreamOutput(baos)) {
            actionRequest.writeTo(osso);
            try (
                StreamInput input = new InputStreamStreamInput(new ByteArrayInputStream(baos.toByteArray()));
                NamedWriteableAwareStreamInput namedInput = new NamedWriteableAwareStreamInput(input, namedWriteableRegistry)
            ) {
                return new ValidateConfigRequest(namedInput);
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("failed to parse ActionRequest into ValidateConfigRequest", e);
        }
    }
}
