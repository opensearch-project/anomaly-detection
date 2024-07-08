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
import java.time.Instant;
import java.util.List;

import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.timeseries.model.FeatureData;
import org.opensearch.timeseries.model.IndexableResult;

public abstract class ResultResponse<IndexableResultType extends IndexableResult> extends ActionResponse implements ToXContentObject {

    protected String error;
    protected List<FeatureData> features;
    protected Long rcfTotalUpdates;
    protected Long configIntervalInMinutes;
    protected Boolean isHC;
    protected String taskId;

    public ResultResponse(
        List<FeatureData> features,
        String error,
        Long rcfTotalUpdates,
        Long configInterval,
        Boolean isHC,
        String taskId
    ) {
        this.error = error;
        this.features = features;
        this.rcfTotalUpdates = rcfTotalUpdates;
        this.configIntervalInMinutes = configInterval;
        this.isHC = isHC;
        this.taskId = taskId;
    }

    /**
     * Leave it as implementation detail in subclass as how to deserialize TimeSeriesResultResponse
     * @param in deserialization stream
     * @throws IOException when deserialization errs
     */
    public ResultResponse(StreamInput in) throws IOException {
        super(in);
    }

    public String getError() {
        return error;
    }

    public List<FeatureData> getFeatures() {
        return features;
    }

    public Long getRcfTotalUpdates() {
        return rcfTotalUpdates;
    }

    public Long getConfigIntervalInMinutes() {
        return configIntervalInMinutes;
    }

    public Boolean isHC() {
        return isHC;
    }

    public String getTaskId() {
        return taskId;
    }

    /**
     *
     * @return whether we should save the response to result index
     */
    public boolean shouldSave() {
        return error != null;
    }

    public abstract List<IndexableResultType> toIndexableResults(
        String configId,
        Instant dataStartInstant,
        Instant dataEndInstant,
        Instant executionStartInstant,
        Instant executionEndInstant,
        Integer schemaVersion,
        User user,
        String error
    );
}
