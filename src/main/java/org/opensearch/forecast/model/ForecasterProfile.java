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

package org.opensearch.forecast.model;

import java.io.IOException;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.timeseries.model.ConfigProfile;

public class ForecasterProfile extends ConfigProfile<ForecastTask, ForecastTaskProfile> {

    public static class Builder extends ConfigProfile.Builder<ForecastTask, ForecastTaskProfile> {
        private ForecastTaskProfile forecastTaskProfile;

        public Builder() {}

        @Override
        public Builder taskProfile(ForecastTaskProfile forecastTaskProfile) {
            this.forecastTaskProfile = forecastTaskProfile;
            return this;
        }

        @Override
        public ForecasterProfile build() {
            ForecasterProfile profile = new ForecasterProfile();
            profile.state = state;
            profile.error = error;
            profile.modelProfile = modelProfile;
            profile.modelCount = modelCount;
            profile.coordinatingNode = coordinatingNode;
            profile.totalSizeInBytes = totalSizeInBytes;
            profile.initProgress = initProgress;
            profile.totalEntities = totalEntities;
            profile.activeEntities = activeEntities;
            profile.taskProfile = forecastTaskProfile;

            return profile;
        }
    }

    public ForecasterProfile() {}

    public ForecasterProfile(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected ForecastTaskProfile createTaskProfile(StreamInput in) throws IOException {
        return new ForecastTaskProfile(in);
    }

    @Override
    protected String getTaskFieldName() {
        return ForecastCommonName.FORECAST_TASK;
    }
}
