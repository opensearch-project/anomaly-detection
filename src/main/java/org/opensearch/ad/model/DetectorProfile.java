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

package org.opensearch.ad.model;

import java.io.IOException;

import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.timeseries.model.ConfigProfile;

public class DetectorProfile extends ConfigProfile<ADTask, ADTaskProfile> {

    public static class Builder extends ConfigProfile.Builder<ADTask, ADTaskProfile> {
        private ADTaskProfile adTaskProfile;

        public Builder() {}

        @Override
        public Builder taskProfile(ADTaskProfile adTaskProfile) {
            this.adTaskProfile = adTaskProfile;
            return this;
        }

        @Override
        public DetectorProfile build() {
            DetectorProfile profile = new DetectorProfile();
            profile.state = state;
            profile.error = error;
            profile.modelProfile = modelProfile;
            profile.modelCount = modelCount;
            profile.coordinatingNode = coordinatingNode;
            profile.totalSizeInBytes = totalSizeInBytes;
            profile.initProgress = initProgress;
            profile.totalEntities = totalEntities;
            profile.activeEntities = activeEntities;
            profile.taskProfile = adTaskProfile;

            return profile;
        }
    }

    public DetectorProfile() {}

    public DetectorProfile(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected ADTaskProfile createTaskProfile(StreamInput in) throws IOException {
        return new ADTaskProfile(in);
    }

    @Override
    protected String getTaskFieldName() {
        return ADCommonName.AD_TASK;
    }
}
