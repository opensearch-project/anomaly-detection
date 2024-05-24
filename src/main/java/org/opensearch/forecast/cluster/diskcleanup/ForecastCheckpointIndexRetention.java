/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.cluster.diskcleanup;

import java.time.Clock;
import java.time.Duration;

import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.timeseries.cluster.diskcleanup.BaseModelCheckpointIndexRetention;
import org.opensearch.timeseries.cluster.diskcleanup.IndexCleanup;

public class ForecastCheckpointIndexRetention extends BaseModelCheckpointIndexRetention {

    public ForecastCheckpointIndexRetention(Duration defaultCheckpointTtl, Clock clock, IndexCleanup indexCleanup) {
        super(defaultCheckpointTtl, clock, indexCleanup, ForecastIndex.CHECKPOINT.getIndexName());
    }

}
