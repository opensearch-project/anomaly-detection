/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.cluster.diskcleanup;

import java.time.Clock;
import java.time.Duration;

import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.timeseries.cluster.diskcleanup.BaseModelCheckpointIndexRetention;
import org.opensearch.timeseries.cluster.diskcleanup.IndexCleanup;

public class ADCheckpointIndexRetention extends BaseModelCheckpointIndexRetention {

    public ADCheckpointIndexRetention(Duration defaultCheckpointTtl, Clock clock, IndexCleanup indexCleanup) {
        super(defaultCheckpointTtl, clock, indexCleanup, ADCommonName.CHECKPOINT_INDEX_NAME);
    }

}
