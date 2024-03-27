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

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;

public class ADStatsITTests extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(TimeSeriesAnalyticsPlugin.class);
    }

    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Collections.singletonList(TimeSeriesAnalyticsPlugin.class);
    }

    public void testNormalADStats() throws ExecutionException, InterruptedException {
        ADStatsRequest adStatsRequest = new ADStatsRequest(new String[0]);

        ADStatsNodesResponse response = client().execute(ADStatsNodesAction.INSTANCE, adStatsRequest).get();
        assertTrue("getting stats failed", !response.hasFailures());
    }
}
