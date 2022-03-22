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

import org.opensearch.action.ActionFuture;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.ad.AnomalyDetectorPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

public class ThresholdResultITTests extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(AnomalyDetectorPlugin.class);
    }
    
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Collections.singletonList(AnomalyDetectorPlugin.class);
    }
    
    public void testEmptyID() throws ExecutionException, InterruptedException {
        ThresholdResultRequest request = new ThresholdResultRequest("", "123-threshold", 2.5d);

        ActionFuture<ThresholdResultResponse> future = client().execute(ThresholdResultAction.INSTANCE, request);

        expectThrows(ActionRequestValidationException.class, () -> future.actionGet());
    }

    public void testIDIsNull() throws ExecutionException, InterruptedException {
        ThresholdResultRequest request = new ThresholdResultRequest(null, "123-threshold", 2.5d);

        ActionFuture<ThresholdResultResponse> future = client().execute(ThresholdResultAction.INSTANCE, request);

        expectThrows(ActionRequestValidationException.class, () -> future.actionGet());
    }
}
