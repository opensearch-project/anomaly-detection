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
import org.opensearch.ad.ADIntegTestCase;
import org.opensearch.ad.AnomalyDetectorPlugin;
import org.opensearch.plugins.Plugin;

public class DeleteITTests extends ADIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(AnomalyDetectorPlugin.class);
    }

    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Collections.singletonList(AnomalyDetectorPlugin.class);
    }

    public void testNormalStopDetector() throws ExecutionException, InterruptedException {
        StopDetectorRequest request = new StopDetectorRequest().adID("123");

        ActionFuture<StopDetectorResponse> future = client().execute(StopDetectorAction.INSTANCE, request);

        StopDetectorResponse response = future.get();
        assertTrue(response.success());
    }

    public void testNormalDeleteModel() throws ExecutionException, InterruptedException {
        DeleteModelRequest request = new DeleteModelRequest("123");

        ActionFuture<DeleteModelResponse> future = client().execute(DeleteModelAction.INSTANCE, request);

        DeleteModelResponse response = future.get();
        assertTrue(!response.hasFailures());
    }

    public void testEmptyIDDeleteModel() throws ExecutionException, InterruptedException {
        DeleteModelRequest request = new DeleteModelRequest("");

        ActionFuture<DeleteModelResponse> future = client().execute(DeleteModelAction.INSTANCE, request);

        expectThrows(ActionRequestValidationException.class, () -> future.actionGet());
    }

    public void testEmptyIDStopDetector() throws ExecutionException, InterruptedException {
        StopDetectorRequest request = new StopDetectorRequest();

        ActionFuture<StopDetectorResponse> future = client().execute(StopDetectorAction.INSTANCE, request);

        expectThrows(ActionRequestValidationException.class, () -> future.actionGet());
    }
}
