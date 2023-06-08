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

package org.opensearch.ad.util;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.opensearch.timeseries.TestHelpers.randomHCADAnomalyDetectResult;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.opensearch.action.ActionListener;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.model.DetectorProfile;
import org.opensearch.ad.model.EntityAnomalyResult;
import org.opensearch.test.OpenSearchTestCase;

public class MultiResponsesDelegateActionListenerTests extends OpenSearchTestCase {

    public void testEmptyResponse() throws InterruptedException {
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        ActionListener<DetectorProfile> actualListener = ActionListener.wrap(response -> {
            assertTrue("Should not reach here", false);
            inProgressLatch.countDown();
        }, exception -> {
            String exceptionMsg = exception.getMessage();
            assertTrue(exceptionMsg, exceptionMsg.contains(MultiResponsesDelegateActionListener.NO_RESPONSE));
            inProgressLatch.countDown();
        });

        MultiResponsesDelegateActionListener<DetectorProfile> multiListener = new MultiResponsesDelegateActionListener<DetectorProfile>(
            actualListener,
            2,
            "blah",
            false
        );
        multiListener.onResponse(null);
        multiListener.onResponse(null);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    @SuppressWarnings("unchecked")
    public void testForceResponse() {
        AnomalyResult anomalyResult1 = randomHCADAnomalyDetectResult(0.25, 0.25, "error");
        AnomalyResult anomalyResult2 = randomHCADAnomalyDetectResult(0.5, 0.5, "error");

        EntityAnomalyResult entityAnomalyResult1 = new EntityAnomalyResult(new ArrayList<AnomalyResult>() {
            {
                add(anomalyResult1);
            }
        });
        EntityAnomalyResult entityAnomalyResult2 = new EntityAnomalyResult(new ArrayList<AnomalyResult>() {
            {
                add(anomalyResult2);
            }
        });

        ActionListener<EntityAnomalyResult> actualListener = mock(ActionListener.class);
        MultiResponsesDelegateActionListener<EntityAnomalyResult> multiListener =
            new MultiResponsesDelegateActionListener<EntityAnomalyResult>(actualListener, 3, "blah", true);
        multiListener.onResponse(entityAnomalyResult1);
        multiListener.onResponse(entityAnomalyResult2);
        multiListener.onFailure(new RuntimeException());
        entityAnomalyResult1.merge(entityAnomalyResult2);

        verify(actualListener).onResponse(entityAnomalyResult1);
    }
}
