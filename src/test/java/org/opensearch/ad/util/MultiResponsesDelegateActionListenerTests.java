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

/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.ad.util;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.opensearch.ad.TestHelpers.randomHCADAnomalyDetectResult;

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
