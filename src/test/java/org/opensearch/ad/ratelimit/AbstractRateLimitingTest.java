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

package org.opensearch.ad.ratelimit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;
import java.util.Optional;

import org.opensearch.action.ActionListener;
import org.opensearch.ad.AbstractADTest;
import org.opensearch.ad.NodeStateManager;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.Entity;
import org.opensearch.threadpool.ThreadPool;

public class AbstractRateLimitingTest extends AbstractADTest {
    Clock clock;
    AnomalyDetector detector;
    NodeStateManager nodeStateManager;
    String detectorId;
    String categoryField;
    Entity entity, entity2, entity3;

    @SuppressWarnings("unchecked")
    @Override
    public void setUp() throws Exception {
        super.setUp();

        clock = mock(Clock.class);
        when(clock.instant()).thenReturn(Instant.now());

        threadPool = mock(ThreadPool.class);
        setUpADThreadPool(threadPool);

        categoryField = "a";
        detectorId = "123";
        detector = TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId, Arrays.asList(categoryField));

        nodeStateManager = mock(NodeStateManager.class);
        doAnswer(invocation -> {
            ActionListener<Optional<AnomalyDetector>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(detector));
            return null;
        }).when(nodeStateManager).getAnomalyDetector(any(String.class), any(ActionListener.class));

        entity = Entity.createSingleAttributeEntity(categoryField, "value");
        entity2 = Entity.createSingleAttributeEntity(categoryField, "value2");
        entity3 = Entity.createSingleAttributeEntity(categoryField, "value3");
    }
}
