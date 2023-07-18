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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;
import java.util.Optional;

import org.opensearch.action.ActionListener;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.model.Entity;

public class AbstractRateLimitingTest extends AbstractTimeSeriesTest {
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
            ActionListener<Optional<AnomalyDetector>> listener = invocation.getArgument(2);
            listener.onResponse(Optional.of(detector));
            return null;
        }).when(nodeStateManager).getConfig(any(String.class), eq(AnalysisType.AD), any(ActionListener.class));

        entity = Entity.createSingleAttributeEntity(categoryField, "value");
        entity2 = Entity.createSingleAttributeEntity(categoryField, "value2");
        entity3 = Entity.createSingleAttributeEntity(categoryField, "value3");
    }
}
