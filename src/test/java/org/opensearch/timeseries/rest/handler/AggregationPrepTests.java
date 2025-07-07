/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.rest.handler;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;

import org.junit.Before;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.aggregations.bucket.histogram.LongBounds;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.common.exception.ValidationException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.model.ValidationAspect;
import org.opensearch.timeseries.model.ValidationIssueType;

public class AggregationPrepTests extends OpenSearchTestCase {

    private AggregationPrep aggregationPrep;
    private Config mockConfig;
    private SearchFeatureDao mockSearchFeatureDao;
    private TimeValue mockRequestTimeout;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        mockConfig = mock(Config.class);
        mockSearchFeatureDao = mock(SearchFeatureDao.class);
        mockRequestTimeout = mock(TimeValue.class);

        aggregationPrep = new AggregationPrep(mockSearchFeatureDao, mockRequestTimeout, mockConfig);
    }

    public void testCreateSearchRequestForFeatureWithEmptyFeatures() {
        // Set up the config to have no enabled features
        when(mockConfig.getEnabledFeatureIds()).thenReturn(Collections.emptyList());

        // Define test inputs
        IntervalTimeConfiguration interval = new IntervalTimeConfiguration(1, ChronoUnit.MINUTES);
        LongBounds bounds = new LongBounds(0L, 60000L);
        HashMap<String, Object> topEntity = new HashMap<>();
        int featureIndex = 0;

        // Execute and assert
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> aggregationPrep.createSearchRequestForFeature(interval, bounds, topEntity, featureIndex)
        );

        assertEquals("empty feature", exception.getMessage());
    }

    public void testComposeHistogramQueryThrowsValidationExceptionForSparseCategory() {
        // Set up config for high cardinality
        when(mockConfig.isHighCardinality()).thenReturn(true);
        when(mockConfig.getTimeField()).thenReturn("timestamp");

        // Define test inputs
        HashMap<String, Object> emptyTopEntity = new HashMap<>();
        int intervalInMinutes = 1;
        LongBounds bounds = new LongBounds(0L, 60000L);
        int minDocCount = 0;

        // Execute and assert
        ValidationException exception = expectThrows(
            ValidationException.class,
            () -> aggregationPrep.composeHistogramQuery(emptyTopEntity, intervalInMinutes, bounds, minDocCount)
        );

        assertEquals(CommonMessages.CATEGORY_FIELD_TOO_SPARSE, exception.getMessage());
        assertEquals(ValidationIssueType.CATEGORY, exception.getType());
        assertEquals(ValidationAspect.MODEL, exception.getAspect());
    }
}
