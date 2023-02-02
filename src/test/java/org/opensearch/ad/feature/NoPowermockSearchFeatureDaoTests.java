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

package org.opensearch.ad.feature;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.ActionListener;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.ad.AbstractADTest;
import org.opensearch.ad.NodeStateManager;
import org.opensearch.ad.dataprocessor.LinearUniformInterpolator;
import org.opensearch.ad.dataprocessor.SingleFeatureLinearUniformInterpolator;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.Entity;
import org.opensearch.ad.model.IntervalTimeConfiguration;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.util.SecurityClientUtil;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.InternalOrder;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;

import com.google.common.collect.ImmutableList;

/**
 * SearchFeatureDaoTests uses Powermock and has strange log4j related errors
 * (e.g., TEST_INSTANCES_ARE_REUSED).
 * Create a new class for new tests related to SearchFeatureDao.
 *
 */
public class NoPowermockSearchFeatureDaoTests extends AbstractADTest {
    private final Logger LOG = LogManager.getLogger(NoPowermockSearchFeatureDaoTests.class);

    private AnomalyDetector detector;
    private Client client;
    private SearchFeatureDao searchFeatureDao;
    private LinearUniformInterpolator interpolator;
    private SecurityClientUtil clientUtil;
    private Settings settings;
    private ClusterService clusterService;
    private Clock clock;
    private String serviceField, hostField;
    private String detectorId;
    private Map<String, Object> attrs1, attrs2;

    @BeforeClass
    public static void setUpBeforeClass() {
        setUpThreadPool(NoPowermockSearchFeatureDaoTests.class.getSimpleName());
    }

    @AfterClass
    public static void tearDownAfterClass() {
        tearDownThreadPool();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        super.setUp();
        serviceField = "service";
        hostField = "host";

        detector = mock(AnomalyDetector.class);
        when(detector.isMultientityDetector()).thenReturn(true);
        when(detector.getCategoryField()).thenReturn(Arrays.asList(new String[] { serviceField, hostField }));
        detectorId = "123";
        when(detector.getDetectorId()).thenReturn(detectorId);
        when(detector.getTimeField()).thenReturn("testTimeField");
        when(detector.getIndices()).thenReturn(Arrays.asList("testIndices"));
        IntervalTimeConfiguration detectionInterval = new IntervalTimeConfiguration(1, ChronoUnit.MINUTES);
        when(detector.getDetectionInterval()).thenReturn(detectionInterval);
        when(detector.getFilterQuery()).thenReturn(QueryBuilders.matchAllQuery());

        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);

        interpolator = new LinearUniformInterpolator(new SingleFeatureLinearUniformInterpolator());

        settings = Settings
            .builder()
            .put("plugins.anomaly_detection.max_entities_per_query", 1000)
            .put("plugins.anomaly_detection.max_entities_for_preview", 30)
            .build();
        ;
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Collections
                .unmodifiableSet(
                    new HashSet<>(
                        Arrays.asList(AnomalyDetectorSettings.MAX_ENTITIES_FOR_PREVIEW, AnomalyDetectorSettings.MAX_ENTITIES_PER_QUERY)
                    )
                )
        );
        clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        clock = mock(Clock.class);
        NodeStateManager nodeStateManager = mock(NodeStateManager.class);
        doAnswer(invocation -> {
            ActionListener<Optional<AnomalyDetector>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(detector));
            return null;
        }).when(nodeStateManager).getAnomalyDetector(any(String.class), any(ActionListener.class));
        clientUtil = new SecurityClientUtil(nodeStateManager, settings);

        searchFeatureDao = new SearchFeatureDao(client, xContentRegistry(), interpolator, threadPool, clientUtil, settings, clusterService);

        String app0 = "app_0";
        String server1 = "server_1";

        attrs1 = new HashMap<>();
        attrs1.put(serviceField, app0);
        attrs1.put(hostField, server1);

        String server2 = "server_2";
        attrs1 = new HashMap<>();
        attrs1.put(serviceField, app0);
        attrs1.put(hostField, server2);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGetHighestCountEntitiesUsingTermsAgg() {
        SearchHits hits = new SearchHits(new SearchHit[] {}, null, Float.NaN);

        String entity1Name = "value1";
        long entity1Count = 3;
        StringTerms.Bucket entity1Bucket = new StringTerms.Bucket(
            new BytesRef(entity1Name.getBytes(StandardCharsets.UTF_8), 0, entity1Name.getBytes(StandardCharsets.UTF_8).length),
            entity1Count,
            null,
            false,
            0L,
            DocValueFormat.RAW
        );
        String entity2Name = "value2";
        long entity2Count = 1;
        StringTerms.Bucket entity2Bucket = new StringTerms.Bucket(
            new BytesRef(entity2Name.getBytes(StandardCharsets.UTF_8), 0, entity2Name.getBytes(StandardCharsets.UTF_8).length),
            entity2Count,
            null,
            false,
            0,
            DocValueFormat.RAW
        );
        List<StringTerms.Bucket> stringBuckets = ImmutableList.of(entity1Bucket, entity2Bucket);
        StringTerms termsAgg = new StringTerms(
            // "term_agg",
            SearchFeatureDao.AGG_NAME_TERM,
            InternalOrder.key(false),
            BucketOrder.count(false),
            1,
            0,
            Collections.emptyMap(),
            DocValueFormat.RAW,
            1,
            false,
            0,
            stringBuckets,
            0
        );

        InternalAggregations internalAggregations = InternalAggregations.from(Collections.singletonList(termsAgg));

        SearchResponseSections searchSections = new SearchResponseSections(hits, internalAggregations, null, false, false, null, 1);

        SearchResponse searchResponse = new SearchResponse(
            searchSections,
            null,
            1,
            1,
            0,
            30,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );

        doAnswer(invocation -> {
            SearchRequest request = invocation.getArgument(0);
            assertEquals(1, request.indices().length);
            assertTrue(detector.getIndices().contains(request.indices()[0]));
            AggregatorFactories.Builder aggs = request.source().aggregations();
            assertEquals(1, aggs.count());
            Collection<AggregationBuilder> factory = aggs.getAggregatorFactories();
            assertTrue(!factory.isEmpty());
            assertThat(factory.iterator().next(), instanceOf(TermsAggregationBuilder.class));

            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(SearchRequest.class), any(ActionListener.class));

        String categoryField = "fieldName";
        when(detector.getCategoryField()).thenReturn(Collections.singletonList(categoryField));
        ActionListener<List<Entity>> listener = mock(ActionListener.class);
        searchFeatureDao.getHighestCountEntities(detector, 10L, 20L, listener);

        ArgumentCaptor<List<Entity>> captor = ArgumentCaptor.forClass(List.class);
        verify(listener).onResponse(captor.capture());
        List<Entity> result = captor.getValue();
        assertEquals(2, result.size());
        assertEquals(new Entity(categoryField, entity1Name), result.get(0));
        assertEquals(new Entity(categoryField, entity2Name), result.get(1));
    }
}
