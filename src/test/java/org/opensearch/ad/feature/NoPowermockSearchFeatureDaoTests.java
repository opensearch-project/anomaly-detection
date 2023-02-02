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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.AnyOf.anyOf;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.BytesRef;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.opensearch.action.ActionListener;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponse.Clusters;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.ad.AbstractADTest;
import org.opensearch.ad.NodeStateManager;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.dataprocessor.LinearUniformInterpolator;
import org.opensearch.ad.dataprocessor.SingleFeatureLinearUniformInterpolator;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.Entity;
import org.opensearch.ad.model.Feature;
import org.opensearch.ad.model.IntervalTimeConfiguration;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.util.SecurityClientUtil;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.*;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.opensearch.search.aggregations.bucket.range.InternalDateRange;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.*;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.internal.InternalSearchResponse;

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

        settings = Settings.EMPTY;
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Collections
                .unmodifiableSet(
                    new HashSet<>(Arrays.asList(AnomalyDetectorSettings.MAX_ENTITIES_FOR_PREVIEW, AnomalyDetectorSettings.PAGE_SIZE))
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

        searchFeatureDao = new SearchFeatureDao(
            client,
            xContentRegistry(), // Important. Without this, ParseUtils cannot parse anything
            interpolator,
            clientUtil,
            settings,
            clusterService,
            AnomalyDetectorSettings.NUM_SAMPLES_PER_TREE,
            clock,
            1,
            1,
            60_000L
        );

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

    private SearchResponse createPageResponse(Map<String, Object> attrs) {
        CompositeAggregation pageOneComposite = mock(CompositeAggregation.class);
        when(pageOneComposite.getName()).thenReturn(SearchFeatureDao.AGG_NAME_TOP);
        when(pageOneComposite.afterKey()).thenReturn(attrs);

        List<CompositeAggregation.Bucket> pageOneBuckets = new ArrayList<>();
        CompositeAggregation.Bucket bucket = mock(CompositeAggregation.Bucket.class);
        when(bucket.getKey()).thenReturn(attrs);
        when(bucket.getDocCount()).thenReturn(1552L);
        pageOneBuckets.add(bucket);

        when(pageOneComposite.getBuckets())
            .thenAnswer((Answer<List<CompositeAggregation.Bucket>>) invocation -> { return pageOneBuckets; });

        Aggregations pageOneAggs = new Aggregations(Collections.singletonList(pageOneComposite));

        SearchResponseSections pageOneSections = new SearchResponseSections(SearchHits.empty(), pageOneAggs, null, false, null, null, 1);

        return new SearchResponse(pageOneSections, null, 1, 1, 0, 0, ShardSearchFailure.EMPTY_ARRAY, Clusters.EMPTY);
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
            SearchFeatureDao.AGG_NAME_TOP,
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
        assertEquals(Entity.createSingleAttributeEntity(categoryField, entity1Name), result.get(0));
        assertEquals(Entity.createSingleAttributeEntity(categoryField, entity2Name), result.get(1));
    }

    @SuppressWarnings("unchecked")
    public void testGetHighestCountEntitiesUsingPagination() {
        SearchResponse response1 = createPageResponse(attrs1);

        CountDownLatch inProgress = new CountDownLatch(1);
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);

            inProgress.countDown();
            listener.onResponse(response1);

            return null;
        }).when(client).search(any(), any());

        ActionListener<List<Entity>> listener = mock(ActionListener.class);

        searchFeatureDao.getHighestCountEntities(detector, 10L, 20L, listener);

        ArgumentCaptor<List<Entity>> captor = ArgumentCaptor.forClass(List.class);
        verify(listener).onResponse(captor.capture());
        List<Entity> result = captor.getValue();
        assertEquals(1, result.size());
        assertEquals(Entity.createEntityByReordering(attrs1), result.get(0));
    }

    @SuppressWarnings("unchecked")
    public void testGetHighestCountEntitiesExhaustedPages() throws InterruptedException {
        SearchResponse response1 = createPageResponse(attrs1);

        CompositeAggregation emptyComposite = mock(CompositeAggregation.class);
        when(emptyComposite.getName()).thenReturn(SearchFeatureDao.AGG_NAME_TOP);
        when(emptyComposite.afterKey()).thenReturn(null);
        // empty bucket
        when(emptyComposite.getBuckets())
            .thenAnswer((Answer<List<CompositeAggregation.Bucket>>) invocation -> { return new ArrayList<CompositeAggregation.Bucket>(); });
        Aggregations emptyAggs = new Aggregations(Collections.singletonList(emptyComposite));
        SearchResponseSections emptySections = new SearchResponseSections(SearchHits.empty(), emptyAggs, null, false, null, null, 1);
        SearchResponse emptyResponse = new SearchResponse(emptySections, null, 1, 1, 0, 0, ShardSearchFailure.EMPTY_ARRAY, Clusters.EMPTY);

        CountDownLatch inProgress = new CountDownLatch(2);
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            inProgress.countDown();
            if (inProgress.getCount() == 1) {
                listener.onResponse(response1);
            } else {
                listener.onResponse(emptyResponse);
            }

            return null;
        }).when(client).search(any(), any());

        ActionListener<List<Entity>> listener = mock(ActionListener.class);

        searchFeatureDao = new SearchFeatureDao(
            client,
            xContentRegistry(),
            interpolator,
            clientUtil,
            settings,
            clusterService,
            AnomalyDetectorSettings.NUM_SAMPLES_PER_TREE,
            clock,
            2,
            1,
            60_000L
        );

        searchFeatureDao.getHighestCountEntities(detector, 10L, 20L, listener);

        ArgumentCaptor<List<Entity>> captor = ArgumentCaptor.forClass(List.class);
        verify(listener).onResponse(captor.capture());
        List<Entity> result = captor.getValue();
        assertEquals(1, result.size());
        assertEquals(Entity.createEntityByReordering(attrs1), result.get(0));
        // both counts are used in client.search
        assertTrue(inProgress.await(10000L, TimeUnit.MILLISECONDS));
    }

    @SuppressWarnings("unchecked")
    public void testGetHighestCountEntitiesNotEnoughTime() throws InterruptedException {
        SearchResponse response1 = createPageResponse(attrs1);
        SearchResponse response2 = createPageResponse(attrs2);

        CountDownLatch inProgress = new CountDownLatch(2);
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            inProgress.countDown();
            if (inProgress.getCount() == 1) {
                listener.onResponse(response1);
            } else {
                listener.onResponse(response2);
            }

            return null;
        }).when(client).search(any(), any());

        ActionListener<List<Entity>> listener = mock(ActionListener.class);

        long timeoutMillis = 60_000L;
        searchFeatureDao = new SearchFeatureDao(
            client,
            xContentRegistry(),
            interpolator,
            clientUtil,
            settings,
            clusterService,
            AnomalyDetectorSettings.NUM_SAMPLES_PER_TREE,
            clock,
            2,
            1,
            timeoutMillis
        );

        CountDownLatch clockInvoked = new CountDownLatch(2);

        when(clock.millis()).thenAnswer(new Answer<Long>() {
            @Override
            public Long answer(InvocationOnMock invocation) throws Throwable {
                clockInvoked.countDown();
                if (clockInvoked.getCount() == 1) {
                    return 1L;
                } else {
                    return 2L + timeoutMillis;
                }
            }
        });

        searchFeatureDao.getHighestCountEntities(detector, 10L, 20L, listener);

        ArgumentCaptor<List<Entity>> captor = ArgumentCaptor.forClass(List.class);
        verify(listener).onResponse(captor.capture());
        List<Entity> result = captor.getValue();
        assertEquals(1, result.size());
        assertEquals(Entity.createEntityByReordering(attrs1), result.get(0));
        // exited early due to timeout
        assertEquals(1, inProgress.getCount());
        // first called to create expired time; second called to check if time has expired
        assertTrue(clockInvoked.await(10000L, TimeUnit.MILLISECONDS));
    }

    @SuppressWarnings("unchecked")
    public void getColdStartSamplesForPeriodsTemplate(DocValueFormat format) throws IOException, InterruptedException {
        detector = TestHelpers.AnomalyDetectorBuilder
            .newInstance()
            .setCategoryFields(ImmutableList.of(randomAlphaOfLength(5)))
            .setFeatureAttributes(
                Collections.singletonList(new Feature("deny_sum", "deny sum", true, new SumAggregationBuilder("deny_sum").field("deny")))
            )
            .build();

        InternalDateRange.Factory factory = new InternalDateRange.Factory();
        InternalDateRange.Bucket bucket1 = factory
            .createBucket(
                "1634786770964-1634786830964",
                1634786770964L,
                1634786830964L,
                1,
                InternalAggregations.from(Arrays.asList(new InternalMax("deny_sum", 840.0, DocValueFormat.RAW, Collections.emptyMap()))),
                false,
                format
            );
        InternalDateRange.Bucket bucket2 = factory
            .createBucket(
                "1634790370964-1634790430964",
                1634790370964L,
                1634790430964L,
                0,
                InternalAggregations.from(Arrays.asList(new InternalMax("deny_sum", 0, DocValueFormat.RAW, Collections.emptyMap()))),
                false,
                format
            );
        InternalDateRange.Bucket bucket3 = factory
            .createBucket(
                "1634793970964-1634794030964",
                1634793970964L,
                1634794030964L,
                1,
                InternalAggregations.from(Arrays.asList(new InternalMax("deny_sum", 3489.0, DocValueFormat.RAW, Collections.emptyMap()))),
                false,
                format
            );
        InternalDateRange range = factory
            .create("date_range", Arrays.asList(bucket2, bucket3, bucket1), DocValueFormat.RAW, false, Collections.emptyMap());

        InternalAggregations aggs = InternalAggregations.from(Arrays.asList(range));
        SearchHits searchHits = new SearchHits(new SearchHit[0], new TotalHits(2189, TotalHits.Relation.EQUAL_TO), Float.NaN);
        InternalSearchResponse internalSearchResponse = new InternalSearchResponse(searchHits, aggs, null, null, false, null, 1);
        SearchResponse response = new SearchResponse(
            internalSearchResponse,
            null,
            1,
            1,
            0,
            4,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );

        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onResponse(response);
            return null;
        }).when(client).search(any(), any(ActionListener.class));

        List<Entry<Long, Long>> sampleRanges = new ArrayList<>();
        sampleRanges.add(new SimpleImmutableEntry<Long, Long>(1634793970964L, 1634794030964L));
        sampleRanges.add(new SimpleImmutableEntry<Long, Long>(1634790370964L, 1634790430964L));
        sampleRanges.add(new SimpleImmutableEntry<Long, Long>(1634786770964L, 1634786830964L));

        CountDownLatch inProgressLatch = new CountDownLatch(1);

        // test that the results are in ascending order of time and zero doc results are not ignored
        searchFeatureDao
            .getColdStartSamplesForPeriods(
                detector,
                sampleRanges,
                Entity.createSingleAttributeEntity("field", "abc"),
                true,
                ActionListener.wrap(samples -> {
                    assertEquals(3, samples.size());
                    for (int i = 0; i < samples.size(); i++) {
                        Optional<double[]> sample = samples.get(i);
                        double[] array = sample.get();
                        assertEquals(1, array.length);
                        if (i == 0) {
                            assertEquals(840, array[0], 1e-10);
                        } else if (i == 1) {
                            assertEquals(0, array[0], 1e-10);
                        } else {
                            assertEquals(3489.0, array[0], 1e-10);
                        }
                    }
                    inProgressLatch.countDown();
                }, exception -> {
                    LOG.error("stack trace", exception);
                    assertTrue("Should not reach here ", false);
                    inProgressLatch.countDown();
                })
            );

        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));

        CountDownLatch inProgressLatch2 = new CountDownLatch(1);

        // test that the results are in ascending order of time and zero doc results are ignored
        searchFeatureDao
            .getColdStartSamplesForPeriods(
                detector,
                sampleRanges,
                Entity.createSingleAttributeEntity("field", "abc"),
                false,
                ActionListener.wrap(samples -> {
                    assertEquals(2, samples.size());
                    for (int i = 0; i < samples.size(); i++) {
                        Optional<double[]> sample = samples.get(i);
                        double[] array = sample.get();
                        assertEquals(1, array.length);
                        if (i == 0) {
                            assertEquals(840, array[0], 1e-10);
                        } else {
                            assertEquals(3489.0, array[0], 1e-10);
                        }
                    }
                    inProgressLatch2.countDown();
                }, exception -> {
                    LOG.error("stack trace", exception);
                    assertTrue("Should not reach here ", false);
                    inProgressLatch.countDown();
                })
            );

        assertTrue(inProgressLatch2.await(100, TimeUnit.SECONDS));
    }

    public void testGetColdStartSamplesForPeriodsMillisFormat() throws IOException, InterruptedException {
        DocValueFormat format = new DocValueFormat.DateTime(
            DateFormatter.forPattern("epoch_millis"),
            ZoneOffset.UTC,
            DateFieldMapper.Resolution.MILLISECONDS
        );
        getColdStartSamplesForPeriodsTemplate(format);
    }

    public void testGetColdStartSamplesForPeriodsDefaultFormat() throws IOException, InterruptedException {
        DocValueFormat format = new DocValueFormat.DateTime(
            DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER,
            ZoneOffset.UTC,
            DateFieldMapper.Resolution.MILLISECONDS
        );
        getColdStartSamplesForPeriodsTemplate(format);
    }

    public void testGetColdStartSamplesForPeriodsRawFormat() throws IOException, InterruptedException {
        getColdStartSamplesForPeriodsTemplate(DocValueFormat.RAW);
    }

    @SuppressWarnings("unchecked")
    public void testGetFeaturesForPeriod_throwToListener_whenSearchFails() throws Exception {

        long start = 100L;
        long end = 200L;
        // when(ParseUtils.generateInternalFeatureQuery(eq(detector), eq(start), eq(end), eq(xContent))).thenReturn(searchSourceBuilder);
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onFailure(new RuntimeException());
            return null;
        }).when(client).search(any(SearchRequest.class), any(ActionListener.class));

        ActionListener<Optional<double[]>> listener = mock(ActionListener.class);
        searchFeatureDao.getFeaturesForPeriod(detector, start, end, listener);

        verify(listener).onFailure(any(Exception.class));
    }

    @SuppressWarnings("unchecked")
    public void testGetEntityMinDataTime() {
        // simulate response {"took":11,"timed_out":false,"_shards":{"total":1,
        // "successful":1,"skipped":0,"failed":0},"hits":{"max_score":null,"hits":[]},
        // "aggregations":{"min_timefield":{"value":1.602211285E12,
        // "value_as_string":"2020-10-09T02:41:25.000Z"},
        // "max_timefield":{"value":1.602348325E12,"value_as_string":"2020-10-10T16:45:25.000Z"}}}
        DocValueFormat dateFormat = new DocValueFormat.DateTime(
            DateFormatter.forPattern("strict_date_optional_time||epoch_millis"),
            ZoneId.of("UTC"),
            DateFieldMapper.Resolution.MILLISECONDS
        );
        double earliest = 1.602211285E12;
        InternalMin minInternal = new InternalMin("min_timefield", earliest, dateFormat, new HashMap<>());
        InternalAggregations internalAggregations = InternalAggregations.from(Arrays.asList(minInternal));
        SearchHits hits = new SearchHits(new SearchHit[] {}, null, Float.NaN);
        SearchResponseSections searchSections = new SearchResponseSections(hits, internalAggregations, null, false, false, null, 1);

        SearchResponse searchResponse = new SearchResponse(
            searchSections,
            null,
            1,
            1,
            0,
            11,
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
            Iterator<AggregationBuilder> iterator = factory.iterator();
            while (iterator.hasNext()) {
                assertThat(iterator.next(), anyOf(instanceOf(MaxAggregationBuilder.class), instanceOf(MinAggregationBuilder.class)));
            }

            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(SearchRequest.class), any(ActionListener.class));

        ActionListener<Optional<Long>> listener = mock(ActionListener.class);
        Entity entity = Entity.createSingleAttributeEntity("field", "app_1");
        searchFeatureDao.getEntityMinDataTime(detector, entity, listener);

        ArgumentCaptor<Optional<Long>> captor = ArgumentCaptor.forClass(Optional.class);
        verify(listener).onResponse(captor.capture());
        Optional<Long> result = captor.getValue();
        assertEquals((long) earliest, result.get().longValue());
    }

    @SuppressWarnings("unchecked")
    public void testGetFeaturesForPeriod_throwToListener_whenResponseParsingFails() throws Exception {

        long start = 100L;
        long end = 200L;

        SearchResponse searchResponse = mock(SearchResponse.class);
        SearchHits hits = new SearchHits(new SearchHit[0], new TotalHits(1L, TotalHits.Relation.EQUAL_TO), 1f);
        when(searchResponse.getHits()).thenReturn(hits);

        // when(ParseUtils.generateInternalFeatureQuery(eq(detector), eq(start), eq(end), eq(xContent))).thenReturn(searchSourceBuilder);
        when(detector.getEnabledFeatureIds()).thenReturn(null);
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(SearchRequest.class), any(ActionListener.class));

        ActionListener<Optional<double[]>> listener = mock(ActionListener.class);
        searchFeatureDao.getFeaturesForPeriod(detector, start, end, listener);

        verify(listener).onResponse(eq(Optional.empty()));
    }

    @SuppressWarnings("unchecked")
    public void testGetFeaturesForSampledPeriods_throwToListener_whenSamplingFail() {
        SearchFeatureDao spySearchFeatureDao = spy(searchFeatureDao);
        doAnswer(invocation -> {
            ActionListener<Optional<double[]>> listener = invocation.getArgument(3);
            listener.onFailure(new RuntimeException());
            return null;
        }).when(spySearchFeatureDao).getFeaturesForPeriod(any(), anyLong(), anyLong(), any(ActionListener.class));

        ActionListener<Optional<Entry<double[][], Integer>>> listener = mock(ActionListener.class);
        spySearchFeatureDao.getFeaturesForSampledPeriods(detector, 1, 1, 0, listener);

        verify(listener).onFailure(any(Exception.class));
    }

    @SuppressWarnings("unchecked")
    public void testGetLatestDataTime_returnExpectedToListener() {
        long epochTime = 100L;

        // simulate response {"took":11,"timed_out":false,"_shards":{"total":1,
        // "successful":1,"skipped":0,"failed":0},"hits":{"max_score":null,"hits":[]},
        // "aggregations":{"min_timefield":{"value":1.602211285E12,
        // "value_as_string":"2020-10-09T02:41:25.000Z"},
        // "max_timefield":{"value":1.602348325E12,"value_as_string":"2020-10-10T16:45:25.000Z"}}}
        DocValueFormat dateFormat = new DocValueFormat.DateTime(
            DateFormatter.forPattern("strict_date_optional_time||epoch_millis"),
            ZoneId.of("UTC"),
            DateFieldMapper.Resolution.MILLISECONDS
        );
        InternalMax minInternal = new InternalMax(CommonName.AGG_NAME_MAX_TIME, epochTime, dateFormat, new HashMap<>());
        InternalAggregations internalAggregations = InternalAggregations.from(Arrays.asList(minInternal));
        SearchHits hits = new SearchHits(new SearchHit[] {}, null, Float.NaN);
        SearchResponseSections searchSections = new SearchResponseSections(hits, internalAggregations, null, false, false, null, 1);

        SearchResponse searchResponse = new SearchResponse(
            searchSections,
            null,
            1,
            1,
            0,
            11,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );

        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(SearchRequest.class), any(ActionListener.class));

        // when(ParseUtils.getLatestDataTime(eq(searchResponse))).thenReturn(Optional.of(epochTime));
        ActionListener<Optional<Long>> listener = mock(ActionListener.class);
        searchFeatureDao.getLatestDataTime(detector, listener);

        ArgumentCaptor<Optional<Long>> captor = ArgumentCaptor.forClass(Optional.class);
        verify(listener).onResponse(captor.capture());
        Optional<Long> result = captor.getValue();
        assertEquals(epochTime, result.get().longValue());
    }
}
