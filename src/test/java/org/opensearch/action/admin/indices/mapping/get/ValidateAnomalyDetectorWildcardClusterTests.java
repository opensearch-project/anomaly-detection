/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.action.admin.indices.mapping.get;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Before;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.rest.handler.ValidateAnomalyDetectorActionHandler;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.rest.RestRequest;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.common.exception.ValidationException;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.model.ValidationAspect;
import org.opensearch.timeseries.model.ValidationIssueType;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.node.NodeClient;

import com.google.common.collect.ImmutableList;

/**
 * Handler-level tests for the wildcard cluster prefix path. Expansion semantics are covered by
 * {@code CrossClusterConfigUtilsTests}; these tests focus on how the handler surfaces failures
 * (e.g. as {@link ValidationIssueType#INDICES} rather than misleading field errors) and confirm
 * that non-wildcard inputs do not trigger a remote cluster lookup.
 */
public class ValidateAnomalyDetectorWildcardClusterTests extends AbstractTimeSeriesTest {

    private ClusterService clusterService;
    private TransportService transportService;
    private ADIndexManagement anomalyDetectionIndices;
    private TimeValue requestTimeout;
    private int maxSingleEntityAnomalyDetectors;
    private int maxMultiEntityAnomalyDetectors;
    private int maxAnomalyFeatures;
    private int maxCategoricalFields;
    private RestRequest.Method method;
    private SearchFeatureDao searchFeatureDao;
    private Clock clock;
    private Settings settings;

    @Mock
    private Client clientMock;
    @Mock
    private ThreadPool threadPool;
    private ThreadContext threadContext;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.initMocks(this);

        settings = Settings.EMPTY;
        clusterService = mock(ClusterService.class);
        ClusterName clusterName = mock(ClusterName.class);
        when(clusterService.getClusterName()).thenReturn(clusterName);
        when(clusterName.value()).thenReturn("local");

        transportService = mock(TransportService.class);

        anomalyDetectionIndices = mock(ADIndexManagement.class);
        when(anomalyDetectionIndices.doesConfigIndexExist()).thenReturn(true);

        requestTimeout = new TimeValue(1000L);
        maxSingleEntityAnomalyDetectors = 1000;
        maxMultiEntityAnomalyDetectors = 10;
        maxAnomalyFeatures = 5;
        maxCategoricalFields = 10;
        method = RestRequest.Method.POST;
        searchFeatureDao = mock(SearchFeatureDao.class);
        clock = mock(Clock.class);

        threadContext = new ThreadContext(settings);
        Mockito.doReturn(threadPool).when(clientMock).threadPool();
        Mockito.doReturn(threadContext).when(threadPool).getThreadContext();
    }

    public void testWildcardClusterPrefix_noRemoteClusters_failsWithIndicesIssue() throws IOException, InterruptedException {
        AnomalyDetector detector = detectorWithIndices(ImmutableList.of("*:test-index"));
        AtomicReference<Exception> error = runValidationExpectingFailure(detector, Collections.emptySet(), new AtomicBoolean());
        assertNotNull(error.get());
        assertTrue("expected ValidationException but got " + error.get().getClass(), error.get() instanceof ValidationException);
        assertEquals(ValidationIssueType.INDICES, ((ValidationException) error.get()).getType());
    }

    public void testWildcardClusterPrefix_noMatchingCluster_failsWithIndicesIssue() throws IOException, InterruptedException {
        AnomalyDetector detector = detectorWithIndices(ImmutableList.of("nomatch*:test-index"));
        Set<String> remotes = new LinkedHashSet<>(Arrays.asList("remote1", "remote2"));
        AtomicReference<Exception> error = runValidationExpectingFailure(detector, remotes, new AtomicBoolean());
        assertNotNull(error.get());
        assertTrue(error.get() instanceof ValidationException);
        assertEquals(ValidationIssueType.INDICES, ((ValidationException) error.get()).getType());
    }

    public void testWildcardClusterPrefix_emptyIndexPart_failsWithIndicesIssue() throws IOException, InterruptedException {
        AnomalyDetector detector = detectorWithIndices(ImmutableList.of("*:"));
        Set<String> remotes = new LinkedHashSet<>(Arrays.asList("remote1"));
        AtomicReference<Exception> error = runValidationExpectingFailure(detector, remotes, new AtomicBoolean());
        assertNotNull(error.get());
        assertTrue(error.get() instanceof ValidationException);
        assertEquals(ValidationIssueType.INDICES, ((ValidationException) error.get()).getType());
    }

    public void testNoWildcard_skipsRemoteClusterLookup() throws IOException, InterruptedException {
        AnomalyDetector detector = detectorWithIndices(ImmutableList.of("local-only-index"));
        AtomicBoolean remoteLookupCalled = new AtomicBoolean(false);
        runValidationExpectingFailure(detector, Collections.emptySet(), remoteLookupCalled);
        assertFalse("remote cluster lookup must not happen for non-wildcard indices", remoteLookupCalled.get());
    }

    /**
     * A wildcard cluster prefix combined with a wildcard index pattern that matches no index on
     * any expanded cluster must surface as an {@link ValidationIssueType#INDICES} error. The
     * mapping call returns successfully with an empty response in this case (rather than throwing
     * {@code IndexNotFoundException}), so a separate code path is needed to detect it.
     */
    public void testWildcardClusterPrefix_indexNotFound_reportsIndicesError() throws IOException, InterruptedException {
        AnomalyDetector detector = detectorWithIndices(ImmutableList.of("*:test-index4*"));
        Set<String> remotes = new LinkedHashSet<>(Arrays.asList("remote1"));

        AtomicReference<Exception> error = runValidationWithEmptyMappings(detector, remotes);
        assertNotNull("expected a validation failure", error.get());
        assertTrue("expected ValidationException but got " + error.get().getClass(), error.get() instanceof ValidationException);
        ValidationException ve = (ValidationException) error.get();
        assertEquals("expected INDICES issue type, not a misleading TIMEFIELD_FIELD failure", ValidationIssueType.INDICES, ve.getType());
        assertTrue("error should mention 'no such index'; got: " + ve.getMessage(), ve.getMessage().contains("no such index"));
    }

    /**
     * Category validation must fail when no single index in the resolved set contains every
     * requested category field — partial coverage spread across indices is not enough.
     */
    public void testCategoryField_partialCoverageAcrossIndices_fails() throws IOException, InterruptedException {
        AnomalyDetector detector = TestHelpers
            .randomAnomalyDetectorUsingCategoryFields("id", "timestamp", ImmutableList.of("idx-1", "idx-2"), Arrays.asList("cat1", "cat2"));

        Map<String, Map<String, String>> indexToFields = new LinkedHashMap<>();
        indexToFields.put("idx-1", Collections.singletonMap("cat1", CommonName.KEYWORD_TYPE));
        indexToFields.put("idx-2", Collections.singletonMap("cat2", CommonName.KEYWORD_TYPE));

        AtomicReference<Exception> error = runValidationWithCategoryMappings(detector, indexToFields);
        assertNotNull("expected a validation failure", error.get());
        assertTrue("expected ValidationException but got " + error.get().getClass(), error.get() instanceof ValidationException);
        ValidationException ve = (ValidationException) error.get();
        assertEquals(ValidationIssueType.CATEGORY, ve.getType());
        assertTrue(
            "expected message naming the missing categorical field; got: " + ve.getMessage(),
            ve.getMessage().contains("categorical field")
        );
    }

    /**
     * Category validation must pass when at least one index in the resolved set contains every
     * requested category field, even if sibling indices are missing some.
     */
    public void testCategoryField_oneIndexHasAllCategories_passesCategoryCheck() throws IOException, InterruptedException {
        AnomalyDetector detector = TestHelpers
            .randomAnomalyDetectorUsingCategoryFields("id", "timestamp", ImmutableList.of("idx-1", "idx-2"), Arrays.asList("cat1", "cat2"));

        Map<String, Map<String, String>> indexToFields = new LinkedHashMap<>();
        indexToFields.put("idx-1", Collections.singletonMap("cat1", CommonName.KEYWORD_TYPE));
        Map<String, String> bothFields = new LinkedHashMap<>();
        bothFields.put("cat1", CommonName.KEYWORD_TYPE);
        bothFields.put("cat2", CommonName.KEYWORD_TYPE);
        indexToFields.put("idx-2", bothFields);

        AtomicReference<Exception> error = runValidationWithCategoryMappings(detector, indexToFields);
        // The handler may still fail downstream (e.g. "no docs in user index"), but the failure
        // must not be a CATEGORY-typed one.
        if (error.get() instanceof ValidationException) {
            assertNotEquals(
                "category check should not have failed when one index covers all categories",
                ValidationIssueType.CATEGORY,
                ((ValidationException) error.get()).getType()
            );
        }
    }

    /**
     * A wildcard cluster prefix paired with a concrete index name (no trailing wildcard) used to
     * surface as a hard 404 {@code IndexNotFoundException} whenever any one expanded cluster
     * lacked the index, even though a sibling cluster had it. The mapping check passes (because
     * at least one cluster validates), but the subsequent cross-cluster {@code SearchRequest}
     * against the user-supplied indices then fails with strict expansion. This test pins the
     * fix: the user-indices {@code SearchRequest} uses lenient cross-cluster options so a
     * missing concrete index on a sibling cluster is treated as "no docs there" rather than
     * failing the request.
     */
    public void testWildcardClusterPrefix_indexExistsOnOnlyOneCluster_usesLenientSearchOptions() throws IOException, InterruptedException {
        AnomalyDetector detector = TestHelpers
            .randomAnomalyDetectorUsingCategoryFields("id", "timestamp", ImmutableList.of("*:test-index1"), Arrays.asList("category"));
        Set<String> remotes = new LinkedHashSet<>(Arrays.asList("remote-with-index", "remote-without-index"));
        AtomicReference<SearchRequest> userIndicesSearch = new AtomicReference<>();
        AtomicReference<Exception> error = runValidationWithPartialClusterCoverage(detector, remotes, userIndicesSearch);

        // The validation flow may legitimately fail downstream of the wildcard search step (for
        // example because the synthetic SearchResponse has no hits, surfacing as
        // "no docs in user index"). What must NOT happen is a leaked IndexNotFoundException.
        if (error.get() != null) {
            assertFalse("validation must not leak IndexNotFoundException: " + error.get(), error.get() instanceof IndexNotFoundException);
            Throwable cause = error.get().getCause();
            while (cause != null) {
                assertFalse(
                    "validation must not leak IndexNotFoundException via cause chain: " + error.get(),
                    cause instanceof IndexNotFoundException
                );
                cause = cause.getCause();
            }
        }

        SearchRequest captured = userIndicesSearch.get();
        assertNotNull("expected user-indices search to be issued", captured);
        assertEquals(
            "user-indices search must use lenient options to tolerate missing index on sibling clusters",
            IndicesOptions.lenientExpandOpen(),
            captured.indicesOptions()
        );
    }

    /**
     * Timefield check must pass when at least one index in the resolved set has the field —
     * mirrors the category-field rule.
     */
    public void testTimeField_oneIndexHasTimeField_passesTimeFieldCheck() throws IOException, InterruptedException {
        AnomalyDetector detector = TestHelpers
            .randomAnomalyDetectorUsingCategoryFields("id", "@timestamp", ImmutableList.of("idx-1", "idx-2"), Arrays.asList("cat1"));

        Map<String, Map<String, String>> timestampMappings = new LinkedHashMap<>();
        timestampMappings.put("idx-1", Collections.emptyMap());
        timestampMappings.put("idx-2", Collections.singletonMap("@timestamp", CommonName.DATE_TYPE));

        Map<String, Map<String, String>> categoryMappings = new LinkedHashMap<>();
        categoryMappings.put("idx-1", Collections.singletonMap("cat1", CommonName.KEYWORD_TYPE));
        categoryMappings.put("idx-2", Collections.singletonMap("cat1", CommonName.KEYWORD_TYPE));

        AtomicReference<Exception> error = runValidationWithExplicitMappings(detector, timestampMappings, categoryMappings);

        // Downstream INDICES failures are fine (search returns zero hits); only the
        // TIMEFIELD_FIELD-typed failure is what we're guarding against here.
        if (error.get() instanceof ValidationException) {
            assertNotEquals(
                "timefield check should pass when at least one index has the time field",
                ValidationIssueType.TIMEFIELD_FIELD,
                ((ValidationException) error.get()).getType()
            );
        }
    }

    /**
     * Wrong-type on any index is still a hard failure — at-least-one-index coverage relaxes
     * only the missing case, not the inconsistent-mapping case.
     */
    public void testTimeField_wrongTypeOnAnyIndex_failsAsTimefield() throws IOException, InterruptedException {
        AnomalyDetector detector = TestHelpers
            .randomAnomalyDetectorUsingCategoryFields("id", "@timestamp", ImmutableList.of("idx-1", "idx-2"), Arrays.asList("cat1"));

        Map<String, Map<String, String>> timestampMappings = new LinkedHashMap<>();
        timestampMappings.put("idx-1", Collections.singletonMap("@timestamp", CommonName.KEYWORD_TYPE));
        timestampMappings.put("idx-2", Collections.singletonMap("@timestamp", CommonName.DATE_TYPE));

        Map<String, Map<String, String>> categoryMappings = new LinkedHashMap<>();
        categoryMappings.put("idx-1", Collections.singletonMap("cat1", CommonName.KEYWORD_TYPE));
        categoryMappings.put("idx-2", Collections.singletonMap("cat1", CommonName.KEYWORD_TYPE));

        AtomicReference<Exception> error = runValidationWithExplicitMappings(detector, timestampMappings, categoryMappings);

        assertNotNull("expected a validation failure", error.get());
        assertTrue("expected ValidationException but got " + error.get().getClass(), error.get() instanceof ValidationException);
        assertEquals(ValidationIssueType.TIMEFIELD_FIELD, ((ValidationException) error.get()).getType());
    }

    private AnomalyDetector detectorWithIndices(java.util.List<String> indices) throws IOException {
        return TestHelpers.randomAnomalyDetectorUsingCategoryFields("id", "timestamp", indices, Arrays.asList("category"));
    }

    /**
     * Builds a handler whose remote cluster lookup is stubbed to return {@code remoteClusters},
     * and records whether the lookup was actually called via {@code remoteLookupCalled}.
     */
    private AtomicReference<Exception> runValidationExpectingFailure(
        AnomalyDetector detector,
        Set<String> remoteClusters,
        AtomicBoolean remoteLookupCalled
    ) throws InterruptedException {
        NodeClient client = new NodeClient(Settings.EMPTY, threadPool);
        NodeStateManager nodeStateManager = mock(NodeStateManager.class);
        SecurityClientUtil clientUtil = new SecurityClientUtil(nodeStateManager, settings);

        ValidateAnomalyDetectorActionHandler handler = new ValidateAnomalyDetectorActionHandler(
            clusterService,
            client,
            clientUtil,
            transportService,
            anomalyDetectionIndices,
            detector,
            requestTimeout,
            maxSingleEntityAnomalyDetectors,
            maxMultiEntityAnomalyDetectors,
            maxAnomalyFeatures,
            maxCategoricalFields,
            method,
            xContentRegistry(),
            null,
            searchFeatureDao,
            ValidationAspect.DETECTOR.getName(),
            clock,
            settings
        ) {
            @Override
            protected Set<String> getRegisteredRemoteClusterNames() {
                remoteLookupCalled.set(true);
                return remoteClusters;
            }
        };

        AtomicReference<Exception> error = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        handler.start(ActionListener.wrap(response -> latch.countDown(), e -> {
            error.set(e);
            latch.countDown();
        }));
        assertTrue("validation did not complete in time", latch.await(10, TimeUnit.SECONDS));
        return error;
    }

    /**
     * Drives the handler against a client that returns an empty {@code GetFieldMappings}
     * response, simulating a wildcard index pattern that matched no index on the remote.
     */
    private AtomicReference<Exception> runValidationWithEmptyMappings(AnomalyDetector detector, Set<String> remoteClusters)
        throws InterruptedException {
        NodeClient client = new NodeClient(Settings.EMPTY, threadPool) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (action.equals(GetFieldMappingsAction.INSTANCE)) {
                    @SuppressWarnings("unchecked")
                    Response empty = (Response) new GetFieldMappingsResponse(Collections.emptyMap());
                    listener.onResponse(empty);
                    return;
                }
                listener.onFailure(new UnsupportedOperationException("unexpected action " + action));
            }

            @Override
            public org.opensearch.transport.client.Client getRemoteClusterClient(String clusterAlias) {
                return this;
            }
        };
        NodeStateManager nodeStateManager = mock(NodeStateManager.class);
        SecurityClientUtil clientUtil = new SecurityClientUtil(nodeStateManager, settings);

        ValidateAnomalyDetectorActionHandler handler = new ValidateAnomalyDetectorActionHandler(
            clusterService,
            client,
            clientUtil,
            transportService,
            anomalyDetectionIndices,
            detector,
            requestTimeout,
            maxSingleEntityAnomalyDetectors,
            maxMultiEntityAnomalyDetectors,
            maxAnomalyFeatures,
            maxCategoricalFields,
            method,
            xContentRegistry(),
            null,
            searchFeatureDao,
            ValidationAspect.DETECTOR.getName(),
            clock,
            settings
        ) {
            @Override
            protected Set<String> getRegisteredRemoteClusterNames() {
                return remoteClusters;
            }
        };

        AtomicReference<Exception> error = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        handler.start(ActionListener.wrap(response -> latch.countDown(), e -> {
            error.set(e);
            latch.countDown();
        }));
        assertTrue("validation did not complete in time", latch.await(10, TimeUnit.SECONDS));
        return error;
    }

    /**
     * Drives the handler against a node client whose per-remote behavior is asymmetric: one
     * remote ("remote-with-index") returns valid date-typed timestamp and keyword-typed
     * category mappings for "test-index1", while the other ("remote-without-index") throws
     * {@link IndexNotFoundException}. Search calls return zero hits and the SearchRequest
     * fired against the user-supplied indices is captured for assertion.
     */
    private AtomicReference<Exception> runValidationWithPartialClusterCoverage(
        AnomalyDetector detector,
        Set<String> remoteClusters,
        AtomicReference<SearchRequest> userIndicesSearchOut
    ) throws InterruptedException {
        NodeClient localClient = new NodeClient(Settings.EMPTY, threadPool) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (action.equals(SearchAction.INSTANCE)) {
                    SearchRequest sr = (SearchRequest) request;
                    if (sr.indices() != null && sr.indices().length > 0 && "*:test-index1".equals(sr.indices()[0])) {
                        userIndicesSearchOut.set(sr);
                    }
                    SearchResponse mock = mock(SearchResponse.class);
                    when(mock.getHits()).thenReturn(TestHelpers.createSearchHits(0));
                    @SuppressWarnings("unchecked")
                    Response casted = (Response) mock;
                    listener.onResponse(casted);
                    return;
                }
                listener.onFailure(new UnsupportedOperationException("unexpected local action " + action));
            }

            @Override
            public org.opensearch.transport.client.Client getRemoteClusterClient(String clusterAlias) {
                boolean hasIndex = "remote-with-index".equals(clusterAlias);
                return new NodeClient(Settings.EMPTY, threadPool) {
                    @Override
                    public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                        ActionType<Response> action,
                        Request request,
                        ActionListener<Response> listener
                    ) {
                        if (action.equals(GetFieldMappingsAction.INSTANCE)) {
                            if (!hasIndex) {
                                listener.onFailure(new IndexNotFoundException("test-index1"));
                                return;
                            }
                            try {
                                GetFieldMappingsRequest req = (GetFieldMappingsRequest) request;
                                boolean isTimestampLookup = Arrays.asList(req.fields()).contains("timestamp");
                                Map<String, Map<String, String>> mappings = new HashMap<>();
                                if (isTimestampLookup) {
                                    mappings.put("test-index1", Collections.singletonMap("timestamp", CommonName.DATE_TYPE));
                                } else {
                                    mappings.put("test-index1", Collections.singletonMap("category", CommonName.KEYWORD_TYPE));
                                }
                                @SuppressWarnings("unchecked")
                                Response resp = (Response) new GetFieldMappingsResponse(TestHelpers.createMultiFieldMappings(mappings));
                                listener.onResponse(resp);
                            } catch (Exception e) {
                                listener.onFailure(e);
                            }
                            return;
                        }
                        listener.onFailure(new UnsupportedOperationException("unexpected remote action " + action));
                    }
                };
            }
        };

        NodeStateManager nodeStateManager = mock(NodeStateManager.class);
        SecurityClientUtil clientUtil = new SecurityClientUtil(nodeStateManager, settings);

        ValidateAnomalyDetectorActionHandler handler = new ValidateAnomalyDetectorActionHandler(
            clusterService,
            localClient,
            clientUtil,
            transportService,
            anomalyDetectionIndices,
            detector,
            requestTimeout,
            maxSingleEntityAnomalyDetectors,
            maxMultiEntityAnomalyDetectors,
            maxAnomalyFeatures,
            maxCategoricalFields,
            method,
            xContentRegistry(),
            null,
            searchFeatureDao,
            ValidationAspect.DETECTOR.getName(),
            clock,
            settings
        ) {
            @Override
            protected Set<String> getRegisteredRemoteClusterNames() {
                return remoteClusters;
            }
        };

        AtomicReference<Exception> error = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        handler.start(ActionListener.wrap(response -> latch.countDown(), e -> {
            error.set(e);
            latch.countDown();
        }));
        assertTrue("validation did not complete in time", latch.await(10, TimeUnit.SECONDS));
        return error;
    }

    /**
     * Drives the handler against a client that returns a valid date-typed timestamp mapping for
     * each provided index and {@code indexToFields} for the category-field lookup. Search calls
     * return zero hits so the flow reaches the category check.
     */
    private AtomicReference<Exception> runValidationWithCategoryMappings(
        AnomalyDetector detector,
        Map<String, Map<String, String>> indexToFields
    ) throws InterruptedException {
        Map<String, Map<String, String>> timestampMappings = new HashMap<>();
        for (String idx : indexToFields.keySet()) {
            timestampMappings.put(idx, Collections.singletonMap("timestamp", CommonName.DATE_TYPE));
        }
        return runValidationWithExplicitMappings(detector, timestampMappings, indexToFields);
    }

    /**
     * Same as {@link #runValidationWithCategoryMappings} but the caller controls the timestamp
     * mappings — use when a test needs an index to be missing the timefield.
     */
    private AtomicReference<Exception> runValidationWithExplicitMappings(
        AnomalyDetector detector,
        Map<String, Map<String, String>> timestampMappings,
        Map<String, Map<String, String>> categoryMappings
    ) throws InterruptedException {
        String configuredTimeField = detector.getTimeField();
        NodeClient client = new NodeClient(Settings.EMPTY, threadPool) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                try {
                    if (action.equals(GetFieldMappingsAction.INSTANCE)) {
                        GetFieldMappingsRequest req = (GetFieldMappingsRequest) request;
                        boolean looksLikeTimestamp = Arrays.asList(req.fields()).contains(configuredTimeField);
                        @SuppressWarnings("unchecked")
                        Response resp = (Response) new GetFieldMappingsResponse(
                            looksLikeTimestamp
                                ? TestHelpers.createMultiFieldMappings(timestampMappings)
                                : TestHelpers.createMultiFieldMappings(categoryMappings)
                        );
                        listener.onResponse(resp);
                        return;
                    }
                    if (action.equals(org.opensearch.action.search.SearchAction.INSTANCE)) {
                        org.opensearch.action.search.SearchResponse sr = mock(org.opensearch.action.search.SearchResponse.class);
                        when(sr.getHits()).thenReturn(TestHelpers.createSearchHits(0));
                        @SuppressWarnings("unchecked")
                        Response casted = (Response) sr;
                        listener.onResponse(casted);
                        return;
                    }
                    listener.onFailure(new UnsupportedOperationException("unexpected action " + action));
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }
        };
        NodeStateManager nodeStateManager = mock(NodeStateManager.class);
        SecurityClientUtil clientUtil = new SecurityClientUtil(nodeStateManager, settings);

        ValidateAnomalyDetectorActionHandler handler = new ValidateAnomalyDetectorActionHandler(
            clusterService,
            client,
            clientUtil,
            transportService,
            anomalyDetectionIndices,
            detector,
            requestTimeout,
            maxSingleEntityAnomalyDetectors,
            maxMultiEntityAnomalyDetectors,
            maxAnomalyFeatures,
            maxCategoricalFields,
            method,
            xContentRegistry(),
            null,
            searchFeatureDao,
            ValidationAspect.DETECTOR.getName(),
            clock,
            settings
        );

        AtomicReference<Exception> error = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        handler.start(ActionListener.wrap(response -> latch.countDown(), e -> {
            error.set(e);
            latch.countDown();
        }));
        assertTrue("validation did not complete in time", latch.await(10, TimeUnit.SECONDS));
        return error;
    }
}
