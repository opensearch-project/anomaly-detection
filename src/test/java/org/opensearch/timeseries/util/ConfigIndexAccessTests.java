/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.util;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.mockito.ArgumentCaptor;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.Version;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.ConfigConstants;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.get.GetResult;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.model.Config;
import org.opensearch.transport.client.Client;

public class ConfigIndexAccessTests extends OpenSearchTestCase {
    private static final String DENIED = "logs-denied";
    private static final String READABLE = "logs-readable";
    private static final String DELETED = "deleted-index";
    private static final String BROKEN = "broken-index";
    private static final String ALIAS = "logs-alias";
    private static final String CONFIG_ID = "config-id";
    private static final String OWNER_ROLE = "owner_role";

    private Client client;
    private ClusterService clusterService;
    private ThreadContext threadContext;
    private User caller;
    private User owner;
    private List<String> searchedIndices;
    private RuntimeException brokenFailure;
    private java.util.function.Consumer<ActionListener<SearchResponse>> listing;
    private AtomicReference<String> rolesDuringListing;
    private AtomicReference<String[]> listedExpressions;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        threadContext = new ThreadContext(Settings.EMPTY);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        // DELETED exists in the cluster state but is gone by the time it is searched.
        Metadata metadata = Metadata
            .builder()
            .put(indexMetadata(READABLE, ALIAS), false)
            .put(indexMetadata(DENIED, ALIAS), false)
            .put(indexMetadata(DELETED), false)
            .put(indexMetadata(BROKEN), false)
            .build();
        clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(ClusterState.builder(new ClusterName("test")).metadata(metadata).build());
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);

        caller = new User("caller", Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
        owner = new User("owner", Collections.emptyList(), Arrays.asList(OWNER_ROLE), Collections.emptyList());
        searchedIndices = Collections.synchronizedList(new ArrayList<>());
        brokenFailure = new OpenSearchStatusException("blocked", RestStatus.FORBIDDEN);
        rolesDuringListing = new AtomicReference<>();
        listedExpressions = new AtomicReference<>();
        listing = listener -> listener.onResponse(listingResponse());
        doAnswer(invocation -> {
            SearchRequest request = invocation.getArgument(0);
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            if (request.source() != null && request.source().aggregations() != null) {
                // the owner's listing of the remote indices
                rolesDuringListing.set(threadContext.getTransient(ConfigConstants.OPENSEARCH_SECURITY_INJECTED_ROLES));
                listedExpressions.set(request.indices());
                listing.accept(listener);
                return null;
            }
            String index = request.indices()[0];
            searchedIndices.add(String.join(",", request.indices()));
            if (index.endsWith(DENIED)) {
                listener.onFailure(new OpenSearchSecurityException("no permissions for [indices:data/read/search]", RestStatus.FORBIDDEN));
            } else if (DELETED.equals(index)) {
                listener.onFailure(new IndexNotFoundException(index));
            } else if (BROKEN.equals(index)) {
                listener.onFailure(brokenFailure);
            } else {
                listener.onResponse(mock(SearchResponse.class));
            }
            return null;
        }).when(client).search(any(SearchRequest.class), any());
    }

    public void testProbesEachIndexInItsOwnSearch() throws IOException {
        AtomicBoolean ran = new AtomicBoolean();
        authorize(Arrays.asList(READABLE, DELETED), failOnFailure(), () -> ran.set(true));
        assertTrue(ran.get());
        assertEquals(Arrays.asList(READABLE, DELETED), searchedIndices);
    }

    public void testDeniedIndexAnswersForbiddenAndDoesNotRun() throws IOException {
        AtomicBoolean ran = new AtomicBoolean();
        AtomicReference<Exception> failure = new AtomicReference<>();
        authorize(Arrays.asList(READABLE, BROKEN, DENIED), failureTo(failure), () -> ran.set(true));
        assertFalse(ran.get());
        assertDenial(failure.get());
    }

    public void testPatternIsAuthorizedForEveryIndexItMatches() throws IOException {
        AtomicBoolean ran = new AtomicBoolean();
        AtomicReference<Exception> failure = new AtomicReference<>();
        authorize(Arrays.asList("logs-*"), failureTo(failure), () -> ran.set(true));
        assertFalse(ran.get());
        assertDenial(failure.get());
        assertEquals(Arrays.asList(DENIED, READABLE), sorted(searchedIndices));
    }

    public void testAliasIsAuthorizedForEveryIndexItPointsTo() throws IOException {
        AtomicBoolean ran = new AtomicBoolean();
        AtomicReference<Exception> failure = new AtomicReference<>();
        authorize(Arrays.asList(ALIAS), failureTo(failure), () -> ran.set(true));
        assertFalse(ran.get());
        assertDenial(failure.get());
        assertEquals(Arrays.asList(DENIED, READABLE), sorted(searchedIndices));
    }

    public void testMissingIndexIsLeftToTheRun() throws IOException {
        AtomicBoolean ran = new AtomicBoolean();
        authorize(Arrays.asList("no-such-index"), failOnFailure(), () -> ran.set(true));
        assertTrue(ran.get());
        verify(client, never()).search(any(SearchRequest.class), any());
    }

    public void testIndexDeletedBeforeProbeIsLeftToTheRun() throws IOException {
        AtomicBoolean ran = new AtomicBoolean();
        authorize(Arrays.asList(DELETED), failOnFailure(), () -> ran.set(true));
        assertTrue(ran.get());
    }

    public void testOtherProbeFailureIsReportedAndDoesNotRun() throws IOException {
        // a 403 that is not a security denial, such as an index block, is reported as itself
        AtomicBoolean ran = new AtomicBoolean();
        AtomicReference<Exception> failure = new AtomicReference<>();
        authorize(Arrays.asList(READABLE, BROKEN), failureTo(failure), () -> ran.set(true));
        assertFalse(ran.get());
        assertSame(brokenFailure, failure.get());
    }

    public void testFailingRunIsReportedAndRunsOnce() throws IOException {
        AtomicInteger runs = new AtomicInteger();
        AtomicReference<Exception> failure = new AtomicReference<>();
        RuntimeException runFailure = new IllegalStateException("run failed");
        authorize(Arrays.asList(READABLE), failureTo(failure), () -> {
            runs.incrementAndGet();
            throw runFailure;
        });
        assertEquals(1, runs.get());
        assertSame(runFailure, failure.get());
    }

    public void testNoCallerSkipsTheProbe() throws IOException {
        AtomicBoolean ran = new AtomicBoolean();
        ConfigIndexAccess
            .authorizeCaller(client, clusterService, config(Arrays.asList(DENIED)), null, stashed(), failOnFailure(), () -> ran.set(true));
        assertTrue(ran.get());
        verify(client, never()).search(any(SearchRequest.class), any());
    }

    public void testNoConfigSkipsTheProbe() {
        AtomicBoolean ran = new AtomicBoolean();
        ConfigIndexAccess.authorizeCaller(client, clusterService, (Config) null, caller, stashed(), failOnFailure(), () -> ran.set(true));
        assertTrue(ran.get());
        verify(client, never()).search(any(SearchRequest.class), any());
    }

    public void testProbeRunsInCallerContextAndRunRestoresStashedContext() throws IOException {
        threadContext.putHeader("caller-header", "present");
        ThreadContext.StoredContext callerContext = threadContext.stashContext();
        AtomicReference<String> headerDuringProbe = new AtomicReference<>();
        AtomicReference<String> headerDuringRun = new AtomicReference<>("unset");
        doAnswer(invocation -> {
            headerDuringProbe.set(threadContext.getHeader("caller-header"));
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onResponse(mock(SearchResponse.class));
            return null;
        }).when(client).search(any(SearchRequest.class), any());

        ConfigIndexAccess
            .authorizeCaller(
                client,
                clusterService,
                config(Arrays.asList(READABLE)),
                caller,
                callerContext,
                failOnFailure(),
                () -> headerDuringRun.set(threadContext.getHeader("caller-header"))
            );

        assertEquals("present", headerDuringProbe.get());
        assertNull(headerDuringRun.get());
        assertNull(threadContext.getHeader("caller-header"));
        ArgumentCaptor<SearchRequest> request = ArgumentCaptor.forClass(SearchRequest.class);
        verify(client, times(1)).search(request.capture(), any());
        assertEquals(0, request.getValue().source().size());
    }

    public void testRemoteExpressionIsListedWithOwnerRolesAndEveryIndexProbed() throws IOException {
        listing = listener -> listener.onResponse(listingResponse("remote:logs-1", "remote:logs-2"));
        AtomicBoolean ran = new AtomicBoolean();
        authorize(Arrays.asList(READABLE, "remote:logs-*"), failOnFailure(), () -> ran.set(true));

        assertTrue(ran.get());
        assertArrayEquals(new String[] { "remote:logs-*" }, listedExpressions.get());
        assertNotNull(rolesDuringListing.get());
        assertTrue(rolesDuringListing.get(), rolesDuringListing.get().contains(OWNER_ROLE));
        assertNull(threadContext.getTransient(ConfigConstants.OPENSEARCH_SECURITY_INJECTED_ROLES));
        assertEquals(Arrays.asList(READABLE, "remote:logs-*", "remote:logs-1", "remote:logs-2"), searchedIndices);
    }

    public void testDeniedRemoteIndexAnswersForbidden() throws IOException {
        listing = listener -> listener.onResponse(listingResponse("remote:" + DENIED));
        AtomicBoolean ran = new AtomicBoolean();
        AtomicReference<Exception> failure = new AtomicReference<>();
        authorize(Arrays.asList("remote:logs-*"), failureTo(failure), () -> ran.set(true));
        assertFalse(ran.get());
        assertDenial(failure.get());
    }

    public void testEmptyRemoteListingStillProbesTheExpression() throws IOException {
        // e.g. the owner's listing found nothing; the caller is still checked against the expression
        AtomicBoolean ran = new AtomicBoolean();
        AtomicReference<Exception> failure = new AtomicReference<>();
        authorize(Arrays.asList("remote:" + DENIED), failureTo(failure), () -> ran.set(true));
        assertFalse(ran.get());
        assertDenial(failure.get());
        assertEquals(Arrays.asList("remote:" + DENIED), searchedIndices);
    }

    public void testRemoteListingFailureIsReportedWithoutTheOwnerAndDoesNotRun() throws IOException {
        listing = listener -> listener
            .onFailure(
                new OpenSearchSecurityException("no permissions for [indices:data/read/search] and User [name=owner]", RestStatus.FORBIDDEN)
            );
        AtomicBoolean ran = new AtomicBoolean();
        AtomicReference<Exception> failure = new AtomicReference<>();
        authorize(Arrays.asList(READABLE, "remote:logs-*"), failureTo(failure), () -> ran.set(true));
        assertFalse(ran.get());
        assertTrue(failure.get() instanceof OpenSearchStatusException);
        assertTrue(failure.get().getMessage(), failure.get().getMessage().startsWith("Failed to list the remote indices"));
        assertFalse(failure.get().getMessage(), failure.get().getMessage().contains("owner"));
        assertTrue(searchedIndices.isEmpty());
    }

    public void testRemoteListingThrowingIsReportedAndDoesNotRun() throws IOException {
        listing = listener -> { throw new IllegalStateException("not connected"); };
        AtomicBoolean ran = new AtomicBoolean();
        AtomicReference<Exception> failure = new AtomicReference<>();
        authorize(Arrays.asList(READABLE, "remote:logs-*"), failureTo(failure), () -> ran.set(true));
        assertFalse(ran.get());
        assertTrue(failure.get() instanceof OpenSearchStatusException);
        assertTrue(failure.get().getMessage(), failure.get().getMessage().startsWith("Failed to list the remote indices"));
        assertTrue(searchedIndices.isEmpty());
    }

    public void testMissingRemoteIndexStillProbesTheExpression() throws IOException {
        listing = listener -> listener.onFailure(new IndexNotFoundException("logs-*"));
        AtomicBoolean ran = new AtomicBoolean();
        authorize(Arrays.asList(READABLE, "remote:logs-*"), failOnFailure(), () -> ran.set(true));
        assertTrue(ran.get());
        assertEquals(Arrays.asList(READABLE, "remote:logs-*"), searchedIndices);
    }

    public void testIncompleteRemoteListingFails() {
        SearchResponse shardFailure = listingResponse("remote:logs-1");
        when(shardFailure.getFailedShards()).thenReturn(1);
        expectThrows(IllegalStateException.class, () -> ConfigIndexAccess.remoteIndices(shardFailure));

        SearchResponse skippedCluster = listingResponse("remote:logs-1");
        when(skippedCluster.getClusters()).thenReturn(new SearchResponse.Clusters(2, 1, 1));
        expectThrows(IllegalStateException.class, () -> ConfigIndexAccess.remoteIndices(skippedCluster));

        SearchResponse tooMany = listingResponse("remote:logs-1");
        Terms terms = tooMany.getAggregations().get("remote_indices");
        when(terms.getSumOfOtherDocCounts()).thenReturn(5L);
        expectThrows(IllegalStateException.class, () -> ConfigIndexAccess.remoteIndices(tooMany));

        expectThrows(IllegalStateException.class, () -> ConfigIndexAccess.remoteIndices(listingResponse("logs-1")));
    }

    public void testRemoteListingWithoutAggregationsIsEmpty() {
        SearchResponse response = mock(SearchResponse.class);
        assertTrue(ConfigIndexAccess.remoteIndices(response).isEmpty());
    }

    public void testFetchedConfigIsAuthorized() throws IOException {
        returnConfig(TestHelpers.createGetResponse(config(Arrays.asList(DENIED)), CONFIG_ID, ADCommonName.CONFIG_INDEX));
        AtomicBoolean ran = new AtomicBoolean();
        AtomicReference<Exception> failure = new AtomicReference<>();
        authorizeById(failureTo(failure), () -> ran.set(true));
        assertFalse(ran.get());
        assertDenial(failure.get());
        assertEquals(Arrays.asList(DENIED), searchedIndices);
    }

    public void testFetchedConfigTheCallerMayReadRuns() throws IOException {
        returnConfig(TestHelpers.createGetResponse(config(Arrays.asList(READABLE)), CONFIG_ID, ADCommonName.CONFIG_INDEX));
        AtomicBoolean ran = new AtomicBoolean();
        authorizeById(failOnFailure(), () -> ran.set(true));
        assertTrue(ran.get());
        assertEquals(Arrays.asList(READABLE), searchedIndices);
    }

    public void testMissingConfigIsLeftToTheRun() {
        returnConfig(getResponse(false, null));
        AtomicBoolean ran = new AtomicBoolean();
        authorizeById(failOnFailure(), () -> ran.set(true));
        assertTrue(ran.get());
        verify(client, never()).search(any(SearchRequest.class), any());
    }

    public void testUnparsableConfigIsReported() {
        returnConfig(getResponse(true, new BytesArray("[]")));
        AtomicBoolean ran = new AtomicBoolean();
        AtomicReference<Exception> failure = new AtomicReference<>();
        authorizeById(failureTo(failure), () -> ran.set(true));
        assertFalse(ran.get());
        assertNotNull(failure.get());
    }

    public void testMissingConfigIndexIsLeftToTheRun() {
        failConfigGet(new IndexNotFoundException(ADCommonName.CONFIG_INDEX));
        AtomicBoolean ran = new AtomicBoolean();
        authorizeById(failOnFailure(), () -> ran.set(true));
        assertTrue(ran.get());
    }

    public void testConfigGetFailureIsReported() {
        RuntimeException getFailure = new IllegalStateException("get failed");
        failConfigGet(getFailure);
        AtomicBoolean ran = new AtomicBoolean();
        AtomicReference<Exception> failure = new AtomicReference<>();
        authorizeById(failureTo(failure), () -> ran.set(true));
        assertFalse(ran.get());
        assertSame(getFailure, failure.get());
    }

    public void testNoCallerSkipsTheConfigFetch() {
        AtomicBoolean ran = new AtomicBoolean();
        ConfigIndexAccess
            .authorizeCaller(
                client,
                clusterService,
                TestHelpers.xContentRegistry(),
                AnomalyDetector.class,
                CONFIG_ID,
                null,
                stashed(),
                failOnFailure(),
                () -> ran.set(true)
            );
        assertTrue(ran.get());
        verify(client, never()).get(any(GetRequest.class), any());
    }

    private void authorize(List<String> indices, ActionListener<?> listener, Runnable onAuthorized) throws IOException {
        ConfigIndexAccess.authorizeCaller(client, clusterService, config(indices), caller, stashed(), listener, onAuthorized);
    }

    private void authorizeById(ActionListener<?> listener, Runnable onAuthorized) {
        ConfigIndexAccess
            .authorizeCaller(
                client,
                clusterService,
                TestHelpers.xContentRegistry(),
                AnomalyDetector.class,
                CONFIG_ID,
                caller,
                stashed(),
                listener,
                onAuthorized
            );
    }

    private AnomalyDetector config(List<String> indices) throws IOException {
        return TestHelpers.AnomalyDetectorBuilder.newInstance(1).setIndices(indices).setUser(owner).build();
    }

    private void returnConfig(GetResponse response) {
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            listener.onResponse(response);
            return null;
        }).when(client).get(any(GetRequest.class), any());
    }

    private void failConfigGet(Exception e) {
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            listener.onFailure(e);
            return null;
        }).when(client).get(any(GetRequest.class), any());
    }

    private static GetResponse getResponse(boolean exists, BytesArray source) {
        return new GetResponse(
            new GetResult(
                ADCommonName.CONFIG_INDEX,
                CONFIG_ID,
                UNASSIGNED_SEQ_NO,
                0,
                -1,
                exists,
                source,
                Collections.emptyMap(),
                Collections.emptyMap()
            )
        );
    }

    @SuppressWarnings("unchecked")
    private static SearchResponse listingResponse(String... indices) {
        List<Terms.Bucket> buckets = new ArrayList<>();
        for (String index : indices) {
            Terms.Bucket bucket = mock(Terms.Bucket.class);
            when(bucket.getKeyAsString()).thenReturn(index);
            buckets.add(bucket);
        }
        Terms terms = mock(Terms.class);
        doAnswer(invocation -> buckets).when(terms).getBuckets();
        when(terms.getName()).thenReturn("remote_indices");
        Aggregations aggregations = new Aggregations(List.of(terms));
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(aggregations);
        return response;
    }

    private static void assertDenial(Exception failure) {
        assertTrue(failure instanceof OpenSearchStatusException);
        assertEquals(RestStatus.FORBIDDEN, ((OpenSearchStatusException) failure).status());
        assertTrue(failure.getMessage(), failure.getMessage().startsWith("User doesn't have read permissions"));
    }

    private static List<String> sorted(List<String> indices) {
        List<String> copy = new ArrayList<>(indices);
        Collections.sort(copy);
        return copy;
    }

    private static IndexMetadata indexMetadata(String name, String... aliases) {
        IndexMetadata.Builder builder = IndexMetadata
            .builder(name)
            .settings(
                Settings
                    .builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            );
        for (String alias : aliases) {
            builder.putAlias(AliasMetadata.builder(alias).build());
        }
        return builder.build();
    }

    private ThreadContext.StoredContext stashed() {
        return threadContext.stashContext();
    }

    private <T> ActionListener<T> failOnFailure() {
        return ActionListener.wrap(r -> {}, e -> fail("unexpected failure " + e));
    }

    private <T> ActionListener<T> failureTo(AtomicReference<Exception> failure) {
        return ActionListener.wrap(r -> fail("unexpected response"), failure::set);
    }
}
