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

package org.opensearch.ad.plugin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.BulkByScrollTask;
import org.opensearch.index.reindex.DeleteByQueryAction;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.index.reindex.UpdateByQueryAction;
import org.opensearch.index.reindex.UpdateByQueryRequest;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.SearchHit;
import org.opensearch.tasks.Task;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

import com.google.common.collect.ImmutableList;

public class MockReindexPlugin extends Plugin implements ActionPlugin {

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays
            .asList(
                new ActionHandler<>(UpdateByQueryAction.INSTANCE, MockTransportUpdateByQueryAction.class),
                new ActionHandler<>(DeleteByQueryAction.INSTANCE, MockTransportDeleteByQueryAction.class)
            );
    }

    public static class MockTransportUpdateByQueryAction extends HandledTransportAction<UpdateByQueryRequest, BulkByScrollResponse> {

        @Inject
        public MockTransportUpdateByQueryAction(ActionFilters actionFilters, TransportService transportService) {
            super(UpdateByQueryAction.NAME, transportService, actionFilters, UpdateByQueryRequest::new);
        }

        @Override
        protected void doExecute(Task task, UpdateByQueryRequest request, ActionListener<BulkByScrollResponse> listener) {
            BulkByScrollResponse response = null;
            try {
                XContentParser parser = TestHelpers
                    .parser(
                        "{\"slice_id\":1,\"total\":2,\"updated\":3,\"created\":0,\"deleted\":0,\"batches\":6,"
                            + "\"version_conflicts\":0,\"noops\":0,\"retries\":{\"bulk\":0,\"search\":10},"
                            + "\"throttled_millis\":0,\"requests_per_second\":13.0,\"canceled\":\"reasonCancelled\","
                            + "\"throttled_until_millis\":14}"
                    );
                parser.nextToken();
                response = new BulkByScrollResponse(
                    TimeValue.timeValueMillis(10),
                    BulkByScrollTask.Status.innerFromXContent(parser),
                    ImmutableList.of(),
                    ImmutableList.of(),
                    false
                );
            } catch (IOException exception) {
                logger.error(exception);
            }
            listener.onResponse(response);
        }
    }

    public static class MockTransportDeleteByQueryAction extends HandledTransportAction<DeleteByQueryRequest, BulkByScrollResponse> {

        private Client client;

        @Inject
        public MockTransportDeleteByQueryAction(ActionFilters actionFilters, TransportService transportService, Client client) {
            super(DeleteByQueryAction.NAME, transportService, actionFilters, DeleteByQueryRequest::new);
            this.client = client;
        }

        private class MultiResponsesActionListener implements ActionListener<DeleteResponse> {
            private final ActionListener<BulkByScrollResponse> delegate;
            private final AtomicInteger collectedResponseCount;
            private final AtomicLong maxResponseCount;
            private final AtomicBoolean hasFailure;

            MultiResponsesActionListener(ActionListener<BulkByScrollResponse> delegate, long maxResponseCount) {
                this.delegate = delegate;
                this.collectedResponseCount = new AtomicInteger(0);
                this.maxResponseCount = new AtomicLong(maxResponseCount);
                this.hasFailure = new AtomicBoolean(false);
            }

            @Override
            public void onResponse(DeleteResponse deleteResponse) {
                if (collectedResponseCount.incrementAndGet() >= maxResponseCount.get()) {
                    finish();
                }
            }

            @Override
            public void onFailure(Exception e) {
                this.hasFailure.set(true);
                if (collectedResponseCount.incrementAndGet() >= maxResponseCount.get()) {
                    finish();
                }
            }

            private void finish() {
                if (this.hasFailure.get()) {
                    this.delegate.onFailure(new RuntimeException("failed to delete old AD tasks"));
                } else {
                    try {
                        XContentParser parser = TestHelpers
                            .parser(
                                "{\"slice_id\":1,\"total\":2,\"updated\":0,\"created\":0,\"deleted\":"
                                    + maxResponseCount
                                    + ",\"batches\":6,\"version_conflicts\":0,\"noops\":0,\"retries\":{\"bulk\":0,"
                                    + "\"search\":10},\"throttled_millis\":0,\"requests_per_second\":13.0,\"canceled\":"
                                    + "\"reasonCancelled\",\"throttled_until_millis\":14}"
                            );
                        parser.nextToken();
                        BulkByScrollResponse response = new BulkByScrollResponse(
                            TimeValue.timeValueMillis(10),
                            BulkByScrollTask.Status.innerFromXContent(parser),
                            ImmutableList.of(),
                            ImmutableList.of(),
                            false
                        );
                        this.delegate.onResponse(response);
                    } catch (IOException exception) {
                        this.delegate.onFailure(new RuntimeException("failed to parse BulkByScrollResponse"));
                    }
                }
            }
        }

        @Override
        protected void doExecute(Task task, DeleteByQueryRequest request, ActionListener<BulkByScrollResponse> listener) {
            SearchRequest searchRequest = request.getSearchRequest();
            client.search(searchRequest, ActionListener.wrap(r -> {
                long totalHits = r.getHits().getTotalHits().value();
                MultiResponsesActionListener delegateListener = new MultiResponsesActionListener(listener, totalHits);
                Iterator<SearchHit> iterator = r.getHits().iterator();
                while (iterator.hasNext()) {
                    String id = iterator.next().getId();
                    DeleteRequest deleteRequest = new DeleteRequest(ADCommonName.DETECTION_STATE_INDEX, id)
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                    client.delete(deleteRequest, delegateListener);
                }
            }, e -> listener.onFailure(e)));
        }
    }
}
