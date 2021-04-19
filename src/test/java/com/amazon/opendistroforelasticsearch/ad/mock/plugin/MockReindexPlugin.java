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
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.ad.mock.plugin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.Client;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentParser;
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
import org.opensearch.transport.TransportService;

import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.mock.transport.MockAnomalyDetectorJobAction;
import com.amazon.opendistroforelasticsearch.ad.mock.transport.MockAnomalyDetectorJobTransportActionWithUser;
import com.google.common.collect.ImmutableList;

public class MockReindexPlugin extends Plugin implements ActionPlugin {

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays
            .asList(
                new ActionHandler<>(UpdateByQueryAction.INSTANCE, MockTransportUpdateByQueryAction.class),
                new ActionHandler<>(DeleteByQueryAction.INSTANCE, MockTransportDeleteByQueryAction.class),
                new ActionHandler<>(MockAnomalyDetectorJobAction.INSTANCE, MockAnomalyDetectorJobTransportActionWithUser.class)
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
                exception.printStackTrace();
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

        @Override
        protected void doExecute(Task task, DeleteByQueryRequest request, ActionListener<BulkByScrollResponse> listener) {
            try {
                SearchRequest searchRequest = request.getSearchRequest();
                client.search(searchRequest, ActionListener.wrap(r -> {
                    long totalHits = r.getHits().getTotalHits().value;
                    Iterator<SearchHit> iterator = r.getHits().iterator();
                    BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
                    while (iterator.hasNext()) {
                        String id = iterator.next().getId();
                        DeleteRequest deleteRequest = new DeleteRequest(CommonName.DETECTION_STATE_INDEX, id);
                        bulkRequestBuilder.add(deleteRequest);
                    }
                    BulkRequest bulkRequest = bulkRequestBuilder.request().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                    client
                        .execute(
                            BulkAction.INSTANCE,
                            bulkRequest,
                            ActionListener
                                .wrap(
                                    res -> { listener.onResponse(mockBulkByScrollResponse(totalHits)); },
                                    ex -> { listener.onFailure(ex); }
                                )
                        );

                }, e -> { listener.onFailure(e); }));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }

        private BulkByScrollResponse mockBulkByScrollResponse(long totalHits) throws IOException {
            XContentParser parser = TestHelpers
                .parser(
                    "{\"slice_id\":1,\"total\":2,\"updated\":0,\"created\":0,\"deleted\":"
                        + totalHits
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
            return response;
        }
    }
}
