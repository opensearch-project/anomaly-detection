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
package org.opensearch.ad.mock.plugin;


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
*/
