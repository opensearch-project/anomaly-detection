package org.opensearch.ad.transport;

import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.Client;
import org.opensearch.client.ParentTaskAssigningClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.BulkByScrollTask;
import org.opensearch.index.reindex.DeleteByQueryAction;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.script.ScriptService;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

public class TransportDeleteByQueryAction extends HandledTransportAction<DeleteByQueryRequest, BulkByScrollResponse> {

    private final ThreadPool threadPool;
    private final Client client;
    private final ScriptService scriptService;
    private final ClusterService clusterService;

    @Inject
    public TransportDeleteByQueryAction(
            ThreadPool threadPool,
            ActionFilters actionFilters,
            Client client,
            TransportService transportService,
            ScriptService scriptService,
            ClusterService clusterService
    ) {
        super(
                DeleteByQueryAction.NAME,
                transportService,
                actionFilters,
                (Writeable.Reader<DeleteByQueryRequest>) DeleteByQueryRequest::new
        );
        this.threadPool = threadPool;
        this.client = client;
        this.scriptService = scriptService;
        this.clusterService = clusterService;
    }

    @Override
    public void doExecute(Task task, DeleteByQueryRequest request, ActionListener<BulkByScrollResponse> listener) {
        BulkByScrollTask bulkByScrollTask = (BulkByScrollTask) task;
        BulkByScrollParallelizationHelper.startSlicedAction(
                request,
                bulkByScrollTask,
                DeleteByQueryAction.INSTANCE,
                listener,
                client,
                clusterService.localNode(),
                () -> {
                    ParentTaskAssigningClient assigningClient = new ParentTaskAssigningClient(
                            client,
                            clusterService.localNode(),
                            bulkByScrollTask
                    );
                    new AsyncDeleteByQueryAction(bulkByScrollTask, logger, assigningClient, threadPool, request, scriptService, listener)
                            .start();
                }
        );
    }
}
