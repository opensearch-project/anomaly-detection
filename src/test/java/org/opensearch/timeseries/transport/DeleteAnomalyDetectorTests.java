/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.transport;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.Version;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.transport.DeleteAnomalyDetectorTransportAction;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.index.get.GetResult;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.tasks.Task;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.function.ExecutorFunction;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.model.Job;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

public class DeleteAnomalyDetectorTests extends AbstractTimeSeriesTest {
    private DeleteAnomalyDetectorTransportAction action;
    private TransportService transportService;
    private ActionFilters actionFilters;
    private Client client;
    private ADTaskManager adTaskManager;
    private PlainActionFuture<DeleteResponse> future;
    private DeleteResponse deleteResponse;
    private GetResponse getResponse;
    ClusterService clusterService;
    private Job jobParameter;
    private NodeStateManager nodeStatemanager;

    @BeforeClass
    public static void setUpBeforeClass() {
        setUpThreadPool(EntityProfileTests.class.getSimpleName());
    }

    @AfterClass
    public static void tearDownAfterClass() {
        tearDownThreadPool();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(AnomalyDetectorSettings.AD_FILTER_BY_BACKEND_ROLES)))
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            null,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet(),
            NoopTracer.INSTANCE
        );

        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);

        actionFilters = mock(ActionFilters.class);
        adTaskManager = mock(ADTaskManager.class);
        nodeStatemanager = mock(NodeStateManager.class);
        action = new DeleteAnomalyDetectorTransportAction(
            transportService,
            actionFilters,
            client,
            clusterService,
            Settings.EMPTY,
            xContentRegistry(),
            nodeStatemanager,
            adTaskManager
        );

        jobParameter = mock(Job.class);
        when(jobParameter.getName()).thenReturn(randomAlphaOfLength(10));
        IntervalSchedule schedule = new IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES);
        when(jobParameter.getSchedule()).thenReturn(schedule);
        when(jobParameter.getWindowDelay()).thenReturn(new IntervalTimeConfiguration(10, ChronoUnit.SECONDS));
    }

    public void testDeleteADTransportAction_FailDeleteResponse() {
        future = mock(PlainActionFuture.class);
        DeleteConfigRequest request = new DeleteConfigRequest("1234", ADIndex.CONFIG.getIndexName());
        setupMocks(true, true, false, false);

        action.doExecute(mock(Task.class), request, future);
        verify(adTaskManager).deleteTasks(eq("1234"), any(), any());
        verify(client, times(1)).delete(any(), any());
        verify(future).onFailure(any(OpenSearchStatusException.class));
    }

    public void testDeleteADTransportAction_NullAnomalyDetector() {
        future = mock(PlainActionFuture.class);
        DeleteConfigRequest request = new DeleteConfigRequest("1234", ADIndex.CONFIG.getIndexName());
        setupMocks(true, false, false, false);

        action.doExecute(mock(Task.class), request, future);
        verify(adTaskManager).deleteTasks(eq("1234"), any(), any());
        verify(client, times(3)).delete(any(), any());
    }

    public void testDeleteADTransportAction_DeleteResponseException() {
        future = mock(PlainActionFuture.class);
        DeleteConfigRequest request = new DeleteConfigRequest("1234", ADIndex.CONFIG.getIndexName());
        setupMocks(true, false, true, false);

        action.doExecute(mock(Task.class), request, future);
        verify(adTaskManager).deleteTasks(eq("1234"), any(), any());
        verify(client, times(1)).delete(any(), any());
        verify(future).onFailure(any(RuntimeException.class));
    }

    public void testDeleteADTransportAction_LatestDetectorLevelTask() {
        when(clusterService.state()).thenReturn(createClusterState());

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            Consumer<Optional<ADTask>> consumer = (Consumer<Optional<ADTask>>) args[2];
            ADTask adTask = ADTask.builder().state("RUNNING").build();
            consumer.accept(Optional.of(adTask));
            return null;
        }).when(adTaskManager).getAndExecuteOnLatestConfigLevelTask(eq("1234"), any(), any(), eq(transportService), eq(true), any());

        future = mock(PlainActionFuture.class);
        DeleteConfigRequest request = new DeleteConfigRequest("1234", ADIndex.CONFIG.getIndexName());
        setupMocks(false, false, false, false);

        action.doExecute(mock(Task.class), request, future);
        verify(future).onFailure(any(OpenSearchStatusException.class));
    }

    public void testDeleteADTransportAction_JobRunning() {
        when(clusterService.state()).thenReturn(createClusterState());
        future = mock(PlainActionFuture.class);
        DeleteConfigRequest request = new DeleteConfigRequest("1234", ADIndex.CONFIG.getIndexName());
        setupMocks(false, false, false, false);

        action.doExecute(mock(Task.class), request, future);
        verify(future).onFailure(any(RuntimeException.class));
    }

    public void testDeleteADTransportAction_GetResponseException() {
        when(clusterService.state()).thenReturn(createClusterState());
        future = mock(PlainActionFuture.class);
        DeleteConfigRequest request = new DeleteConfigRequest("1234", ADIndex.CONFIG.getIndexName());
        setupMocks(false, false, false, true);

        action.doExecute(mock(Task.class), request, future);
        verify(client).get(any(), any());
        verify(client).get(any(), any());
    }

    private ClusterState createClusterState() {
        Map<String, IndexMetadata> immutableOpenMap = new HashMap<>();
        immutableOpenMap
            .put(
                CommonName.JOB_INDEX,
                IndexMetadata
                    .builder("test")
                    .settings(
                        Settings
                            .builder()
                            .put("index.number_of_shards", 1)
                            .put("index.number_of_replicas", 1)
                            .put("index.version.created", Version.CURRENT.id)
                    )
                    .build()
            );
        Metadata metaData = Metadata.builder().indices(immutableOpenMap).build();
        ClusterState clusterState = new ClusterState(
            new ClusterName("test_name"),
            1l,
            "uuid",
            metaData,
            null,
            null,
            null,
            new HashMap<>(),
            1,
            true
        );
        return clusterState;
    }

    private void setupMocks(
        boolean nullAnomalyDetectorResponse,
        boolean failDeleteDeleteResponse,
        boolean deleteResponseException,
        boolean getResponseFailure
    ) {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            Consumer<Optional<AnomalyDetector>> consumer = (Consumer<Optional<AnomalyDetector>>) args[2];
            if (nullAnomalyDetectorResponse) {
                consumer.accept(Optional.empty());
            } else {
                AnomalyDetector ad = mock(AnomalyDetector.class);
                consumer.accept(Optional.of(ad));
            }
            return null;
        }).when(nodeStatemanager).getConfig(any(), any(), any(), any());

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ExecutorFunction function = (ExecutorFunction) args[1];

            function.execute();
            return null;
        }).when(adTaskManager).deleteTasks(eq("1234"), any(), any());

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<DeleteResponse> listener = (ActionListener<DeleteResponse>) args[1];
            deleteResponse = mock(DeleteResponse.class);
            if (deleteResponseException) {
                listener.onFailure(new RuntimeException("Failed to delete anomaly detector job"));
                return null;
            }
            if (failDeleteDeleteResponse) {
                doReturn(DocWriteResponse.Result.CREATED).when(deleteResponse).getResult();
            } else {
                doReturn(DocWriteResponse.Result.DELETED).when(deleteResponse).getResult();
            }
            listener.onResponse(deleteResponse);
            return null;
        }).when(client).delete(any(), any());

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) args[1];
            if (getResponseFailure) {
                listener.onFailure(new RuntimeException("Fail to get anomaly detector job"));
                return null;
            }
            getResponse = new GetResponse(
                new GetResult(
                    CommonName.JOB_INDEX,
                    "id",
                    UNASSIGNED_SEQ_NO,
                    0,
                    -1,
                    true,
                    BytesReference
                        .bytes(
                            new Job(
                                "1234",
                                jobParameter.getSchedule(),
                                jobParameter.getWindowDelay(),
                                true,
                                Instant.now().minusSeconds(60),
                                Instant.now(),
                                Instant.now(),
                                60L,
                                TestHelpers.randomUser(),
                                jobParameter.getCustomResultIndexOrAlias(),
                                AnalysisType.AD
                            ).toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS)
                        ),
                    Collections.emptyMap(),
                    Collections.emptyMap()
                )
            );

            listener.onResponse(getResponse);
            return null;
        }).when(client).get(any(), any());
    }
}
