package org.opensearch.ad.transport;

import static org.mockito.Mockito.mock;

import java.time.Instant;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.ad.AnomalyDetectorJobRunner;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.util.RestHandlerUtils;
import org.opensearch.client.Client;
import org.opensearch.extensions.action.ExtensionActionRequest;
import org.opensearch.extensions.action.ExtensionActionResponse;
import org.opensearch.jobscheduler.spi.JobDocVersion;
import org.opensearch.jobscheduler.spi.JobExecutionContext;
import org.opensearch.jobscheduler.spi.utils.LockService;
import org.opensearch.jobscheduler.transport.ExtensionJobActionRequest;
import org.opensearch.jobscheduler.transport.JobRunnerRequest;
import org.opensearch.sdk.SDKClient.SDKRestClient;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.TransportService;

public class ADJobRunnerTransportActionTests extends OpenSearchIntegTestCase {

    private ADJobRunnerTransportAction action;

    private Task task;

    private ActionListener<ExtensionActionResponse> response;

    private ExtensionActionRequest extensionActionRequest;

    private LockService lockService;

    private ADTaskManager adTaskManager;

    private SDKRestClient sdkRestClient;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        sdkRestClient = mock(SDKRestClient.class);
        action = new ADJobRunnerTransportAction(mock(TransportService.class), mock(ActionFilters.class), sdkRestClient);

        task = mock(Task.class);
        lockService = new LockService(mock(Client.class), clusterService());
        adTaskManager = mock(ADTaskManager.class);
        JobDocVersion jobDocVersion = new JobDocVersion(1L, 1L, 1L);
        Instant time = Instant.ofEpochSecond(1L);
        JobExecutionContext jobExecutionContext = new JobExecutionContext(time, jobDocVersion, lockService, "jobIndex", "jobId");
        JobRunnerRequest jobRunnerRequest = new JobRunnerRequest("token", "jobParameterId", jobExecutionContext);
        extensionActionRequest = new ExtensionJobActionRequest<>(RestHandlerUtils.EXTENSION_JOB_RUNNER_ACTION_NAME, jobRunnerRequest);

        response = new ActionListener<>() {

            @Override
            public void onResponse(ExtensionActionResponse extensionActionResponse) {
                Assert.assertFalse(true);
            }

            @Override
            public void onFailure(Exception e) {
                Assert.assertTrue(true);
            }
        };
    }

    @Test
    public void testJobRunnerTransportAction() {
        AnomalyDetectorJobRunner.getJobRunnerInstance().setAdTaskManager(adTaskManager);
        Mockito.doNothing().when(adTaskManager).refreshRealtimeJobRunTime(Mockito.anyString());
        action.doExecute(task, extensionActionRequest, response);
    }
}
