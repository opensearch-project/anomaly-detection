package org.opensearch.ad.transport;

import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.time.Instant;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
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

    private SDKRestClient sdkRestClient;

    private JobExecutionContext jobExecutionContext;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        sdkRestClient = mock(SDKRestClient.class);
        action = new ADJobRunnerTransportAction(mock(TransportService.class), mock(ActionFilters.class), sdkRestClient);

        task = mock(Task.class);
        lockService = new LockService(mock(Client.class), clusterService());
        JobDocVersion jobDocVersion = new JobDocVersion(1L, 1L, 1L);
        Instant time = Instant.ofEpochSecond(1L);
        jobExecutionContext = new JobExecutionContext(time, jobDocVersion, lockService, "jobIndex", "jobId");
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
        action = new ADJobRunnerTransportAction(mock(TransportService.class), mock(ActionFilters.class), null);
        action.doExecute(task, extensionActionRequest, response);
    }

    @Test
    public void testJobRunnerTransportActionWithNullJobParameterId() throws IOException {
        JobDocVersion jobDocVersion = new JobDocVersion(1L, 1L, 1L);
        Instant time = Instant.ofEpochSecond(1L);
        JobExecutionContext jobExecutionContext = new JobExecutionContext(time, jobDocVersion, lockService, "jobIndex", "jobId");
        JobRunnerRequest jobRunnerRequest = new JobRunnerRequest("token", "", jobExecutionContext);
        extensionActionRequest = new ExtensionJobActionRequest<>(RestHandlerUtils.EXTENSION_JOB_RUNNER_ACTION_NAME, jobRunnerRequest);

        action.doExecute(task, extensionActionRequest, response);
    }

    @Test
    public void testJobRunnerTransportActionWithNullExtensionActionRequest() throws IOException {
        action.doExecute(task, null, response);
    }
}
