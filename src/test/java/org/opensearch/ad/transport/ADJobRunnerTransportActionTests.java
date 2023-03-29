package org.opensearch.ad.transport;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Instant;

import org.junit.Before;
import org.junit.Test;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.client.Client;
import org.opensearch.extensions.action.ExtensionActionRequest;
import org.opensearch.jobscheduler.spi.JobDocVersion;
import org.opensearch.jobscheduler.spi.JobExecutionContext;
import org.opensearch.jobscheduler.spi.utils.LockService;
import org.opensearch.jobscheduler.transport.request.JobRunnerRequest;
import org.opensearch.jobscheduler.transport.response.JobRunnerResponse;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.SDKClient.SDKRestClient;
import org.opensearch.sdk.SDKNamedXContentRegistry;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;
import org.opensearch.test.OpenSearchIntegTestCase;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class ADJobRunnerTransportActionTests extends OpenSearchIntegTestCase {

    private ADJobRunnerTransportAction action;

    private Task task;

    private ActionListener<JobRunnerResponse> response;

    private ExtensionActionRequest extensionActionRequest;

    private JobRunnerRequest jobRunnerRequest;

    private LockService lockService;

    private SDKRestClient sdkRestClient;

    private JobExecutionContext jobExecutionContext;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        sdkRestClient = mock(SDKRestClient.class);

        ExtensionsRunner extensionsRunner = mock(ExtensionsRunner.class);
        SDKNamedXContentRegistry sdkNamedXContentRegistry = mock(SDKNamedXContentRegistry.class);
        when(extensionsRunner.getNamedXContentRegistry()).thenReturn(sdkNamedXContentRegistry);
        when(sdkNamedXContentRegistry.getRegistry()).thenReturn(xContentRegistry());
        action = new ADJobRunnerTransportAction(
            mock(ActionFilters.class),
            mock(TaskManager.class),
            sdkNamedXContentRegistry,
            sdkRestClient
        );

        task = mock(Task.class);
        lockService = new LockService(mock(Client.class), clusterService());
        JobDocVersion jobDocVersion = new JobDocVersion(1L, 1L, 1L);
        Instant time = Instant.ofEpochSecond(1L);
        jobExecutionContext = new JobExecutionContext(time, jobDocVersion, null, "jobIndex", "jobId");
        jobRunnerRequest = new JobRunnerRequest("token", "jobParameterId", jobExecutionContext);
        response = new ActionListener<JobRunnerResponse>() {

            @Override
            public void onResponse(JobRunnerResponse jobRunnerResponse) {
                assertNotNull(jobRunnerResponse);
                assertEquals(false, jobRunnerResponse.getJobRunnerStatus());
            }

            @Override
            public void onFailure(Exception e) {
                assertTrue(e instanceof OpenSearchStatusException);
            }
        };
    }

    @Test
    public void testJobRunnerTransportAction() {
        action.doExecute(task, jobRunnerRequest, response);
    }

    @Test
    public void testJobRunnerTransportActionWithNullJobParameterId() throws IOException {
        JobDocVersion jobDocVersion = new JobDocVersion(1L, 1L, 1L);
        Instant time = Instant.ofEpochSecond(1L);
        JobExecutionContext jobExecutionContext = new JobExecutionContext(time, jobDocVersion, lockService, "jobIndex", "jobId");
        JobRunnerRequest nullJobParameterIdJobRunnerRequest = new JobRunnerRequest("token", "", jobExecutionContext);
        action.doExecute(task, nullJobParameterIdJobRunnerRequest, response);
    }
}
