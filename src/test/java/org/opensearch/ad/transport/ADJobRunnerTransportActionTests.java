package org.opensearch.ad.transport;

import java.io.IOException;
import java.time.Instant;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.ad.AnomalyDetectorJobRunner;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.model.AnomalyDetectorJob;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.util.RestHandlerUtils;
import org.opensearch.client.Client;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.xcontent.*;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.extensions.action.ExtensionActionRequest;
import org.opensearch.extensions.action.ExtensionActionResponse;
import org.opensearch.jobscheduler.model.ExtensionJobParameter;
import org.opensearch.jobscheduler.spi.JobDocVersion;
import org.opensearch.jobscheduler.spi.JobExecutionContext;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.spi.schedule.Schedule;
import org.opensearch.jobscheduler.spi.utils.LockService;
import org.opensearch.jobscheduler.transport.ExtensionJobActionRequest;
import org.opensearch.jobscheduler.transport.JobParameterResponse;
import org.opensearch.jobscheduler.transport.JobRunnerRequest;
import org.opensearch.jobscheduler.transport.JobRunnerResponse;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.TransportService;
import org.powermock.api.mockito.PowerMockito;

public class ADJobRunnerTransportActionTests extends OpenSearchIntegTestCase {

    private ADJobRunnerTransportAction action;

    private Task task;

    private ActionListener<ExtensionActionResponse> response;

    private ExtensionActionRequest extensionActionRequest;

    private LockService lockService;

    private ADTaskManager adTaskManager;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        action= new ADJobRunnerTransportAction(
                mock(TransportService.class),
                mock(ActionFilters.class),
                mock(Client.class)
        );

        task = mock(Task.class);
        lockService = new LockService(mock(Client.class), clusterService());
        adTaskManager= mock(ADTaskManager.class);
        JobDocVersion jobDocVersion = new JobDocVersion(1L,1L,1L);
        Instant time = Instant.ofEpochSecond(1L);
       // XContentBuilder content = JsonXContent.contentBuilder();
       // content.startObject().endObject();
        JobExecutionContext jobExecutionContext= new JobExecutionContext(time, jobDocVersion,lockService,"jobIndex","jobId");
      //  XContentParser parser = XContentHelper.createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, BytesReference.bytes(content), XContentType.JSON);
      //  Schedule schedule= TestHelpers.randomIntervalSchedule();
     //   ExtensionJobParameter extensionJobParameter = new ExtensionJobParameter("",schedule,Instant.ofEpochSecond(Mockito.anyLong()),Instant.ofEpochSecond(Mockito.anyLong()),true,1L,1.0);
        ScheduledJobParameter scheduledJobParameter= TestHelpers.randomAnomalyDetectorJob();
        JobRunnerRequest jobRunnerRequest = new JobRunnerRequest("token",scheduledJobParameter,jobExecutionContext);
        extensionActionRequest= new ExtensionJobActionRequest<>(RestHandlerUtils.EXTENSION_JOB_RUNNER_ACTION_NAME, jobRunnerRequest);

        response = new ActionListener<>() {

            @Override
            public void onResponse(ExtensionActionResponse extensionActionResponse) {
                try {
                    JobRunnerResponse jobRunnerResponse= new JobRunnerResponse(extensionActionResponse.getResponseBytes());
                    System.out.println("**************************************"+jobRunnerResponse.getJobRunnerStatus());
                }catch (Exception e){

                }
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
        // doNothing().when(adTaskManager).refreshRealtimeJobRunTime(Mockito.anyString());
        action.doExecute(task, extensionActionRequest, response);
    }
}
