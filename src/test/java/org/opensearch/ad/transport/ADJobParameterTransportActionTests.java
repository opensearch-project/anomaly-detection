package org.opensearch.ad.transport;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import static org.mockito.Mockito.*;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.util.RestHandlerUtils;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.xcontent.*;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.extensions.action.ExtensionActionRequest;
import org.opensearch.extensions.action.ExtensionActionResponse;
import org.opensearch.jobscheduler.transport.ExtensionJobActionRequest;
import org.opensearch.jobscheduler.transport.ExtensionJobActionResponse;
import org.opensearch.jobscheduler.transport.JobParameterRequest;
import org.opensearch.jobscheduler.transport.JobParameterResponse;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.TransportService;
import org.opensearch.jobscheduler.spi.JobDocVersion;

public class ADJobParameterTransportActionTests extends OpenSearchIntegTestCase {

    private ADJobParameterTransportAction action;

    private Task task;

    private ActionListener<ExtensionActionResponse> response;

    private ExtensionActionRequest extensionActionRequest;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        action= new ADJobParameterTransportAction(
                mock(TransportService.class),
                mock(ActionFilters.class),
                xContentRegistry()
        );
        task = mock(Task.class);
        JobDocVersion jobDocVersion = new JobDocVersion(1L,1L,1L);
        XContentBuilder content = JsonXContent.contentBuilder();
        content.startObject();
        content.endObject();

        XContentParser parser = XContentHelper.createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, BytesReference.bytes(content), XContentType.JSON);
        JobParameterRequest jobParamRequest = new JobParameterRequest("token",parser,"id",jobDocVersion);
        extensionActionRequest= new ExtensionJobActionRequest<>(RestHandlerUtils.EXTENSION_JOB_PARAMETER_ACTION_NAME, jobParamRequest);
        response = new ActionListener<>() {

            @Override
            public void onResponse(ExtensionActionResponse extensionActionResponse) {
                try {
                    JobParameterResponse jobParameterResponse= new JobParameterResponse(extensionActionResponse.getResponseBytes());
                    System.out.println(jobParameterResponse);
                }catch (Exception e){

                }
                Assert.assertTrue(false);
            }

            @Override
            public void onFailure(Exception e) {
                Assert.assertTrue(true);
            }
        };
    }

    @Test
    public void testJobParameterTransportAction() {
        action.doExecute(task, extensionActionRequest, response);
    }
}
