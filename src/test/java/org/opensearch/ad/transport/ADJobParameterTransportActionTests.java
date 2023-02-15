package org.opensearch.ad.transport;

import static org.mockito.Mockito.*;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.util.RestHandlerUtils;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.extensions.action.ExtensionActionRequest;
import org.opensearch.extensions.action.ExtensionActionResponse;
import org.opensearch.jobscheduler.spi.JobDocVersion;
import org.opensearch.jobscheduler.transport.request.ExtensionJobActionRequest;
import org.opensearch.jobscheduler.transport.request.JobParameterRequest;
import org.opensearch.jobscheduler.transport.response.JobParameterResponse;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.TransportService;

public class ADJobParameterTransportActionTests extends OpenSearchIntegTestCase {

    private ADJobParameterTransportAction action;

    private Task task;

    private ActionListener<ExtensionActionResponse> response;

    private ExtensionActionRequest extensionActionRequest;

    private JobDocVersion jobDocVersion;

    private XContentParser parser;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        action = new ADJobParameterTransportAction(mock(TransportService.class), mock(ActionFilters.class), xContentRegistry());
        task = mock(Task.class);
        jobDocVersion = new JobDocVersion(1L, 1L, 1L);
        response = new ActionListener<>() {

            @Override
            public void onResponse(ExtensionActionResponse extensionActionResponse) {
                assertNotNull(extensionActionResponse);
                try {
                    JobParameterResponse jobParameterResponse = new JobParameterResponse(extensionActionResponse.getResponseBytes());
                    assertNotNull(jobParameterResponse.getJobParameter());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

            }

            @Override
            public void onFailure(Exception e) {
                assertTrue(e instanceof OpenSearchStatusException);
            }
        };
    }

    @Test
    public void testJobParameterTransportAction() throws IOException {
        XContentBuilder content = TestHelpers.randomXContent();
        setExtensionActionRequest(content);
        action.doExecute(task, extensionActionRequest, response);
    }

    @Test
    public void testJobParameterTransportActionWithZeroJobSource() throws IOException {
        XContentBuilder content = JsonXContent.contentBuilder().startObject().endObject();
        setExtensionActionRequest(content);
        action.doExecute(task, extensionActionRequest, response);
    }

    private void setExtensionActionRequest(XContentBuilder content) throws IOException {
        parser = XContentHelper
            .createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, BytesReference.bytes(content), XContentType.JSON);
        JobParameterRequest jobParamRequest = new JobParameterRequest("token", parser, "id", jobDocVersion);
        extensionActionRequest = new ExtensionJobActionRequest<>(RestHandlerUtils.EXTENSION_JOB_PARAMETER_ACTION_NAME, jobParamRequest);
    }

}
