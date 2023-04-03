package org.opensearch.ad.transport;

import static org.mockito.Mockito.*;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.ad.TestHelpers;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.jobscheduler.spi.JobDocVersion;
import org.opensearch.jobscheduler.transport.request.JobParameterRequest;
import org.opensearch.jobscheduler.transport.response.JobParameterResponse;
import org.opensearch.sdk.SDKNamedXContentRegistry;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;
import org.opensearch.test.OpenSearchIntegTestCase;

public class ADJobParameterTransportActionTests extends OpenSearchIntegTestCase {

    private ADJobParameterTransportAction action;

    private Task task;

    private ActionListener<JobParameterResponse> response;

    private JobParameterRequest jobParameterRequest;

    private JobDocVersion jobDocVersion;

    private XContentParser parser;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        action = new ADJobParameterTransportAction(
            mock(ActionFilters.class),
            mock(TaskManager.class),
            mock(SDKNamedXContentRegistry.class)
        );
        task = mock(Task.class);
        jobDocVersion = new JobDocVersion(1L, 1L, 1L);
        response = new ActionListener<JobParameterResponse>() {

            @Override
            public void onResponse(JobParameterResponse jobParameterResponse) {
                assertNotNull(jobParameterResponse);
                assertNotNull(jobParameterResponse.getJobParameter());
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
        setJobParameterRequest(content);
        action.doExecute(task, jobParameterRequest, response);
    }

    @Test
    public void testJobParameterTransportActionWithZeroJobSource() throws IOException {
        XContentBuilder content = JsonXContent.contentBuilder().startObject().endObject();
        setJobParameterRequest(content);
        action.doExecute(task, jobParameterRequest, response);
    }

    private void setJobParameterRequest(XContentBuilder content) throws IOException {
        parser = XContentHelper
            .createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, BytesReference.bytes(content), XContentType.JSON);
        jobParameterRequest = new JobParameterRequest("token", parser, "id", jobDocVersion);
    }

}
