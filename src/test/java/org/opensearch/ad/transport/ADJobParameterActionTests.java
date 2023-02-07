package org.opensearch.ad.transport;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.util.RestHandlerUtils;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.xcontent.*;
import org.opensearch.extensions.action.ExtensionActionRequest;
import org.opensearch.extensions.action.ExtensionActionResponse;
import org.opensearch.jobscheduler.model.ExtensionJobParameter;
import org.opensearch.jobscheduler.spi.JobDocVersion;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.transport.ExtensionJobActionRequest;
import org.opensearch.jobscheduler.transport.ExtensionJobActionResponse;
import org.opensearch.jobscheduler.transport.JobParameterRequest;
import org.opensearch.jobscheduler.transport.JobParameterResponse;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

public class ADJobParameterActionTests extends OpenSearchSingleNodeTestCase {
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return getInstanceFromNode(NamedWriteableRegistry.class);
    }

    @Test
    public void testExtensionActionRequest() throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();
        XContentBuilder content = TestHelpers.randomXContent();
        JobDocVersion jobDocVersion = new JobDocVersion(1L, 1L, 1L);
        XContentParser parser = XContentHelper
            .createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, BytesReference.bytes(content), XContentType.JSON);
        JobParameterRequest jobParamRequest = new JobParameterRequest("token", parser, "id", jobDocVersion);
        ExtensionActionRequest request = new ExtensionJobActionRequest<>(
            RestHandlerUtils.EXTENSION_JOB_PARAMETER_ACTION_NAME,
            jobParamRequest
        );

        request.writeTo(out);
        out.flush();
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writableRegistry());
        ExtensionActionRequest newRequest = new ExtensionActionRequest(input);
        Assert.assertEquals(request.getAction(), newRequest.getAction());
        Assert.assertNull(newRequest.validate());
    }

    @Test
    public void testExtensionActionResponse() throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();
        ScheduledJobParameter scheduledJobParameter = TestHelpers.randomAnomalyDetectorJob();
        JobParameterResponse jobParameterResponse = new JobParameterResponse(new ExtensionJobParameter(scheduledJobParameter));
        ExtensionActionResponse response = new ExtensionJobActionResponse<>(jobParameterResponse);
        response.writeTo(out);
        out.flush();
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writableRegistry());
        ExtensionActionResponse newResponse = new ExtensionActionResponse(input);
        Assert.assertEquals(response.getResponseBytes().length, newResponse.getResponseBytes().length);
    }

    @Test
    public void testADJobParameterAction() throws Exception {
        Assert.assertNotNull(ADJobParameterAction.INSTANCE.name());
        Assert.assertEquals(ADJobParameterAction.INSTANCE.name(), ADJobParameterAction.NAME);
    }
}
