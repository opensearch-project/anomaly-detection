package org.opensearch.ad.transport;

import static org.mockito.Mockito.mock;

import java.time.Instant;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.ad.util.RestHandlerUtils;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.extensions.action.ExtensionActionRequest;
import org.opensearch.extensions.action.ExtensionActionResponse;
import org.opensearch.jobscheduler.spi.JobDocVersion;
import org.opensearch.jobscheduler.spi.JobExecutionContext;
import org.opensearch.jobscheduler.spi.utils.LockService;
import org.opensearch.jobscheduler.transport.request.ExtensionJobActionRequest;
import org.opensearch.jobscheduler.transport.request.JobRunnerRequest;
import org.opensearch.jobscheduler.transport.response.JobRunnerResponse;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

public class ADJobRunnerActionTests extends OpenSearchSingleNodeTestCase {

    private LockService lockService;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        lockService = new LockService(mock(Client.class), mock(ClusterService.class));

    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return getInstanceFromNode(NamedWriteableRegistry.class);
    }

    @Test
    public void testExtensionActionRequest() throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();
        JobDocVersion jobDocVersion = new JobDocVersion(1L, 1L, 1L);
        Instant time = Instant.ofEpochSecond(1L);
        JobExecutionContext jobExecutionContext = new JobExecutionContext(time, jobDocVersion, lockService, "jobIndex", "jobId");
        JobRunnerRequest jobRunnerRequest = new JobRunnerRequest("token", "jobParameterId", jobExecutionContext);
        ExtensionActionRequest request = new ExtensionJobActionRequest<>(
            RestHandlerUtils.EXTENSION_JOB_RUNNER_ACTION_NAME,
            jobRunnerRequest
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

        JobRunnerResponse jobRunnerResponse = new JobRunnerResponse(true);

        // Convert response to bytes
        BytesStreamOutput jobRunnerResponseOutput = new BytesStreamOutput();
        jobRunnerResponse.writeTo(jobRunnerResponseOutput);
        jobRunnerResponseOutput.flush();
        byte[] bytes = BytesReference.toBytes(jobRunnerResponseOutput.bytes());

        // Create ExtensionActionResponse
        BytesStreamOutput extensionActionResponseOutput = new BytesStreamOutput();
        ExtensionActionResponse response = new ExtensionActionResponse(bytes);
        response.writeTo(extensionActionResponseOutput);
        extensionActionResponseOutput.flush();
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(
            extensionActionResponseOutput.bytes().streamInput(),
            writableRegistry()
        );
        ExtensionActionResponse newResponse = new ExtensionActionResponse(input);
        Assert.assertEquals(response.getResponseBytes().length, newResponse.getResponseBytes().length);
    }

    @Test
    public void testADJobRunnerAction() throws Exception {
        Assert.assertNotNull(ADJobRunnerAction.INSTANCE.name());
        Assert.assertEquals(ADJobRunnerAction.INSTANCE.name(), ADJobRunnerAction.NAME);
    }
}
