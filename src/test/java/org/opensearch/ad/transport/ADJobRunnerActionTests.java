package org.opensearch.ad.transport;

import org.junit.Assert;
import org.junit.Test;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

public class ADJobRunnerActionTests extends OpenSearchSingleNodeTestCase {

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return getInstanceFromNode(NamedWriteableRegistry.class);
    }

    @Test
    public void testADJobRunnerAction() throws Exception {
        Assert.assertNotNull(ADJobRunnerAction.INSTANCE.name());
        Assert.assertEquals(ADJobRunnerAction.INSTANCE.name(), ADJobRunnerAction.NAME);
    }
}
