package org.opensearch.ad.transport;

import org.junit.Assert;
import org.junit.Test;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

public class ADJobParameterActionTests extends OpenSearchSingleNodeTestCase {

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return getInstanceFromNode(NamedWriteableRegistry.class);
    }

    @Test
    public void testADJobParameterAction() throws Exception {
        Assert.assertNotNull(ADJobParameterAction.INSTANCE.name());
        Assert.assertEquals(ADJobParameterAction.INSTANCE.name(), ADJobParameterAction.NAME);
    }
}
