/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/* @anomaly.detection - commented until we have support for SDKClusterService.AddLifeCycleListener
package org.opensearch.ad.cluster;


public class ClusterManagerEventListenerTests extends AbstractADTest {
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private Client client;
    private Clock clock;
    private Cancellable hourlyCancellable;
    private Cancellable checkpointIndexRetentionCancellable;
    private ClusterManagerEventListener clusterManagerService;
    private ClientUtil clientUtil;
    private DiscoveryNodeFilterer nodeFilter;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        clusterService = mock(ClusterService.class);
        threadPool = mock(ThreadPool.class);
        hourlyCancellable = mock(Cancellable.class);
        checkpointIndexRetentionCancellable = mock(Cancellable.class);
        when(threadPool.scheduleWithFixedDelay(any(HourlyCron.class), any(TimeValue.class), any(String.class)))
            .thenReturn(hourlyCancellable);
        when(threadPool.scheduleWithFixedDelay(any(ModelCheckpointIndexRetention.class), any(TimeValue.class), any(String.class)))
            .thenReturn(checkpointIndexRetentionCancellable);
        client = mock(Client.class);
        clock = mock(Clock.class);
        clientUtil = mock(ClientUtil.class);
        HashMap<String, String> ignoredAttributes = new HashMap<String, String>();
        ignoredAttributes.put(CommonName.BOX_TYPE_KEY, CommonName.WARM_BOX_TYPE);
        nodeFilter = new DiscoveryNodeFilterer(clusterService);

        clusterManagerService = new ClusterManagerEventListener(clusterService, threadPool, client, clock, clientUtil, nodeFilter);
    }

    public void testOnOffMaster() {
        clusterManagerService.onMaster();
        assertThat(hourlyCancellable, is(notNullValue()));
        assertThat(checkpointIndexRetentionCancellable, is(notNullValue()));
        assertTrue(!clusterManagerService.getHourlyCron().isCancelled());
        assertTrue(!clusterManagerService.getCheckpointIndexRetentionCron().isCancelled());
        clusterManagerService.offMaster();
        assertThat(clusterManagerService.getCheckpointIndexRetentionCron(), is(nullValue()));
        assertThat(clusterManagerService.getHourlyCron(), is(nullValue()));
    }

    public void testBeforeStop() {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(
                String.format(Locale.ROOT, "The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)),
                args.length == 1
            );

            LifecycleListener listener = null;
            if (args[0] instanceof LifecycleListener) {
                listener = (LifecycleListener) args[0];
            }

            assertTrue(listener != null);
            listener.beforeStop();

            return null;
        }).when(clusterService).addLifecycleListener(any());

        clusterManagerService.onMaster();
        assertThat(clusterManagerService.getCheckpointIndexRetentionCron(), is(nullValue()));
        assertThat(clusterManagerService.getHourlyCron(), is(nullValue()));
        clusterManagerService.offMaster();
        assertThat(clusterManagerService.getCheckpointIndexRetentionCron(), is(nullValue()));
        assertThat(clusterManagerService.getHourlyCron(), is(nullValue()));
    }
}
*/
