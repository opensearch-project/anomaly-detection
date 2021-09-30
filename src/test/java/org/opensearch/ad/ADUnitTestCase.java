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

package org.opensearch.ad;

import static org.opensearch.cluster.node.DiscoveryNodeRole.BUILT_IN_ROLES;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;
import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.TransportAddress;
import org.opensearch.test.OpenSearchTestCase;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

public class ADUnitTestCase extends OpenSearchTestCase {

    @Captor
    protected ArgumentCaptor<Exception> exceptionCaptor;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.initMocks(this);
    }

    /**
     * Create cluster setting.
     *
     * @param settings cluster settings
     * @param setting add setting if the code to be tested contains setting update consumer
     * @return instance of ClusterSettings
     */
    public ClusterSettings clusterSetting(Settings settings, Setting<?>... setting) {
        final Set<Setting<?>> settingsSet = Stream
            .concat(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(), Sets.newHashSet(setting).stream())
            .collect(Collectors.toSet());
        ClusterSettings clusterSettings = new ClusterSettings(settings, settingsSet);
        return clusterSettings;
    }

    protected DiscoveryNode createNode(String nodeId) {
        return new DiscoveryNode(
            nodeId,
            new TransportAddress(TransportAddress.META_ADDRESS, 9300),
            ImmutableMap.of(),
            BUILT_IN_ROLES,
            Version.CURRENT
        );
    }

    protected DiscoveryNode createNode(String nodeId, String ip, int port, Map<String, String> attributes) throws UnknownHostException {
        return new DiscoveryNode(
            nodeId,
            new TransportAddress(InetAddress.getByName(ip), port),
            attributes,
            BUILT_IN_ROLES,
            Version.CURRENT
        );
    }
}
