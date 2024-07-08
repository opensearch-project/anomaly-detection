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

package org.opensearch.timeseries;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.cluster.node.DiscoveryNodeRole.BUILT_IN_ROLES;
import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.util.StackLocatorUtil;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;
import org.opensearch.Version;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.model.DetectorInternalState;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.http.HttpRequest;
import org.opensearch.http.HttpResponse;
import org.opensearch.index.get.GetResult;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestRequest.Method;
import org.opensearch.search.SearchModule;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.ClientUtil;
import org.opensearch.transport.TransportInterceptor;
import org.opensearch.transport.TransportService;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import test.org.opensearch.ad.util.FakeNode;

public class AbstractTimeSeriesTest extends OpenSearchTestCase {

    @Captor
    protected ArgumentCaptor<Exception> exceptionCaptor;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.initMocks(this);
    }

    protected static final Logger LOG = (Logger) LogManager.getLogger(AbstractTimeSeriesTest.class);

    // transport test node
    protected int nodesCount;
    protected FakeNode[] testNodes;

    /**
     * Log4j appender that uses a list to store log messages
     *
     */
    protected class TestAppender extends AbstractAppender {
        private static final String EXCEPTION_CLASS = "exception_class";
        private static final String EXCEPTION_MSG = "exception_message";
        private static final String EXCEPTION_STACK_TRACE = "stacktrace";

        Map<Class<? extends Throwable>, Map<String, Object>> exceptions;
        // whether record exception and its stack trace or not.
        // If you log(msg, exception), by default we won't record exception and its stack trace.
        boolean recordExceptions;

        protected TestAppender(String name) {
            this(name, false);
        }

        protected TestAppender(String name, boolean recordExceptions) {
            super(name, null, PatternLayout.createDefaultLayout(), true, Property.EMPTY_ARRAY);
            this.recordExceptions = recordExceptions;
            this.exceptions = new HashMap<Class<? extends Throwable>, Map<String, Object>>();
        }

        // in case multiple threads writing to the same list.
        // Check ADClusterEventListenerTests.testInProgress.
        public List<String> messages = new CopyOnWriteArrayList<String>();

        public boolean containsMessage(String msg, boolean formatString) {
            Pattern p = null;
            if (formatString) {
                String regex = convertToRegex(msg);
                p = Pattern.compile(regex);
            }
            for (String logMsg : messages) {
                LOG.info(logMsg);
                if (p != null) {
                    Matcher m = p.matcher(logMsg);
                    if (m.matches()) {
                        return true;
                    }
                } else if (logMsg.contains(msg)) {
                    return true;
                }
            }
            return false;
        }

        public boolean containsMessage(String msg) {
            return containsMessage(msg, false);
        }

        public int countMessage(String msg, boolean formatString) {
            Pattern p = null;
            if (formatString) {
                String regex = convertToRegex(msg);
                p = Pattern.compile(regex);
            }
            int count = 0;
            for (String logMsg : messages) {
                LOG.info(logMsg);
                if (p != null) {
                    Matcher m = p.matcher(logMsg);
                    if (m.matches()) {
                        count++;
                    }
                } else if (logMsg.contains(msg)) {
                    count++;
                }
            }
            return count;
        }

        public int countMessage(String msg) {
            return countMessage(msg, false);
        }

        public Boolean containExceptionClass(Class<? extends Throwable> throwable, String className) {
            Map<String, Object> throwableInformation = exceptions.get(throwable);
            return Optional.ofNullable(throwableInformation).map(m -> m.get(EXCEPTION_CLASS)).map(s -> s.equals(className)).orElse(false);
        }

        public Boolean containExceptionMsg(Class<? extends Throwable> throwable, String msg) {
            Map<String, Object> throwableInformation = exceptions.get(throwable);
            return Optional
                .ofNullable(throwableInformation)
                .map(m -> m.get(EXCEPTION_MSG))
                .map(s -> ((String) s).contains(msg))
                .orElse(false);
        }

        public Boolean containExceptionTrace(Class<? extends Throwable> throwable, String traceElement) {
            Map<String, Object> throwableInformation = exceptions.get(throwable);
            return Optional
                .ofNullable(throwableInformation)
                .map(m -> m.get(EXCEPTION_STACK_TRACE))
                .map(s -> ((String) s).contains(traceElement))
                .orElse(false);
        }

        @Override
        public void append(LogEvent event) {
            messages.add(event.getMessage().getFormattedMessage());
            if (recordExceptions && event.getThrown() != null) {
                Map<String, Object> throwableInformation = new HashMap<String, Object>();
                final Throwable throwable = event.getThrown();
                if (throwable.getClass().getCanonicalName() != null) {
                    throwableInformation.put(EXCEPTION_CLASS, throwable.getClass().getCanonicalName());
                }
                if (throwable.getMessage() != null) {
                    throwableInformation.put(EXCEPTION_MSG, throwable.getMessage());
                }
                if (throwable.getMessage() != null) {
                    StringBuilder stackTrace = new StringBuilder(ExceptionUtils.getStackTrace(throwable));
                    throwableInformation.put(EXCEPTION_STACK_TRACE, stackTrace.toString());
                }
                exceptions.put(throwable.getClass(), throwableInformation);
            }
        }

        /**
         * Convert a string with format like "Cannot save %s due to write block."
         *  to a regex with .* like "Cannot save .* due to write block."
         * @return converted regex
         */
        private String convertToRegex(String formattedStr) {
            int percentIndex = formattedStr.indexOf("%");
            return formattedStr.substring(0, percentIndex) + ".*" + formattedStr.substring(percentIndex + 2);
        }

    }

    protected static ThreadPool threadPool;

    protected TestAppender testAppender;

    protected Logger logger;

    protected void setUpLog4jForJUnit(Class<?> cls, boolean recordExceptions) {
        Pair<TestAppender, Logger> appenderAndLogger = getLog4jAppenderForJUnit(cls, recordExceptions);
        testAppender = appenderAndLogger.getLeft();
        logger = appenderAndLogger.getRight();
    }

    protected void setUpLog4jForJUnit(Class<?> cls) {
        setUpLog4jForJUnit(cls, false);
    }

    /**
     * Set up test with junit that a warning was logged with log4j. We can call this method multiple times
     * to track log messages from different class.
     *
     * @param cls class object
     * @param recordExceptions whether we should record exceptions
     */
    protected Pair<TestAppender, Logger> getLog4jAppenderForJUnit(Class<?> cls, boolean recordExceptions) {
        String loggerName = toLoggerName(callerClass(cls));
        Logger logger = (Logger) LogManager.getLogger(loggerName);
        Loggers.setLevel(logger, Level.DEBUG);
        TestAppender appender = new TestAppender(loggerName, recordExceptions);
        appender.start();
        logger.addAppender(appender);
        return Pair.of(appender, logger);
    }

    private static String toLoggerName(final Class<?> cls) {
        String canonicalName = cls.getCanonicalName();
        return canonicalName != null ? canonicalName : cls.getName();
    }

    private static Class<?> callerClass(final Class<?> clazz) {
        if (clazz != null) {
            return clazz;
        }
        final Class<?> candidate = StackLocatorUtil.getCallerClass(3);
        if (candidate == null) {
            throw new UnsupportedOperationException("No class provided, and an appropriate one cannot be found.");
        }
        return candidate;
    }

    /**
     * remove the appender
     */
    protected void tearDownLog4jForJUnit() {
        tearDownLog4jForJUnit(testAppender, logger);
    }

    protected void tearDownLog4jForJUnit(TestAppender appender, Logger logger) {
        logger.removeAppender(appender);
        appender.stop();
    }

    protected static void setUpThreadPool(String name) {
        threadPool = new TestThreadPool(
            name,
            new FixedExecutorBuilder(
                Settings.EMPTY,
                TimeSeriesAnalyticsPlugin.AD_THREAD_POOL_NAME,
                1,
                1000,
                "opensearch.ad." + TimeSeriesAnalyticsPlugin.AD_THREAD_POOL_NAME
            )
        );
    }

    protected static void tearDownThreadPool() {
        LOG.info("tear down threadPool ", threadPool);
        assertTrue(ThreadPool.terminate(threadPool, 60, TimeUnit.SECONDS));
        threadPool = null;
    }

    /**
     *
     * @param transportInterceptor Interceptor to for transport requests. Used
     *  to mock transport layer.
     * @param nodeSettings node override of setting
     * @param setting the supported setting set.
     */
    public void setupTestNodes(TransportInterceptor transportInterceptor, final Settings nodeSettings, Setting<?>... setting) {
        setupTestNodes(transportInterceptor, randomIntBetween(2, 10), nodeSettings, Version.CURRENT, setting);
    }

    /**
    *
    * @param transportInterceptor Interceptor to for transport requests. Used
    *  to mock transport layer.
    * @param numberOfNodes number of nodes in the cluster
    * @param nodeSettings node override of setting
    * @param setting the supported setting set.
    */
    public void setupTestNodes(
        TransportInterceptor transportInterceptor,
        int numberOfNodes,
        final Settings nodeSettings,
        Version version,
        Setting<?>... setting
    ) {
        nodesCount = numberOfNodes;
        testNodes = new FakeNode[nodesCount];
        Set<Setting<?>> settingSet = new HashSet<>(Arrays.asList(setting));
        for (int i = 0; i < testNodes.length; i++) {
            testNodes[i] = new FakeNode("node" + i, threadPool, nodeSettings, settingSet, transportInterceptor, version);
        }
        FakeNode.connectNodes(testNodes);
    }

    public void setupTestNodes(Setting<?>... setting) {
        setupTestNodes(TransportService.NOOP_TRANSPORT_INTERCEPTOR, Settings.EMPTY, setting);
    }

    public void setupTestNodes(Settings nodeSettings) {
        setupTestNodes(TransportService.NOOP_TRANSPORT_INTERCEPTOR, nodeSettings);
    }

    public void setupTestNodes(TransportInterceptor transportInterceptor) {
        setupTestNodes(transportInterceptor, Settings.EMPTY);
    }

    public void tearDownTestNodes() {
        if (testNodes == null) {
            return;
        }
        for (FakeNode testNode : testNodes) {
            testNode.close();
        }
        testNodes = null;
    }

    public void assertException(
        PlainActionFuture<? extends ActionResponse> listener,
        Class<? extends Exception> exceptionType,
        String msg
    ) {
        Exception e = expectThrows(exceptionType, () -> listener.actionGet(20_000));
        assertThat("actual message: " + e.getMessage(), e.getMessage(), containsString(msg));
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        List<NamedXContentRegistry.Entry> entries = searchModule.getNamedXContents();
        entries
            .addAll(
                Arrays
                    .asList(
                        AnomalyDetector.XCONTENT_REGISTRY,
                        AnomalyResult.XCONTENT_REGISTRY,
                        DetectorInternalState.XCONTENT_REGISTRY,
                        Job.XCONTENT_REGISTRY
                    )
            );
        return new NamedXContentRegistry(entries);
    }

    protected RestRequest createRestRequest(Method method) {
        return RestRequest.request(xContentRegistry(), new HttpRequest() {

            @Override
            public Method method() {
                return method;
            }

            @Override
            public String uri() {
                return "/";
            }

            @Override
            public BytesReference content() {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public Map<String, List<String>> getHeaders() {
                return new HashMap<>();
            }

            @Override
            public List<String> strictCookies() {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public HttpVersion protocolVersion() {
                return HttpRequest.HttpVersion.HTTP_1_1;
            }

            @Override
            public HttpRequest removeHeader(String header) {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public HttpResponse createResponse(RestStatus status, BytesReference content) {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public Exception getInboundException() {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public void release() {
                // TODO Auto-generated method stub

            }

            @Override
            public HttpRequest releaseAndCopy() {
                // TODO Auto-generated method stub
                return null;
            }

        }, null);
    }

    protected IndexMetadata indexMeta(String name, long creationDate, String... aliases) {
        IndexMetadata.Builder builder = IndexMetadata
            .builder(name)
            .settings(
                Settings
                    .builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 1)
                    .put("index.version.created", Version.CURRENT.id)
            );
        builder.creationDate(creationDate);
        for (String alias : aliases) {
            builder.putAlias(AliasMetadata.builder(alias).build());
        }
        return builder.build();
    }

    protected void setUpADThreadPool(ThreadPool mockThreadPool) {
        ExecutorService executorService = mock(ExecutorService.class);

        when(mockThreadPool.executor(TimeSeriesAnalyticsPlugin.AD_THREAD_POOL_NAME)).thenReturn(executorService);
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(executorService).execute(any(Runnable.class));
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

    protected void setupGetDetector(AnomalyDetector detector, Client client) {
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            listener
                .onResponse(
                    new GetResponse(
                        new GetResult(
                            CommonName.CONFIG_INDEX,
                            detector.getId(),
                            UNASSIGNED_SEQ_NO,
                            0,
                            -1,
                            true,
                            BytesReference.bytes(detector.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS)),
                            Collections.emptyMap(),
                            Collections.emptyMap()
                        )
                    )
                );
            return null;
        }).when(client).get(any(), any());
    }

    protected DiscoveryNode createDiscoverynode(String nodeName) {
        return new DiscoveryNode(
            nodeName,
            OpenSearchTestCase.buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );
    }

    protected ClusterService createClusterServiceForNode(
        ThreadPool threadPool,
        DiscoveryNode discoveryNode,
        Set<Setting<?>> nodestateSetting
    ) {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, nodestateSetting);

        return ClusterServiceUtils.createClusterService(threadPool, discoveryNode, clusterSettings);
    }

    protected NodeStateManager createNodeStateManager(
        Client client,
        ClientUtil clientUtil,
        ThreadPool threadPool,
        ClusterService clusterService
    ) {
        return new NodeStateManager(
            client,
            xContentRegistry(),
            Settings.EMPTY,
            clientUtil,
            mock(Clock.class),
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            clusterService,
            TimeSeriesSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE,
            TimeSeriesSettings.BACKOFF_MINUTES
        );
    }
}
