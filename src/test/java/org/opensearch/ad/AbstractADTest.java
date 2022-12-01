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

import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.util.StackLocatorUtil;
import org.opensearch.Version;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyDetectorJob;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.model.DetectorInternalState;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.http.HttpRequest;
import org.opensearch.http.HttpResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestRequest.Method;
import org.opensearch.rest.RestStatus;
import org.opensearch.search.SearchModule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportInterceptor;
import org.opensearch.transport.TransportService;

import test.org.opensearch.ad.util.FakeNode;

public class AbstractADTest extends OpenSearchTestCase {

    protected static final Logger LOG = (Logger) LogManager.getLogger(AbstractADTest.class);

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
            if (recordExceptions) {
                exceptions = new HashMap<Class<? extends Throwable>, Map<String, Object>>();
            }
        }

        public List<String> messages = new ArrayList<String>();

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

    Logger logger;

    /**
     * Set up test with junit that a warning was logged with log4j
     */
    protected void setUpLog4jForJUnit(Class<?> cls, boolean recordExceptions) {
        String loggerName = toLoggerName(callerClass(cls));
        logger = (Logger) LogManager.getLogger(loggerName);
        Loggers.setLevel(logger, Level.DEBUG);
        testAppender = new TestAppender(loggerName, recordExceptions);
        testAppender.start();
        logger.addAppender(testAppender);
    }

    protected void setUpLog4jForJUnit(Class<?> cls) {
        setUpLog4jForJUnit(cls, false);
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
        logger.removeAppender(testAppender);
        testAppender.stop();
    }

    protected static void setUpThreadPool(String name) {
        threadPool = new TestThreadPool(
            name,
            new FixedExecutorBuilder(
                Settings.EMPTY,
                AnomalyDetectorPlugin.AD_THREAD_POOL_NAME,
                1,
                1000,
                "opensearch.ad." + AnomalyDetectorPlugin.AD_THREAD_POOL_NAME
            )
        );
    }

    protected static void tearDownThreadPool() {
        LOG.info("tear down threadPool");
        assertTrue(ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS));
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
                        AnomalyDetectorJob.XCONTENT_REGISTRY
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

        when(mockThreadPool.executor(AnomalyDetectorPlugin.AD_THREAD_POOL_NAME)).thenReturn(executorService);
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(executorService).execute(any(Runnable.class));
    }
}
