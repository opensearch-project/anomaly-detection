/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.util;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.security.Principal;

import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ActionType;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.identity.Subject;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

public class PluginClientTests extends OpenSearchTestCase {

    private Client delegate;
    private ThreadContext threadContext;
    private Subject subject;
    private PluginClient pluginClient;

    private static class TestRequest extends ActionRequest {
        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    private static class TestResponse extends ActionResponse {
        public TestResponse() {}

        public TestResponse(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void writeTo(StreamOutput out) { /* no-op */ }
    }

    private static final ActionType<TestResponse> TEST_ACTION = new ActionType<>("test:action", TestResponse::new);

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadContext = new ThreadContext(Settings.EMPTY);

        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        delegate = mock(Client.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
        when(delegate.threadPool()).thenReturn(threadPool);

        subject = mock(Subject.class);

        Principal principal = mock(Principal.class);
        when(principal.getName()).thenReturn("test-subject");
        when(subject.getPrincipal()).thenReturn(principal);

        doAnswer(inv -> {
            CheckedRunnable<?> r = inv.getArgument(0);
            r.run(); // may throw Exception; Mockito will surface it
            return null;
        }).when(subject).runAs(any());

        pluginClient = new PluginClient(delegate, subject);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testThrowsIfSubjectIsNull() {
        pluginClient = new PluginClient(delegate); // subject not set
        IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            () -> pluginClient.execute(TEST_ACTION, new TestRequest(), ActionListener.wrap(r -> {}, e -> {}))
        );
        assertTrue(ex.getMessage().contains("PluginClient is not initialized."));
    }

    public void testDelegatesExecuteAndRunsAsSubject_AndCapturesResponse() {
        TestRequest req = new TestRequest();
        TestResponse resp = new TestResponse();

        // Delegate responds with resp
        doAnswer(inv -> {
            ActionListener<TestResponse> l = inv.getArgument(2);
            l.onResponse(resp);
            return null;
        }).when(delegate).execute(eq(TEST_ACTION), eq(req), any());

        @SuppressWarnings("unchecked")
        ActionListener<TestResponse> userListener = mock(ActionListener.class);

        pluginClient.execute(TEST_ACTION, req, userListener);

        verify(delegate, times(1)).execute(eq(TEST_ACTION), eq(req), any());

        // Verify runAs was used with a CheckedRunnable
        verify(subject, times(1)).runAs(any());

        // Capture and assert the response delivered to user's listener
        ArgumentCaptor<TestResponse> respCaptor = ArgumentCaptor.forClass(TestResponse.class);
        verify(userListener, times(1)).onResponse(respCaptor.capture());
        assertSame(resp, respCaptor.getValue());

        verify(userListener, never()).onFailure(any());
    }

    public void testThreadContextIsRestoredBeforeUserListenerRuns() {
        threadContext.putHeader("foo", "orig");

        // Inside delegate: add a *new* header and prove it disappears after restore
        doAnswer(inv -> {
            ActionListener<TestResponse> l = inv.getArgument(2);

            threadContext.putHeader("bar", "changed");

            l.onResponse(new TestResponse());
            return null;
        }).when(delegate).execute(eq(TEST_ACTION), any(TestRequest.class), any());

        ActionListener<TestResponse> assertingListener = ActionListener.wrap(r -> {
            // ctx::restore ran BEFORE this listener, so:
            assertEquals("orig", threadContext.getHeader("foo"));   // preserved
            assertNull(threadContext.getHeader("bar"));             // reverted (wasn't in original)
        }, e -> fail("Should not fail"));

        pluginClient.execute(TEST_ACTION, new TestRequest(), assertingListener);
    }

    public void testCheckedExceptionFromRunAsIsWrapped() {
        // Make runAs throw a checked Exception
        doAnswer(inv -> { throw new Exception("boom"); }).when(subject).runAs(any());

        RuntimeException ex = expectThrows(
            RuntimeException.class,
            () -> pluginClient.execute(TEST_ACTION, new TestRequest(), ActionListener.wrap(r -> {}, e -> {}))
        );
        assertTrue(ex.getMessage().contains("boom"));
    }

    public void testRuntimeExceptionFromDelegateBubblesUp() {
        doAnswer(inv -> { throw new IllegalStateException("delegate-broke"); })
            .when(delegate)
            .execute(eq(TEST_ACTION), any(TestRequest.class), any());

        IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            () -> pluginClient.execute(TEST_ACTION, new TestRequest(), ActionListener.wrap(r -> {}, e -> {}))
        );
        assertTrue(ex.getMessage().contains("delegate-broke"));
    }
}
