/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.exception;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.common.exception.TimeSeriesException;

/** Exercises every reflection branch in TimeSeriesException#cloneWithMsgPrefix. */
public class TimeSeriesExceptionCloneTests extends OpenSearchTestCase {

    /* ------------ helper subclasses used in the tests ---------------- */

    /** Missing the required 3-arg ctor → triggers NoSuchMethodException path. */
    public static class BadTimeSeriesException extends TimeSeriesException {
        public BadTimeSeriesException(String msg) {
            super(msg);
        }
    }

    /** Has the required ctor but that ctor throws → triggers generic Exception path. */
    public static class ExplodingTimeSeriesException extends TimeSeriesException {
        public ExplodingTimeSeriesException(String msg) {
            super(msg);
        }

        /** Required (cfg,msg,cause) ctor – deliberately explodes. */
        public ExplodingTimeSeriesException(String cfg, String msg, Throwable t) {
            super(cfg, msg, t);
            throw new IllegalArgumentException("BOOM");
        }
    }

    /** Well-behaved subclass used to verify addSuppressed copy. */
    public static class GoodTimeSeriesException extends TimeSeriesException {
        public GoodTimeSeriesException(String cfg, String msg, Throwable t) {
            super(cfg, msg, t);
        }

        public GoodTimeSeriesException(String msg) {
            super(msg);
        }
    }

    /* ------------------------------------------------------------------
        NoSuchMethodException branch -> IllegalStateException
       ------------------------------------------------------------------ */
    public void testCloneMissingCtor_throwsIllegalStateException() {
        TimeSeriesException ex = new BadTimeSeriesException("orig");

        IllegalStateException ise = expectThrows(IllegalStateException.class, () -> ex.cloneWithMsgPrefix("P:"));

        assertTrue(
            "actual: " + ise.getMessage(),
            ise.getMessage().contains("must provide a (String configId, String message, Throwable cause) constructor")
        );
    }

    /* ------------------------------------------------------------------
       Constructor exists but throws -> RuntimeException("Failed to clone …")
       ------------------------------------------------------------------ */
    public void testCloneCtorThrows_throwsRuntimeException() {
        TimeSeriesException ex = new ExplodingTimeSeriesException("orig");

        RuntimeException rte = expectThrows(RuntimeException.class, () -> ex.cloneWithMsgPrefix("P:"));

        assertTrue("actual: " + rte.getMessage(), rte.getMessage().startsWith("Failed to clone exception"));

        /* unwrap InvocationTargetException to reach the real root cause */
        Throwable lvl1 = rte.getCause();                // InvocationTargetException
        assertNotNull("First-level cause is null", lvl1);

        Throwable root = lvl1.getCause();               // IllegalArgumentException("BOOM")
        assertNotNull("Root cause is null", root);
        assertTrue("root msg: " + root.getMessage(), root.getMessage().contains("BOOM"));
    }

    /* ------------------------------------------------------------------
       Suppressed exceptions copied via addSuppressed
       ------------------------------------------------------------------ */
    public void testSuppressedExceptionsAreCopied() {
        GoodTimeSeriesException src = new GoodTimeSeriesException("cfg-1", "orig", null);
        IllegalStateException suppressed = new IllegalStateException("supp");
        src.addSuppressed(suppressed);

        TimeSeriesException clone = src.cloneWithMsgPrefix("P:");

        assertEquals("Suppressed count mismatch", 1, clone.getSuppressed().length);
        assertSame("Suppressed throwable not copied", suppressed, clone.getSuppressed()[0]);
    }
}
