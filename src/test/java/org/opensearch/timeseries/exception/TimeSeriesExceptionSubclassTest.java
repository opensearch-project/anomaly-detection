/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.exception;

import java.lang.reflect.Constructor;
import java.util.Set;

import org.junit.jupiter.api.Assertions;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.reflections.Reflections;

public class TimeSeriesExceptionSubclassTest extends OpenSearchTestCase {

    public void testAllSubclassesHaveProperConstructorAndNoIllegalState() throws Exception {
        // Adjust the package name as needed
        Reflections reflections = new Reflections("org.opensearch.timeseries.common.exception");
        Set<Class<? extends TimeSeriesException>> subclasses = reflections.getSubTypesOf(TimeSeriesException.class);

        for (Class<? extends TimeSeriesException> subclass : subclasses) {
            // Skip abstract classes or classes that cannot be instantiated
            if (java.lang.reflect.Modifier.isAbstract(subclass.getModifiers())) {
                continue;
            }

            // We need to verify that (String configId, String message, Throwable cause) constructor exists
            Constructor<? extends TimeSeriesException> ctor;
            try {
                ctor = subclass.getConstructor(String.class, String.class, Throwable.class);
            } catch (NoSuchMethodException e) {
                Assertions
                    .fail(
                        "Subclass " + subclass.getName() + " must have a (String configId, String message, Throwable cause) constructor."
                    );
                continue;
            }

            // Create an instance using known arguments
            TimeSeriesException instance = ctor.newInstance("testConfigId", "testMessage", null);

            // Attempt to clone with a message prefix.
            // If the subclass is missing required constructor logic, it should fail here.
            Assertions.assertDoesNotThrow(() -> {
                TimeSeriesException cloned = instance.cloneWithMsgPrefix("Prefix: ");
                Assertions.assertNotNull(cloned);
                // Optionally verify that the cloned message has the prefix
                Assertions.assertTrue(cloned.getMessage().startsWith("Prefix: testMessage"));
            }, "Cloning failed for subclass: " + subclass.getName());
        }
    }
}
