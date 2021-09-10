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

package org.opensearch.ad.transport;

import static org.opensearch.ad.TestHelpers.randomIntervalTimeConfiguration;
import static org.opensearch.ad.TestHelpers.randomQuery;
import static org.opensearch.ad.TestHelpers.randomUser;
import static org.opensearch.ad.model.ADTaskAction.CLEAN_STALE_RUNNING_ENTITIES;

import java.io.IOException;
import java.time.Instant;

import org.opensearch.Version;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.ad.ADUnitTestCase;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.common.exception.ADVersionException;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.settings.AnomalyDetectorSettings;

import com.google.common.collect.ImmutableList;

public class ForwardADTaskRequestTests extends ADUnitTestCase {

    public void testUnsupportedVersion() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableList.of());
        expectThrows(ADVersionException.class, () -> new ForwardADTaskRequest(detector, null, null, null, null, Version.V_1_0_0));
    }

    public void testNullDetectorIdAndTaskAction() throws IOException {
        AnomalyDetector detector = new AnomalyDetector(
            null,
            randomLong(),
            randomAlphaOfLength(20),
            randomAlphaOfLength(30),
            randomAlphaOfLength(5),
            ImmutableList.of(randomAlphaOfLength(10).toLowerCase()),
            ImmutableList.of(),
            randomQuery(),
            randomIntervalTimeConfiguration(),
            randomIntervalTimeConfiguration(),
            randomIntBetween(1, AnomalyDetectorSettings.MAX_SHINGLE_SIZE),
            null,
            randomInt(),
            Instant.now(),
            null,
            randomUser()
        );
        ForwardADTaskRequest request = new ForwardADTaskRequest(detector, null, null, null, null, Version.V_1_1_0);
        ActionRequestValidationException validate = request.validate();
        assertEquals("Validation Failed: 1: AD ID is missing;2: AD task action is missing;", validate.getMessage());
    }

    public void testEmptyStaleEntities() throws IOException {
        ADTask adTask = TestHelpers.randomAdTask();
        ForwardADTaskRequest request = new ForwardADTaskRequest(adTask, CLEAN_STALE_RUNNING_ENTITIES, null);
        ActionRequestValidationException validate = request.validate();
        assertEquals("Validation Failed: 1: Empty stale running entities;", validate.getMessage());
    }
}
