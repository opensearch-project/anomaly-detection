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

/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.ad.transport;

import java.io.IOException;

import org.junit.Test;
import org.opensearch.ad.AbstractADTest;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.model.DetectorValidationIssue;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;

public class ValidateAnomalyDetectorResponseTests extends AbstractADTest {

    @Test
    public void testResponseSerialization() throws IOException {
        DetectorValidationIssue issue = TestHelpers.randomDetectorValidationIssue();
        ValidateAnomalyDetectorResponse response = new ValidateAnomalyDetectorResponse(issue);
        BytesStreamOutput output = new BytesStreamOutput();
        response.writeTo(output);

        StreamInput streamInput = output.bytes().streamInput();
        ValidateAnomalyDetectorResponse readResponse = ValidateAnomalyDetectorAction.INSTANCE.getResponseReader().read(streamInput);

        assertEquals("serialization has the wrong issue", issue, readResponse.getIssue());
    }

    @Test
    public void testResponseSerializationWithEmptyIssue() throws IOException {
        ValidateAnomalyDetectorResponse response = new ValidateAnomalyDetectorResponse((DetectorValidationIssue) null);
        BytesStreamOutput output = new BytesStreamOutput();
        response.writeTo(output);

        StreamInput streamInput = output.bytes().streamInput();
        ValidateAnomalyDetectorResponse readResponse = ValidateAnomalyDetectorAction.INSTANCE.getResponseReader().read(streamInput);

        assertNull("serialization should have empty issue", readResponse.getIssue());
    }
}
