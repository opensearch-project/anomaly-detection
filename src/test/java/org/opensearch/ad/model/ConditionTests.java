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

package org.opensearch.ad.model;

import java.io.IOException;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

public class ConditionTests extends OpenSearchTestCase {

    public void testSerializeNullableOperator() throws IOException {
        Condition condition = new Condition("feature", ThresholdType.ACTUAL_IS_OVER_EXPECTED, null, null);

        Condition deserializedCondition = copyCondition(condition);

        assertEquals(condition, deserializedCondition);
        assertNull(deserializedCondition.getOperator());
    }

    public void testSerializeOperator() throws IOException {
        Condition condition = new Condition("feature", ThresholdType.ACTUAL_OVER_EXPECTED_RATIO, Operator.LTE, 0.5);

        Condition deserializedCondition = copyCondition(condition);

        assertEquals(condition, deserializedCondition);
        assertEquals(Operator.LTE, deserializedCondition.getOperator());
    }

    private Condition copyCondition(Condition condition) throws IOException {
        BytesStreamOutput output = new BytesStreamOutput();
        condition.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        return new Condition(input);
    }
}
