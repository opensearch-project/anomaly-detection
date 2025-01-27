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

import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.settings.ADNumericSetting;
import org.opensearch.ad.transport.ADEntityProfileAction;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.EntityProfileRunner;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.client.Client;

public class ADEntityProfileRunner extends EntityProfileRunner<ADEntityProfileAction> {

    public ADEntityProfileRunner(
        Client client,
        SecurityClientUtil clientUtil,
        NamedXContentRegistry xContentRegistry,
        long requiredSamples
    ) {
        super(
            client,
            clientUtil,
            xContentRegistry,
            requiredSamples,
            AnomalyDetector::parse,
            ADNumericSetting.maxCategoricalFields(),
            AnalysisType.AD,
            ADEntityProfileAction.INSTANCE,
            ADCommonName.ANOMALY_RESULT_INDEX_ALIAS,
            AnomalyResult.DETECTOR_ID_FIELD,
            ADCommonName.CONFIG_INDEX
        );
    }
}
