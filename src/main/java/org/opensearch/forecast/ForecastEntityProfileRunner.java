/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast;

import org.opensearch.client.Client;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.forecast.settings.ForecastNumericSetting;
import org.opensearch.forecast.transport.ForecastEntityProfileAction;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.EntityProfileRunner;
import org.opensearch.timeseries.util.SecurityClientUtil;

public class ForecastEntityProfileRunner extends EntityProfileRunner<ForecastEntityProfileAction> {

    public ForecastEntityProfileRunner(
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
            Forecaster::parse,
            ForecastNumericSetting.maxCategoricalFields(),
            AnalysisType.FORECAST,
            ForecastEntityProfileAction.INSTANCE,
            ForecastIndex.RESULT.getIndexName(),
            ForecastCommonName.FORECASTER_ID_KEY,
            ForecastCommonName.CONFIG_INDEX
        );
    }
}
