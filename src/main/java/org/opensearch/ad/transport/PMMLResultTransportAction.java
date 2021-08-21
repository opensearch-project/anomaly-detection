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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.ad.breaker.ADCircuitBreakerService;
import org.opensearch.ad.common.exception.AnomalyDetectionException;
import org.opensearch.ad.common.exception.LimitExceededException;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.inject.Inject;
import org.opensearch.ml.client.MachineLearningClient;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.dataframe.ColumnMeta;
import org.opensearch.ml.common.dataframe.DataFrame;
import org.opensearch.ml.common.dataframe.DataFrameBuilder;
import org.opensearch.ml.common.dataframe.Row;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class PMMLResultTransportAction extends HandledTransportAction<PMMLResultRequest, PMMLResultResponse> {
    private static final Logger LOG = LogManager.getLogger(PMMLResultTransportAction.class);
    private ADCircuitBreakerService adCircuitBreakerService;
    // AD's client is a node client, but we only cast it here to be used with the ML plugin
    private final NodeClient client;

    // Right now we (only) require/take "outlier" and "decisionFunction" fields out of the pmml model predictions
    private static String PMML_FORMAT = "pmml";
    private static String OUTLIER_FIELD = "outlier";
    private static String DECISION_FUNCTION_FIELD = "decisionFunction";

    @Inject
    public PMMLResultTransportAction(
        ActionFilters actionFilters,
        TransportService transportService,
        ADCircuitBreakerService adCircuitBreakerService,
        NodeClient client
    ) {
        super(PMMLResultAction.NAME, transportService, actionFilters, PMMLResultRequest::new);
        this.adCircuitBreakerService = adCircuitBreakerService;
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, PMMLResultRequest request, ActionListener<PMMLResultResponse> listener) {

        if (adCircuitBreakerService.isOpen()) {
            listener.onFailure(new LimitExceededException(request.getAdID(), CommonErrorMessages.MEMORY_CIRCUIT_BROKEN_ERR_MSG));
            return;
        }

        try {
            // TODO: create a singleton of this ML client in AD plugin initialization
            MachineLearningClient mlClient = new MachineLearningNodeClient(client);
            sendPredictionRequest(mlClient, request, listener);
        } catch (Exception e) {
            LOG.error(e);
            listener.onFailure(e);
        }
    }

    /**
     * utilize the ml client layer to make prediction calls
     */
    private void sendPredictionRequest(
        MachineLearningClient mlClient,
        PMMLResultRequest request,
        ActionListener<PMMLResultResponse> listener
    ) {
        DataFrame inputData = loadDataFrame(request.getFeatureNames(), request.getFeatureValues());
        mlClient.predict(PMML_FORMAT, null, inputData, request.getMlModelID(), ActionListener.wrap(response -> {
            Map<String, Object> output = unloadDataFrame(response);
            listener.onResponse(new PMMLResultResponse((boolean) output.get(OUTLIER_FIELD), (double) output.get(DECISION_FUNCTION_FIELD)));
        }, listener::onFailure));
    }

    /**
     * Loads data frame from array of feature names and array of feature values
     *
     * @return data frame containing features
     */
    private DataFrame loadDataFrame(String[] featureNames, double[] featureValues) throws AnomalyDetectionException {
        if (featureNames.length != featureValues.length) {
            throw new AnomalyDetectionException("features names and features have different lengths");
        }
        List<Map<String, Object>> input = new ArrayList<>();
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < featureValues.length; i++) {
            map.put(featureNames[i], featureValues[i]);
        }
        input.add(map);
        return DataFrameBuilder.load(input);
    }

    /**
     * Unloads data frame to a map of result names and result values. PMML result data frame example:
     * ColumnMeta[] = [ColumnMeta(boolean outlier), ColumnMeta(double decisionFunction)]
     * Row[] = [Row(false, 0.3), Row(true, -0.1)]
     *
     * @return map containing prediction results
     */
    private Map<String, Object> unloadDataFrame(DataFrame results) throws AnomalyDetectionException {
        if (results == null || results.size() == 0) {
            throw new AnomalyDetectionException("null or empty response from the ML plugin");
        }
        Map<String, Object> result = new HashMap<>();
        ColumnMeta[] header = results.columnMetas();
        Row prediction = results.getRow(0);
        for (int i = 0; i < prediction.size(); i++) {
            result.put(header[i].getName(), prediction.getValue(i).getValue());
        }
        if (!result.containsKey(OUTLIER_FIELD) || !result.containsKey(DECISION_FUNCTION_FIELD)) {
            throw new AnomalyDetectionException("response from the ML plugin doesn't contain the required fields");
        }
        return result;
    }
}
