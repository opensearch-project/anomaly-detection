/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.transport;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.opensearch.Version;
import org.opensearch.ad.constant.ADCommonMessages;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.core.action.ActionListener;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.common.exception.InternalFailure;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.FeatureData;
import org.opensearch.timeseries.transport.ResultProcessor;

public class RCFResultUnitTests extends AbstractTimeSeriesTest {
    private String adID = "123";

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        setUpLog4jForJUnit(AbstractTimeSeriesTest.class);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        tearDownLog4jForJUnit();
        super.tearDown();
    }

    // For single stream detector
    class RCFActionListener implements ActionListener<RCFResultResponse> {
        private String modelID;
        private AtomicReference<Exception> failure;
        private String rcfNodeID;
        private Config detector;
        private ActionListener<AnomalyResultResponse> listener;
        private List<FeatureData> featureInResponse;
        private final String adID;

        RCFActionListener(
            String modelID,
            AtomicReference<Exception> failure,
            String rcfNodeID,
            Config detector,
            ActionListener<AnomalyResultResponse> listener,
            List<FeatureData> features,
            String adID
        ) {
            this.modelID = modelID;
            this.failure = failure;
            this.rcfNodeID = rcfNodeID;
            this.detector = detector;
            this.listener = listener;
            this.featureInResponse = features;
            this.adID = adID;
        }

        @Override
        public void onResponse(RCFResultResponse response) {
            try {
                if (response != null) {
                    listener
                        .onResponse(
                            new AnomalyResultResponse(
                                response.getAnomalyGrade(),
                                response.getConfidence(),
                                response.getRCFScore(),
                                featureInResponse,
                                null,
                                response.getTotalUpdates(),
                                detector.getIntervalInMinutes(),
                                false,
                                response.getRelativeIndex(),
                                response.getAttribution(),
                                response.getPastValues(),
                                response.getExpectedValuesList(),
                                response.getLikelihoodOfValues(),
                                response.getThreshold(),
                                null
                            )
                        );
                } else {
                    LOG.warn(ResultProcessor.NULL_RESPONSE + " {} for {}", modelID, rcfNodeID);
                    listener.onFailure(new InternalFailure(adID, ADCommonMessages.NO_MODEL_ERR_MSG));
                }
            } catch (Exception ex) {
                LOG.error(new ParameterizedMessage("Unexpected exception for [{}]", adID), ex);
                ResultProcessor.handleExecuteException(ex, listener, adID);
            }
        }

        @Override
        public void onFailure(Exception exception) {
            try {
                if (exception != null) {
                    listener.onFailure(exception);
                } else {
                    listener.onFailure(new InternalFailure(adID, "Node connection problem or unexpected exception"));
                }
            } catch (Exception ex) {
                LOG.error(new ParameterizedMessage("Unexpected exception for [{}]", adID), ex);
                ResultProcessor.handleExecuteException(ex, listener, adID);
            }
        }
    }

    // no exception should be thrown
    @SuppressWarnings("unchecked")
    public void testOnFailureNull() throws IOException {
        RCFActionListener listener = new RCFActionListener(null, null, null, null, mock(ActionListener.class), null, null);
        listener.onFailure(null);
    }

    @SuppressWarnings("unchecked")
    public void testNullRCFResult() {
        RCFActionListener listener = new RCFActionListener("123-rcf-0", null, "123", null, mock(ActionListener.class), null, null);
        listener.onResponse(null);
        assertTrue(testAppender.containsMessage(ResultProcessor.NULL_RESPONSE));
    }

    @SuppressWarnings("unchecked")
    public void testNormalRCFResult() {
        ActionListener<AnomalyResultResponse> listener = mock(ActionListener.class);
        AnomalyDetector detector = mock(AnomalyDetector.class);
        RCFActionListener rcfListener = new RCFActionListener("123-rcf-0", null, "nodeID", detector, listener, null, adID);
        double[] attribution = new double[] { 1. };
        long totalUpdates = 32;
        double grade = 0.5;
        ArgumentCaptor<AnomalyResultResponse> responseCaptor = ArgumentCaptor.forClass(AnomalyResultResponse.class);
        rcfListener
            .onResponse(new RCFResultResponse(0.3, 0, 26, attribution, totalUpdates, grade, Version.CURRENT, 0, null, null, null, 1.1));
        verify(listener, times(1)).onResponse(responseCaptor.capture());
        assertEquals(grade, responseCaptor.getValue().getAnomalyGrade(), 1e-10);
    }

    @SuppressWarnings("unchecked")
    public void testNullPointerRCFResult() {
        ActionListener<AnomalyResultResponse> listener = mock(ActionListener.class);
        // detector being null causes NullPointerException
        RCFActionListener rcfListener = new RCFActionListener("123-rcf-0", null, "nodeID", null, listener, null, adID);
        double[] attribution = new double[] { 1. };
        long totalUpdates = 32;
        double grade = 0.5;
        ArgumentCaptor<Exception> failureCaptor = ArgumentCaptor.forClass(Exception.class);
        rcfListener
            .onResponse(new RCFResultResponse(0.3, 0, 26, attribution, totalUpdates, grade, Version.CURRENT, 0, null, null, null, 1.1));
        verify(listener, times(1)).onFailure(failureCaptor.capture());
        Exception failure = failureCaptor.getValue();
        assertTrue(failure instanceof InternalFailure);
    }

}
