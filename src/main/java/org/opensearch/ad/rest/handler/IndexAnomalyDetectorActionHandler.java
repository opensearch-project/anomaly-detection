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

package org.opensearch.ad.rest.handler;

import org.opensearch.action.ActionListener;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.ad.feature.SearchFeatureDao;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.transport.IndexAnomalyDetectorResponse;
import org.opensearch.ad.util.SecurityClientUtil;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.commons.authuser.User;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.TransportService;

/**
 * Anomaly detector REST action handler to process POST/PUT request.
 * POST request is for creating anomaly detector.
 * PUT request is for updating anomaly detector.
 */
public class IndexAnomalyDetectorActionHandler extends AbstractAnomalyDetectorActionHandler<IndexAnomalyDetectorResponse> {

    /**
     * Constructor function.
     *
     * @param clusterService          ClusterService
     * @param client                  ES node client that executes actions on the local node
     * @param clientUtil              AD client util
     * @param transportService        ES transport service
     * @param listener                 ES channel used to construct bytes / builder based outputs, and send responses
     * @param anomalyDetectionIndices anomaly detector index manager
     * @param detectorId              detector identifier
     * @param seqNo                   sequence number of last modification
     * @param primaryTerm             primary term of last modification
     * @param refreshPolicy           refresh policy
     * @param anomalyDetector         anomaly detector instance
     * @param requestTimeout          request time out configuration
     * @param maxSingleEntityAnomalyDetectors     max single-entity anomaly detectors allowed
     * @param maxMultiEntityAnomalyDetectors      max multi-entity detectors allowed
     * @param maxAnomalyFeatures      max features allowed per detector
     * @param method                  Rest Method type
     * @param xContentRegistry        Registry which is used for XContentParser
     * @param user                    User context
     * @param adTaskManager           AD Task manager
     * @param searchFeatureDao        Search feature dao
     * @param settings                Node settings
     */
    public IndexAnomalyDetectorActionHandler(
        ClusterService clusterService,
        Client client,
        SecurityClientUtil clientUtil,
        TransportService transportService,
        ActionListener<IndexAnomalyDetectorResponse> listener,
        AnomalyDetectionIndices anomalyDetectionIndices,
        String detectorId,
        Long seqNo,
        Long primaryTerm,
        WriteRequest.RefreshPolicy refreshPolicy,
        AnomalyDetector anomalyDetector,
        TimeValue requestTimeout,
        Integer maxSingleEntityAnomalyDetectors,
        Integer maxMultiEntityAnomalyDetectors,
        Integer maxAnomalyFeatures,
        RestRequest.Method method,
        NamedXContentRegistry xContentRegistry,
        User user,
        ADTaskManager adTaskManager,
        SearchFeatureDao searchFeatureDao,
        Settings settings
    ) {
        super(
            clusterService,
            client,
            clientUtil,
            transportService,
            listener,
            anomalyDetectionIndices,
            detectorId,
            seqNo,
            primaryTerm,
            refreshPolicy,
            anomalyDetector,
            requestTimeout,
            maxSingleEntityAnomalyDetectors,
            maxMultiEntityAnomalyDetectors,
            maxAnomalyFeatures,
            method,
            xContentRegistry,
            user,
            adTaskManager,
            searchFeatureDao,
            null,
            false,
            null,
            settings
        );
    }

    /**
     * Start function to process create/update anomaly detector request.
     */
    @Override
    public void start() {
        super.start();
    }
}
