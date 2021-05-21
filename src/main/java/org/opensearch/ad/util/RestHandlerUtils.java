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

package org.opensearch.ad.util;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.Feature;
import org.opensearch.common.Strings;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.fetch.subphase.FetchSourceContext;

import com.google.common.collect.ImmutableMap;

/**
 * Utility functions for REST handlers.
 */
public final class RestHandlerUtils {

    public static final String _ID = "_id";
    public static final String _VERSION = "_version";
    public static final String _SEQ_NO = "_seq_no";
    public static final String IF_SEQ_NO = "if_seq_no";
    public static final String _PRIMARY_TERM = "_primary_term";
    public static final String IF_PRIMARY_TERM = "if_primary_term";
    public static final String REFRESH = "refresh";
    public static final String DETECTOR_ID = "detectorID";
    public static final String ANOMALY_DETECTOR = "anomaly_detector";
    public static final String ANOMALY_DETECTOR_JOB = "anomaly_detector_job";
    public static final String ANOMALY_DETECTION_TASK = "anomaly_detection_task";
    public static final String RUN = "_run";
    public static final String PREVIEW = "_preview";
    public static final String START_JOB = "_start";
    public static final String STOP_JOB = "_stop";
    public static final String PROFILE = "_profile";
    public static final String TYPE = "type";
    public static final String ENTITY = "entity";
    public static final String COUNT = "count";
    public static final String MATCH = "match";
    public static final ToXContent.MapParams XCONTENT_WITH_TYPE = new ToXContent.MapParams(ImmutableMap.of("with_type", "true"));

    private static final String OPENSEARCH_DASHBOARDS_USER_AGENT = "OpenSearch Dashboards";
    private static final String[] UI_METADATA_EXCLUDE = new String[] { AnomalyDetector.UI_METADATA_FIELD };

    private RestHandlerUtils() {}

    /**
     * Checks to see if the request came from Kibana, if so we want to return the UI Metadata from the document.
     * If the request came from the client then we exclude the UI Metadata from the search result.
     *
     * @param request rest request
     * @return instance of {@link org.opensearch.search.fetch.subphase.FetchSourceContext}
     */
    public static FetchSourceContext getSourceContext(RestRequest request) {
        String userAgent = Strings.coalesceToEmpty(request.header("User-Agent"));
        if (!userAgent.contains(OPENSEARCH_DASHBOARDS_USER_AGENT)) {
            return new FetchSourceContext(true, Strings.EMPTY_ARRAY, UI_METADATA_EXCLUDE);
        } else {
            return null;
        }
    }

    public static XContentParser createXContentParser(RestChannel channel, BytesReference bytesReference) throws IOException {
        return XContentHelper
            .createParser(channel.request().getXContentRegistry(), LoggingDeprecationHandler.INSTANCE, bytesReference, XContentType.JSON);
    }

    public static XContentParser createXContentParserFromRegistry(NamedXContentRegistry xContentRegistry, BytesReference bytesReference)
        throws IOException {
        return XContentHelper.createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, bytesReference, XContentType.JSON);
    }

    public static String validateAnomalyDetector(AnomalyDetector anomalyDetector, int maxAnomalyFeatures) {
        List<Feature> features = anomalyDetector.getFeatureAttributes();
        if (features != null) {
            if (features.size() > maxAnomalyFeatures) {
                return "Can't create anomaly features more than " + maxAnomalyFeatures;
            }
            return validateFeatures(anomalyDetector.getFeatureAttributes());
        }
        return null;
    }

    private static String validateFeatures(List<Feature> features) {
        final Set<String> duplicateFeatureNames = new HashSet<>();
        final Set<String> featureNames = new HashSet<>();
        final Set<String> duplicateFeatureAggNames = new HashSet<>();
        final Set<String> featureAggNames = new HashSet<>();

        features.forEach(feature -> {
            if (!featureNames.add(feature.getName())) {
                duplicateFeatureNames.add(feature.getName());
            }
            if (!featureAggNames.add(feature.getAggregation().getName())) {
                duplicateFeatureAggNames.add(feature.getAggregation().getName());
            }
        });

        StringBuilder errorMsgBuilder = new StringBuilder();
        if (duplicateFeatureNames.size() > 0) {
            errorMsgBuilder.append("Detector has duplicate feature names: ");
            errorMsgBuilder.append(String.join(", ", duplicateFeatureNames)).append("\n");
        }
        if (duplicateFeatureAggNames.size() > 0) {
            errorMsgBuilder.append("Detector has duplicate feature aggregation query names: ");
            errorMsgBuilder.append(String.join(", ", duplicateFeatureAggNames));
        }
        return errorMsgBuilder.toString();
    }
}
