/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ad.rest;

import static org.opensearch.ad.model.AnomalyDetector.*;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestStatus.OK;
import static org.opensearch.rest.RestStatus.BAD_REQUEST;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.opensearch.ad.AnomalyDetectorExtension;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.settings.EnabledSetting;
import org.opensearch.ad.util.RestHandlerUtils;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.GetResponse;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.compress.CompressorFactory;
import org.opensearch.common.io.stream.BytesStream;
import org.opensearch.common.xcontent.*;
import org.opensearch.extensions.rest.ExtensionRestRequest;
import org.opensearch.extensions.rest.ExtensionRestResponse;
import org.opensearch.rest.RestRequest.Method;
import org.opensearch.rest.RestStatus;
import org.opensearch.sdk.BaseExtensionRestHandler;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.RouteHandler;

public class RestGetDetectorAction extends BaseExtensionRestHandler {
    private final Logger logger = LogManager.getLogger(RestGetDetectorAction.class);
    private final OpenSearchClient sdkClient;
    private final NamedXContentRegistry xContentRegistry;

    public RestGetDetectorAction(ExtensionsRunner runner, AnomalyDetectorExtension extension) {
        this.xContentRegistry = runner.getNamedXContentRegistry().getRegistry();
        this.sdkClient = extension.getClient();
    }

    @Override
    protected List<RouteHandler> routeHandlers() {
        return List.of(new RouteHandler(GET, "/detectors/{detectorId}", (r) -> handleGetRequest(r)));
    }

    static BytesReference bytes(XContentBuilder xContentBuilder) {
        xContentBuilder.close();
        OutputStream stream = xContentBuilder.getOutputStream();
        if (stream instanceof ByteArrayOutputStream) {
            return new BytesArray(((ByteArrayOutputStream) stream).toByteArray());
        } else {
            return ((BytesStream) stream).bytes();
        }
    }

    public static XContentBuilder builder(XContent xContent) throws IOException {
        return new XContentBuilder(xContent, new ByteArrayOutputStream());
    }

    public ExtensionRestResponse handleGetRequest(ExtensionRestRequest request) {
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new IllegalStateException(CommonErrorMessages.DISABLED_ERR_MSG);
        }

        String detectorId = request.param("detectorId");
        // TODO
        // 1. Get Detector id
        // 2. Find a way to query SearchRequest
        // 3. Else, match detectorId with the SearchRequest response
        // 4. Follow the steps on the issue
        // 5. Look for MultiGetRequest

        // Commented this code for SearchResponse
//        SearchResponse<AnomalyDetector> searchResponse = null;
//        try {
//            searchResponse = sdkClient.search(s -> s.index(ANOMALY_DETECTORS_INDEX), AnomalyDetector.class);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

        GetResponse<AnomalyDetector> getResponse = null;
        try{
            getResponse = sdkClient.get(s -> s.index(ANOMALY_DETECTORS_INDEX).id(detectorId), AnomalyDetector.class);
        } catch (Exception e) {
            e.printStackTrace();
        }

        logger.info("GET RESPONSE :{}", getResponse.version());


        XContentBuilder builder = null;
        XContentParser parser;
        AnomalyDetector detector;
        // Commented this code for SearchResponse
//        try {
//            for (int i = 0; i < searchResponse.hits().hits().size(); i++) {
//                logger.info("Search query response: {}", searchResponse.hits().hits().size());
//                if (searchResponse.hits().hits().get(i).id().equals(detectorId)) {
//////                    parser = request.contentParser(this.xContentRegistry);
////                    parser = RestHandlerUtils.createXContentParserFromRegistry(xContentRegistry, (BytesReference) searchResponse.hits().hits().stream());
////                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
////                    detector = AnomalyDetector.parse(parser);
//                    builder = XContentBuilder.builder(XContentType.JSON.xContent());
//                    builder.startObject();
//                    builder.field("id", searchResponse.hits().hits().get(i).id());
//                    builder.field("version", searchResponse.hits().hits().get(i).version());
//                    builder.field("seqNo", searchResponse.hits().hits().get(i).seqNo());
//                    builder.field("primaryTerm", searchResponse.hits().hits().get(i).primaryTerm());
//                    builder.field("detector", searchResponse.hits().hits().get(i).source());
//                    builder.field("status", RestStatus.CREATED);
//                    builder.endObject();
//                }
//            }
//        } catch (Exception e) {
//            return new ExtensionRestResponse(request, BAD_REQUEST, builder);
//        }

//        XContentParser parser;
        BytesReference sources = null;
        try {
            sources = BytesReference.bytes(XContentBuilder.builder(XContentType.JSON.xContent()).startObject()
                    .field(NAME_FIELD, getResponse.source().getName())
                    .field(DESCRIPTION_FIELD, getResponse.source().getDescription())
                    .field(TIMEFIELD_FIELD, getResponse.source().getTimeField())
                    .field(INDICES_FIELD, getResponse.source().getIndices())
                    .field(FILTER_QUERY_FIELD, getResponse.source().getFilterQuery())
                    .field(DETECTION_INTERVAL_FIELD, getResponse.source().getDetectionInterval())
                    .field(WINDOW_DELAY_FIELD, getResponse.source().getWindowDelay())
                    .field(SHINGLE_SIZE_FIELD, getResponse.source().getShingleSize())
                    .field(CommonName.SCHEMA_VERSION_FIELD, getResponse.source().getSchemaVersion())
                    .field(FEATURE_ATTRIBUTES_FIELD, getResponse.source().getFeatureAttributes())
                    .endObject());

        } catch(Exception e) {
            e.printStackTrace();;
        }

        try{
            parser = RestHandlerUtils.createXContentParserFromRegistry(xContentRegistry, sources);

            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            detector = AnomalyDetector.parse(parser);
            builder = XContentBuilder.builder(XContentType.JSON.xContent());
            builder.startObject();
            builder.field("id", getResponse.id());
            builder.field("version", getResponse.version());
            builder.field("seqNo", getResponse.seqNo());
            builder.field("primaryTerm", getResponse.primaryTerm());
            builder.field("detector", detector);
            builder.field("status", RestStatus.CREATED);
            builder.endObject();

        } catch(Exception e) {
            e.printStackTrace();
            return new ExtensionRestResponse(request, BAD_REQUEST, builder);
        }


        return new ExtensionRestResponse(request, OK, builder);
    }
}
