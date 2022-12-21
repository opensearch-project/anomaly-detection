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
import static org.opensearch.ad.model.AnomalyDetector.FEATURE_ATTRIBUTES_FIELD;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.ANOMALY_DETECTORS_INDEX_MAPPING_FILE;
import static org.opensearch.ad.util.RestHandlerUtils.XCONTENT_WITH_TYPE;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.rest.RestRequest.Method.POST;
import static org.opensearch.rest.RestStatus.BAD_REQUEST;
import static org.opensearch.rest.RestStatus.CREATED;
import static org.opensearch.rest.RestStatus.OK;

import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import jakarta.json.Json;
import jakarta.json.stream.JsonGenerator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.AnomalyDetectorExtension;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.IndexAnomalyDetector;
import org.opensearch.ad.rest.handler.CreateIndexRequestBuilder;
import org.opensearch.ad.rest.handler.IndexLoader;
import org.opensearch.ad.rest.handler.IndexSerializeRequestBuilder;
import org.opensearch.ad.rest.handler.JacksonDetector;
import org.opensearch.ad.settings.EnabledSetting;
import org.opensearch.client.json.*;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch._types.mapping.TypeMapping;
import org.opensearch.client.opensearch.core.IndexRequest;
import org.opensearch.client.opensearch.core.IndexResponse;
import org.opensearch.client.opensearch.core.search.CompletionSuggestOption;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.CreateIndexResponse;
import org.opensearch.client.opensearch.indices.PutMappingResponse;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.*;
import org.opensearch.extensions.rest.ExtensionRestRequest;
import org.opensearch.extensions.rest.ExtensionRestResponse;
import org.opensearch.sdk.BaseExtensionRestHandler;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.RouteHandler;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import jakarta.json.stream.JsonParser;

public class RestCreateDetectorAction extends BaseExtensionRestHandler {

    private final OpenSearchClient sdkClient;
    private final NamedXContentRegistry xContentRegistry;
    private static final Logger logger = LogManager.getLogger(RestCreateDetectorAction.class);

    public RestCreateDetectorAction(ExtensionsRunner runner, AnomalyDetectorExtension extension) {
        this.xContentRegistry = runner.getNamedXContentRegistry().getRegistry();
        this.sdkClient = extension.getClient();
    }

    @Override
    protected List<RouteHandler> routeHandlers() {
        return List.of(new RouteHandler(POST, "/detectors", (r) -> handlePostRequest(r)));
    }

    private String getAnomalyDetectorMappings() throws IOException {
        URL url = AnomalyDetectionIndices.class.getClassLoader().getResource(ANOMALY_DETECTORS_INDEX_MAPPING_FILE);
        return Resources.toString(url, Charsets.UTF_8);
    }

    private IndexResponse indexAnomalyDetector(AnomalyDetector anomalyDetector) throws IOException {
        AnomalyDetector detector = new AnomalyDetector(
            anomalyDetector.getDetectorId(),
            anomalyDetector.getVersion(),
            anomalyDetector.getName(),
            anomalyDetector.getDescription(),
            anomalyDetector.getTimeField(),
            anomalyDetector.getIndices(),
            anomalyDetector.getFeatureAttributes(),
            anomalyDetector.getFilterQuery(),
            anomalyDetector.getDetectionInterval(),
            anomalyDetector.getWindowDelay(),
            anomalyDetector.getShingleSize(),
            anomalyDetector.getUiMetadata(),
            anomalyDetector.getSchemaVersion(),
            Instant.now(),
            anomalyDetector.getCategoryField(),
            null,
            anomalyDetector.getResultIndex()
        );


        JacksonDetector jacksonDetector = new JacksonDetector(detector, detector.toString());
//        IndexSerializeRequestBuilder indexSerializeRequestBuilder = new IndexSerializeRequestBuilder();
//
//        JacksonJsonpMapper mapper = (JacksonJsonpMapper) sdkClient._transport().jsonpMapper();
//        JsonParser parser = Json.createParser(new ByteArrayInputStream(getAnomalyDetectorMappings().getBytes(StandardCharsets.UTF_8)));
//        IndexSerializeRequestBuilder indexSerializeRequestBuilder = mapper.deserialize(parser, IndexSerializeRequestBuilder.class);

        IndexAnomalyDetector indexAnomalyDetector = new IndexAnomalyDetector();
        indexAnomalyDetector.setName(anomalyDetector.getName());
        indexAnomalyDetector.setDetectorId(anomalyDetector.getDetectorId());
        indexAnomalyDetector.setDetectorType(anomalyDetector.getDetectorType());
        indexAnomalyDetector.setDetectionInterval(anomalyDetector.getDetectionInterval());
        indexAnomalyDetector.setDetectionDateRange(anomalyDetector.getDetectionDateRange());
        indexAnomalyDetector.setDescription(anomalyDetector.getDescription());
        indexAnomalyDetector.setFeatureAttributes(anomalyDetector.getFeatureAttributes());
        indexAnomalyDetector.setFilterQuery(anomalyDetector.getFilterQuery());
        indexAnomalyDetector.setCategoryFields(anomalyDetector.getCategoryField());
        indexAnomalyDetector.setLastUpdateTime(anomalyDetector.getLastUpdateTime());
        indexAnomalyDetector.setResultIndex(anomalyDetector.getResultIndex());
        indexAnomalyDetector.setSchemaVersion(anomalyDetector.getSchemaVersion());
        indexAnomalyDetector.setShingleSize(anomalyDetector.getShingleSize());
        indexAnomalyDetector.setTimeField(anomalyDetector.getTimeField());
        indexAnomalyDetector.setWindowDelay(anomalyDetector.getWindowDelay());
        indexAnomalyDetector.setUiMetadata(anomalyDetector.getUiMetadata());
        indexAnomalyDetector.setVersion(anomalyDetector.getVersion());

        IndexLoader indexLoader = new IndexLoader();
//        detector.toXContent(indexLoader.toXContent(XContentFactory.jsonBuilder(), detector));

        IndexRequest<AnomalyDetector> indexRequest = new IndexRequest.Builder<AnomalyDetector>()
            .index(ANOMALY_DETECTORS_INDEX)
//                .tDocumentSerializer((JsonpSerializer<IndexSerializeRequestBuilder>) indexSerializeRequestBuilder.build())
//            .document(detector)

                .document(detector)
            .build();
        IndexResponse indexResponse = sdkClient.index(indexRequest);
        return indexResponse;

    }

    private CreateIndexRequest initAnomalyDetectorIndex() {
        JsonpMapper mapper = sdkClient._transport().jsonpMapper();
        JsonParser parser = null;
        try {
            parser = mapper
                .jsonProvider()
                .createParser(new ByteArrayInputStream(getAnomalyDetectorMappings().getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            e.printStackTrace();
        }

        CreateIndexRequest request = null;
        try {
            request = new CreateIndexRequest.Builder()
                .index(ANOMALY_DETECTORS_INDEX)
                .mappings(TypeMapping._DESERIALIZER.deserialize(parser, mapper))
                .build();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return request;
    }

    private ExtensionRestResponse handlePostRequest(ExtensionRestRequest request) {
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new IllegalStateException(CommonErrorMessages.DISABLED_ERR_MSG);
        }

        XContentParser parser;
        AnomalyDetector detector;
        XContentBuilder builder = null;
        CreateIndexRequest createIndexRequest;
        try {
            parser = request.contentParser(this.xContentRegistry);
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            detector = AnomalyDetector.parse(parser);
            createIndexRequest = initAnomalyDetectorIndex();
            CreateIndexResponse createIndexResponse = sdkClient.indices().create(createIndexRequest);
            if (createIndexResponse.acknowledged()) {
                IndexResponse indexResponse = indexAnomalyDetector(detector);
                try {
                    builder = XContentBuilder.builder(XContentType.JSON.xContent());
                    builder.startObject();
                    builder.field("id", indexResponse.id());
                    builder.field("version", indexResponse.version());
                    builder.field("seqNo", indexResponse.seqNo());
                    builder.field("primaryTerm", indexResponse.primaryTerm());
                    builder.field("detector", detector);
                    builder.field("status", CREATED);
                    builder.endObject();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            return new ExtensionRestResponse(request, BAD_REQUEST, builder);
        }
        return new ExtensionRestResponse(request, OK, builder);
    }
}
