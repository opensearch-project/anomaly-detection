package org.opensearch.ad.rest;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.ad.AnomalyDetectorExtension;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.settings.EnabledSetting;
import org.opensearch.ad.transport.IndexAnomalyDetectorRequest;
import org.opensearch.ad.transport.IndexAnomalyDetectorResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.CreateIndexResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.*;
import org.opensearch.extensions.rest.ExtensionRestRequest;
import org.opensearch.extensions.rest.ExtensionRestResponse;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.sdk.BaseExtensionRestHandler;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.RouteHandler;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

import static org.opensearch.ad.constant.CommonErrorMessages.FAIL_TO_CREATE_DETECTOR;
import static org.opensearch.ad.constant.CommonErrorMessages.FAIL_TO_UPDATE_DETECTOR;
import static org.opensearch.ad.indices.AnomalyDetectionIndices.getAnomalyDetectorMappings;
import static org.opensearch.ad.model.AnomalyDetector.ANOMALY_DETECTORS_INDEX;
import static org.opensearch.ad.util.RestHandlerUtils.*;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.rest.RestRequest.Method.POST;
import static org.opensearch.rest.RestStatus.*;

public class RestSDKCreateDetectorAction extends BaseExtensionRestHandler {
    private final Logger logger = LogManager.getLogger(RestIndexAnomalyDetectorAction.class);
    Settings settings = Settings.builder()
            .put("path.home", ".")
            .put("discovery.zen.ping.unicast.hosts","127.0.0.1").build();
    //    private final OpenSearchClient sdkClient;
    private final RestHighLevelClient sdkRestClient;
    private final NamedXContentRegistry xContentRegistry;
    private boolean filterByEnabled = AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES.get(settings);

    public RestSDKCreateDetectorAction(ExtensionsRunner runner, AnomalyDetectorExtension extension) {
        this.xContentRegistry = runner.getNamedXContentRegistry().getRegistry();
        this.sdkRestClient = extension.getRestClient();
    }

    @Override
    public List<RouteHandler> routeHandlers() {
        return List.of(new RouteHandler(POST, "/sdk", (r) -> prepareRequest(r)));
    }

    private CreateIndexRequest initAnomalyDetectorIndex() throws IOException {
        Settings settings = Settings.builder().put("index.hidden", true).build();
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(ANOMALY_DETECTORS_INDEX)
                .mapping(getAnomalyDetectorMappings(), XContentType.JSON)
                .settings(settings);

        return createIndexRequest;
    }

    private XContentBuilder indexAnomalyDetector(AnomalyDetector anomalyDetector, String detectorId) throws IOException {
        AnomalyDetector detector = new AnomalyDetector(
                anomalyDetector.getName(),
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

        IndexRequest indexRequest = new IndexRequest(ANOMALY_DETECTORS_INDEX)
                .source(detector.toXContent(XContentFactory.jsonBuilder(), XCONTENT_WITH_TYPE));
        if (StringUtils.isNotBlank(detectorId)) {
            indexRequest.id(detectorId);
        }
        XContentBuilder builder = null;

//        IndexResponse indexResponse = sdkRestClient.indexAsync(indexRequest, RequestOptions.DEFAULT, new ActionListener<IndexResponse>() {
//            @Override
//            public void onResponse(IndexResponse indexResponse) {
//                try {
//                    builder = XContentBuilder.builder(XContentType.JSON.xContent());
//                    builder.startObject();
//                    builder.field("id", indexResponse.getId());
//                    builder.field("version", indexResponse.getVersion());
//                    builder.field("seqNo", indexResponse.getSeqNo());
//                    builder.field("primaryTerm", indexResponse.getPrimaryTerm());
//                    builder.field("detector", detector);
//                    builder.field("status", CREATED);
//                    builder.endObject();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//
//            @Override
//            public void onFailure(Exception e) {
//
//            }
//        });
        IndexResponse indexResponse = sdkRestClient.index(indexRequest, RequestOptions.DEFAULT);
        try {
            builder = XContentBuilder.builder(XContentType.JSON.xContent());
            builder.startObject();
            builder.field("id", indexResponse.getId());
            builder.field("version", indexResponse.getVersion());
            builder.field("seqNo", indexResponse.getSeqNo());
            builder.field("primaryTerm", indexResponse.getPrimaryTerm());
            builder.field("detector", detector);
            builder.field("status", CREATED);
            builder.endObject();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return builder;

    }


    protected ExtensionRestResponse prepareRequest(ExtensionRestRequest request) {
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new IllegalStateException(CommonErrorMessages.DISABLED_ERR_MSG);
        }

        String detectorId = request.param(DETECTOR_ID, AnomalyDetector.NO_ID);
        logger.info("AnomalyDetector {} action for detectorId {}", request.method(), detectorId);

        XContentParser parser;
        AnomalyDetector detector = null;
        XContentBuilder builder = null;

        try {
            parser = request.contentParser(this.xContentRegistry);
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            // TODO: check detection interval < modelTTL
            detector = AnomalyDetector.parse(parser, detectorId, null, null, null);


            long seqNo = request.paramAsLong(IF_SEQ_NO, SequenceNumbers.UNASSIGNED_SEQ_NO);
            long primaryTerm = request.paramAsLong(IF_PRIMARY_TERM, SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
            WriteRequest.RefreshPolicy refreshPolicy = request.hasParam(REFRESH)
                    ? WriteRequest.RefreshPolicy.parse(request.param(REFRESH))
                    : WriteRequest.RefreshPolicy.IMMEDIATE;
//        RestRequest.Method method = request.getHttpRequest().method();

            IndexAnomalyDetectorRequest indexAnomalyDetectorRequest = new IndexAnomalyDetectorRequest(
                    detectorId,
                    seqNo,
                    primaryTerm,
                    refreshPolicy,
                    detector,
                    null,
                    null,
                    null,
                    null,
                    null
            );
            RestRequest.Method method = RestRequest.Method.POST;
            String errorMessage = method == RestRequest.Method.PUT ? FAIL_TO_UPDATE_DETECTOR : FAIL_TO_CREATE_DETECTOR;
            ActionListener<IndexAnomalyDetectorResponse> listener = wrapRestActionListener(null, errorMessage);
            CreateIndexRequest createIndexRequest = initAnomalyDetectorIndex();
            CreateIndexResponse createIndexResponse = sdkRestClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            if(createIndexResponse.isAcknowledged()) {
                builder = indexAnomalyDetector(detector, detectorId);
            }

        } catch (Exception e) {
            e.printStackTrace();
            return new ExtensionRestResponse(request, BAD_REQUEST, builder);
        }
        return new ExtensionRestResponse(request, OK, builder);
    }
}
