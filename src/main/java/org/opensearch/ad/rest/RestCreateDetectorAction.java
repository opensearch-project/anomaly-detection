package org.opensearch.ad.rest;

import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestRequest.Method.PUT;
import static org.opensearch.rest.RestStatus.BAD_REQUEST;
import static org.opensearch.rest.RestStatus.NOT_ACCEPTABLE;
import static org.opensearch.rest.RestStatus.NOT_FOUND;
import static org.opensearch.rest.RestStatus.NOT_MODIFIED;
import static org.opensearch.rest.RestStatus.NOT_FOUND;
import static org.opensearch.rest.RestStatus.OK;
import static org.opensearch.ad.indices.AnomalyDetectionIndices.getAnomalyDetectorMappings;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.ANOMALY_DETECTORS_INDEX_MAPPING_FILE;
import static org.opensearch.ad.settings.EnabledSetting.settings;
import static org.opensearch.ad.util.RestHandlerUtils.*;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.rest.RestRequest.Method.*;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.opensearch.extensions.rest.ExtensionRestResponse;
import java.util.function.Function;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.ad.AnomalyDetectorExtension;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.settings.EnabledSetting;
import org.opensearch.ad.transport.IndexAnomalyDetectorAction;
import org.opensearch.ad.transport.IndexAnomalyDetectorRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.node.NodeClient;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.util.ObjectBuilder;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.extensions.rest.ExtensionRestRequest;
import org.opensearch.extensions.rest.ExtensionRestResponse;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.rest.RestHandler.Route;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestRequest.Method;
import org.opensearch.sdk.ExtensionRestHandler;


public class RestCreateDetectorAction extends AbstractAnomalyDetectorAction implements ExtensionRestHandler  {
    private final Logger logger = LogManager.getLogger(RestCreateDetectorAction.class);
    private static final String GREETING = "Hello, %s!";
    private String worldName = "World";
    private OpenSearchClient sdkClient = AnomalyDetectorExtension.getClient();

    public RestCreateDetectorAction(Settings settings, ClusterService clusterService) throws IOException {
        super(settings, clusterService);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/detectors"));
    }

    public String getAnomalyDetectorMappings() throws IOException {
        URL url = AnomalyDetectionIndices.class.getClassLoader().getResource(ANOMALY_DETECTORS_INDEX_MAPPING_FILE);
        return Resources.toString(url, Charsets.UTF_8);
    }

    public CreateIndexRequest initAnomalyDetectorIndex(ActionListener<CreateIndexResponse> actionListener) throws IOException {
        CreateIndexRequest request = new CreateIndexRequest(AnomalyDetector.ANOMALY_DETECTORS_INDEX)
                .mapping(getAnomalyDetectorMappings(), XContentType.JSON)
                .settings(settings);
        return request;
    }


    @Override
    public ExtensionRestResponse handleRequest(ExtensionRestRequest request) {
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new IllegalStateException(CommonErrorMessages.DISABLED_ERR_MSG);
        }

        List<String> consumedParams = new ArrayList<>();
        Method method = request.method();
        //// String uri = request.uri();
        // String name = uri.substring("/detectors/".length());
        // consumedParams.add("name");
        // worldName = URLDecoder.decode(name, StandardCharsets.UTF_8);
        // logger.info("AnomalyDetector {} action for uri {} and world {} and consumed {}", method, uri, worldName, consumedParams);

        // CreateIndexRequest createIndexRequest = new CreateIndexRequest.Builder().index(AnomalyDetector.ANOMALY_DETECTORS_INDEX).build();
        //
        // try {
        // sdkClient.indices().create(createIndexRequest);
        // } catch (IOException e) {
        // e.printStackTrace();
        // }
         String detectorId = request.param(DETECTOR_ID, AnomalyDetector.NO_ID);
         logger.info("AnomalyDetector {} action for detectorId {}", method, detectorId);
        //
        NamedXContentRegistry xContentRegistry = NamedXContentRegistry.EMPTY;
         BytesReference content = request.content();
        XContentParser parser = null;
        try {
            parser = request.getXContentType().xContent().createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, content.streamInput());
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // // TODO: check detection interval < modelTTL
        AnomalyDetector detector = null;
        try {
            detector = AnomalyDetector.parse(parser, detectorId, null, detectionInterval, detectionWindowDelay);
        } catch (IOException e) {
            e.printStackTrace();
        }

        long seqNo = request.paramAsLong(IF_SEQ_NO, SequenceNumbers.UNASSIGNED_SEQ_NO);
        long primaryTerm = request.paramAsLong(IF_PRIMARY_TERM, SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
        WriteRequest.RefreshPolicy refreshPolicy = request.hasParam(REFRESH)
         ? WriteRequest.RefreshPolicy.parse(request.param(REFRESH))
         : WriteRequest.RefreshPolicy.IMMEDIATE;


         IndexAnomalyDetectorRequest indexAnomalyDetectorRequest = new IndexAnomalyDetectorRequest(
         detectorId,
         seqNo,
         primaryTerm,
         refreshPolicy,
         detector,
         method,
         requestTimeout,
         maxSingleEntityDetectors,
         maxMultiEntityDetectors,
         maxAnomalyFeatures
         );

         CreateIndexRequest createIndexRequest = null;
        try {
            createIndexRequest  = initAnomalyDetectorIndex(null);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Call  markMappingUpToDate after createComponent has anomalyDetectionIndices object
        try {
            sdkClient.indices().create((Function<org.opensearch.client.opensearch.indices.CreateIndexRequest.Builder, ObjectBuilder<org.opensearch.client.opensearch.indices.CreateIndexRequest>>) createIndexRequest);
        } catch (IOException e) {
            e.printStackTrace();
        }

//         return channel -> sdkClient
//         .execute(IndexAnomalyDetectorAction.INSTANCE, indexAnomalyDetectorRequest, indexAnomalyDetectorResponse(channel, method));
         return new ExtensionRestResponse(request, OK, "Created AD index " + AnomalyDetector.ANOMALY_DETECTORS_INDEX);
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient nodeClient) throws IOException {
        return null;
    }
}
