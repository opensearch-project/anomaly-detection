package org.opensearch.ad.rest;

import static org.opensearch.ad.indices.AnomalyDetectionIndices.getAnomalyDetectorMappings;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.ANOMALY_DETECTORS_INDEX_MAPPING_FILE;
import static org.opensearch.ad.settings.EnabledSetting.settings;
import static org.opensearch.ad.util.RestHandlerUtils.*;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.rest.RestRequest.Method.*;
import static org.opensearch.rest.RestStatus.OK;

import java.io.IOException;
import java.net.URL;
import java.util.List;

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
import org.opensearch.ad.transport.IndexAnomalyDetectorRequest;
import org.opensearch.client.node.NodeClient;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
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

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

public class RestCreateDetectorAction extends AbstractAnomalyDetectorAction implements ExtensionRestHandler {
    private final Logger logger = LogManager.getLogger(RestCreateDetectorAction.class);
    private static final String GREETING = "Hello, %s!";
    private String worldName = "World";
    private AnomalyDetectorExtension anomalyDetectorExtension = new AnomalyDetectorExtension();
    private OpenSearchClient sdkClient = anomalyDetectorExtension.getClient();

    public RestCreateDetectorAction(Settings settings, ClusterService clusterService) throws IOException {
        super(settings, clusterService);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/detectors"));
    }

    private String getAnomalyDetectorMappings() throws IOException {
        URL url = AnomalyDetectionIndices.class.getClassLoader().getResource(ANOMALY_DETECTORS_INDEX_MAPPING_FILE);
        return Resources.toString(url, Charsets.UTF_8);
    }

    private CreateIndexRequest initAnomalyDetectorIndex(ActionListener<CreateIndexResponse> actionListener) throws IOException {
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

        Method method = request.method();

        String detectorId = request.param(DETECTOR_ID, AnomalyDetector.NO_ID);
        logger.info("AnomalyDetector {} action for detectorId {}", method, detectorId);
        NamedXContentRegistry xContentRegistry = NamedXContentRegistry.EMPTY;
        XContentParser parser = null;
        try {
            parser = request.contentParser(xContentRegistry);
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

        // Can remove this
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
            createIndexRequest = initAnomalyDetectorIndex(null);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Call markMappingUpToDate after createComponent has anomalyDetectionIndices object
        try {
            sdkClient.indices().create(createIndexRequest, markMappingUpToDate(ADIndex.CONFIG, actionListener));
        } catch (IOException e) {
            e.printStackTrace();
        }

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
