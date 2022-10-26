package org.opensearch.ad.rest;

import static org.opensearch.ad.settings.AnomalyDetectorSettings.ANOMALY_DETECTORS_INDEX_MAPPING_FILE;
import static org.opensearch.ad.util.RestHandlerUtils.*;
import static org.opensearch.rest.RestRequest.Method.*;
import static org.opensearch.rest.RestStatus.OK;

import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.AnomalyDetectorExtension;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.settings.EnabledSetting;
import org.opensearch.client.json.JsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.mapping.TypeMapping;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.CreateIndexResponse;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.extensions.rest.ExtensionRestRequest;
import org.opensearch.extensions.rest.ExtensionRestResponse;
import org.opensearch.rest.RestHandler.Route;
import org.opensearch.rest.RestRequest.Method;
import org.opensearch.sdk.ExtensionRestHandler;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import jakarta.json.stream.JsonParser;

public class RestCreateDetectorAction implements ExtensionRestHandler {
    private final Logger logger = LogManager.getLogger(RestCreateDetectorAction.class);
    private AnomalyDetectorExtension anomalyDetectorExtension = new AnomalyDetectorExtension();
    private OpenSearchClient sdkClient = anomalyDetectorExtension.getClient();

    public RestCreateDetectorAction() throws IOException {}

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/detectors"));
    }

    private String getAnomalyDetectorMappings() throws IOException {
        URL url = AnomalyDetectionIndices.class.getClassLoader().getResource(ANOMALY_DETECTORS_INDEX_MAPPING_FILE);
        return Resources.toString(url, Charsets.UTF_8);
    }

    private CreateIndexRequest initAnomalyDetectorIndex() throws FileNotFoundException {
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
                .index(AnomalyDetector.ANOMALY_DETECTORS_INDEX)
                .mappings(TypeMapping._DESERIALIZER.deserialize(parser, mapper))
                .build();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return request;
    }

    @Override
    public ExtensionRestResponse handleRequest(ExtensionRestRequest request) {
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new IllegalStateException(CommonErrorMessages.DISABLED_ERR_MSG);
        }
        Method method = request.method();

        NamedXContentRegistry xContentRegistry = NamedXContentRegistry.EMPTY;
        XContentParser parser = null;
        try {
            parser = request.contentParser(xContentRegistry);
        } catch (Exception e) {
            e.printStackTrace();
        }

        String detectorId = null;
        try {
            detectorId = (String) parser.mapOrdered().get("name");
        } catch (IOException e) {
            e.printStackTrace();
        }

        logger.info("AnomalyDetector {} action for detectorId {}", method, detectorId);

        CreateIndexRequest createIndexRequest = null;
        try {
            createIndexRequest = initAnomalyDetectorIndex();
        } catch (FileNotFoundException e) {
            logger.info("File Not Found", e);
            e.printStackTrace();
        }

        // Call markMappingUpToDate after createComponent has anomalyDetectionIndices object
        try {
            CreateIndexResponse createIndexResponse = sdkClient.indices().create(createIndexRequest);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new ExtensionRestResponse(request, OK, "Created AD index " + AnomalyDetector.ANOMALY_DETECTORS_INDEX);
    }

}
