package org.opensearch.ad.rest;

import static org.opensearch.ad.util.RestHandlerUtils.TYPE;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.rest.RestRequest.Method.POST;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.AnomalyDetectorExtension;
import org.opensearch.ad.common.exception.ADValidationException;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.DetectorValidationIssue;
import org.opensearch.ad.model.ValidationAspect;
import org.opensearch.ad.settings.EnabledSetting;
import org.opensearch.ad.transport.ValidateAnomalyDetectorRequest;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.extensions.rest.ExtensionRestRequest;
import org.opensearch.extensions.rest.ExtensionRestResponse;
import org.opensearch.rest.RestStatus;
import org.opensearch.sdk.BaseExtensionRestHandler;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.RouteHandler;

public class RestValidateDetectorAction extends BaseExtensionRestHandler {
    private final Logger logger = LogManager.getLogger(RestValidateDetectorAction.class);
    private final OpenSearchClient sdkClient;
    private final NamedXContentRegistry xContentRegistry;

    public static final Set<String> ALL_VALIDATION_ASPECTS_STRS = Arrays
        .asList(ValidationAspect.values())
        .stream()
        .map(aspect -> aspect.getName())
        .collect(Collectors.toSet());

    public RestValidateDetectorAction(ExtensionsRunner runner, AnomalyDetectorExtension extension) {
        this.xContentRegistry = runner.getNamedXContentRegistry().getRegistry();
        this.sdkClient = extension.getClient();
    }

    @Override
    protected List<RouteHandler> routeHandlers() {
        return List.of(new RouteHandler(POST, "/detectors/_validate", (r) -> handleValidateDetectorRequest(r)));
    }

    private ExtensionRestResponse handleValidateDetectorRequest(ExtensionRestRequest request) {
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new IllegalStateException(CommonErrorMessages.DISABLED_ERR_MSG);
        }
        AnomalyDetector detector;
        XContentParser parser;
        XContentBuilder builder = null;
        ValidateAnomalyDetectorRequest validateAnomalyDetectorRequest;
        try {
            parser = request.contentParser(this.xContentRegistry);
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            String typesStr = validateTypeString(request);
            DetectorValidationIssue issue = null;
            try {
                detector = AnomalyDetector.parse(parser);
                // validateAnomalyDetectorRequest= new ValidateAnomalyDetectorRequest(
                // detector,
                // typesStr,
                // extension,
                // maxMultiEntityDetectors,
                // maxAnomalyFeatures,
                // requestTimeout
                // );
            } catch (Exception e) {
                if (e instanceof ADValidationException) {
                    ADValidationException ADException = (ADValidationException) e;
                    issue = new DetectorValidationIssue(ADException.getAspect(), ADException.getType(), ADException.getMessage());
                }
            }

            try {
                builder = XContentBuilder.builder(XContentType.JSON.xContent());
                builder.startObject();
                builder.field("issue", issue);
                builder.endObject();
            } catch (IOException e) {
                e.printStackTrace();
            }

        } catch (Exception e) {
            return new ExtensionRestResponse(request, RestStatus.BAD_REQUEST, builder);
        }
        return new ExtensionRestResponse(request, RestStatus.OK, "placeholder");
    }

    private Boolean validationTypesAreAccepted(String validationType) {
        Set<String> typesInRequest = new HashSet<>(Arrays.asList(validationType.split(",")));
        return (!Collections.disjoint(typesInRequest, ALL_VALIDATION_ASPECTS_STRS));
    }

    private String validateTypeString(ExtensionRestRequest request) {
        String typesStr = request.param(TYPE);

        // if type param isn't blank and isn't a part of possible validation types throws exception
        if (!StringUtils.isBlank(typesStr)) {
            if (!validationTypesAreAccepted(typesStr)) {
                throw new IllegalStateException(CommonErrorMessages.NOT_EXISTENT_VALIDATION_TYPE);
            }
        }
        return typesStr;
    }

}
