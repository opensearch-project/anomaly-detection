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

package org.opensearch.ad.rest;

import static org.opensearch.ad.settings.AnomalyDetectorSettings.REQUEST_TIMEOUT;
import static org.opensearch.ad.util.RestHandlerUtils.DETECTOR_ID;
import static org.opensearch.ad.util.RestHandlerUtils.IF_PRIMARY_TERM;
import static org.opensearch.ad.util.RestHandlerUtils.IF_SEQ_NO;
import static org.opensearch.ad.util.RestHandlerUtils.START_JOB;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.ad.AnomalyDetectorExtension;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.model.AnomalyDetectorJob;
import org.opensearch.ad.model.DetectionDateRange;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.settings.EnabledSetting;
import org.opensearch.ad.transport.ADJobParameterAction;
import org.opensearch.ad.transport.ADJobRunnerAction;
import org.opensearch.ad.transport.AnomalyDetectorJobAction;
import org.opensearch.ad.transport.AnomalyDetectorJobRequest;
import org.opensearch.ad.transport.AnomalyDetectorJobResponse;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.Strings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.extensions.rest.ExtensionRestRequest;
import org.opensearch.extensions.rest.ExtensionRestResponse;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.jobscheduler.JobSchedulerPlugin;
import org.opensearch.jobscheduler.rest.request.GetJobDetailsRequest;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.sdk.BaseExtensionRestHandler;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.RouteHandler;
import org.opensearch.sdk.SDKClient.SDKRestClient;
import org.opensearch.sdk.SDKClusterService;
import org.opensearch.sdk.SDKNamedXContentRegistry;

import com.google.common.collect.ImmutableList;

/**
 * This class consists of the REST handler to handle request to start/stop AD job.
 */
public class RestAnomalyDetectorJobAction extends BaseExtensionRestHandler {

    private static final Logger LOG = LogManager.getLogger(RestAnomalyDetectorJobAction.class);

    public static final String AD_JOB_ACTION = "anomaly_detector_job_action";

    private ExtensionsRunner extensionsRunner;
    private SDKRestClient client;
    private SDKClusterService clusterService;
    private SDKNamedXContentRegistry namedXContentRegistry;
    private Settings settings;
    private volatile TimeValue requestTimeout;
    private boolean jobDetailsAreRegistered;

    public RestAnomalyDetectorJobAction(ExtensionsRunner extensionsRunner, SDKRestClient client) {
        this.extensionsRunner = extensionsRunner;
        this.client = client;
        this.clusterService = extensionsRunner.getSdkClusterService();
        this.namedXContentRegistry = extensionsRunner.getNamedXContentRegistry();
        this.settings = extensionsRunner.getEnvironmentSettings();
        this.requestTimeout = REQUEST_TIMEOUT.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(REQUEST_TIMEOUT, it -> requestTimeout = it);
        this.jobDetailsAreRegistered = false;
    }

    public String getName() {
        return AD_JOB_ACTION;
    }

    @Override
    public List<RouteHandler> routeHandlers() {
        return ImmutableList
            .of(
                // start AD Job
                new RouteHandler(
                    RestRequest.Method.POST,
                    String.format(Locale.ROOT, "%s/{%s}/%s", AnomalyDetectorExtension.AD_BASE_DETECTORS_URI, DETECTOR_ID, START_JOB),
                    handleRequest
                )
            );
    }

    private Function<ExtensionRestRequest, ExtensionRestResponse> handleRequest = (request) -> {
        try {
            return prepareRequest(request);
        } catch (Exception e) {
            return exceptionalRequest(request, e);
        }
    };

    private void registerJobDetails() throws IOException {

        XContentBuilder requestBody = JsonXContent.contentBuilder();
        requestBody.startObject();
        requestBody.field(GetJobDetailsRequest.JOB_INDEX, AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX);
        requestBody.field(GetJobDetailsRequest.JOB_TYPE, AnomalyDetectorExtension.AD_JOB_TYPE);
        requestBody.field(GetJobDetailsRequest.JOB_PARAMETER_ACTION, ADJobParameterAction.NAME);
        requestBody.field(GetJobDetailsRequest.JOB_RUNNER_ACTION, ADJobRunnerAction.NAME);
        requestBody.field(GetJobDetailsRequest.EXTENSION_UNIQUE_ID, extensionsRunner.getUniqueId());
        requestBody.endObject();

        Request request = new Request("PUT", String.format(Locale.ROOT, "%s/%s", JobSchedulerPlugin.JS_BASE_URI, "_job_details"));
        request.setJsonEntity(Strings.toString(requestBody));

        Response response = client.performRequest(request);
        this.jobDetailsAreRegistered = RestStatus.fromCode(response.getStatusLine().getStatusCode()) == RestStatus.OK ? true : false;
        LOG.info("Job Details Registered : " + jobDetailsAreRegistered);
    }

    protected ExtensionRestResponse prepareRequest(ExtensionRestRequest request) throws IOException {
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new IllegalStateException(CommonErrorMessages.DISABLED_ERR_MSG);
        }

        // Ensure job details are registered with Job Scheduler prior to creating a job
        if (!jobDetailsAreRegistered) {
            registerJobDetails();
        }

        String detectorId = request.param(DETECTOR_ID);
        long seqNo = request.paramAsLong(IF_SEQ_NO, SequenceNumbers.UNASSIGNED_SEQ_NO);
        long primaryTerm = request.paramAsLong(IF_PRIMARY_TERM, SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
        // Passed false until historical analysis workflow is enabled
        boolean historical = false;
        String rawPath = request.path();
        DetectionDateRange detectionDateRange = parseDetectionDateRange(request);

        AnomalyDetectorJobRequest anomalyDetectorJobRequest = new AnomalyDetectorJobRequest(
            detectorId,
            detectionDateRange,
            historical,
            seqNo,
            primaryTerm,
            rawPath
        );

        // Execute anomaly detector job transport action
        CompletableFuture<AnomalyDetectorJobResponse> adJobFutureResponse = new CompletableFuture<>();
        client
            .execute(
                AnomalyDetectorJobAction.INSTANCE,
                anomalyDetectorJobRequest,
                ActionListener
                    .wrap(adJobResponse -> adJobFutureResponse.complete(adJobResponse), ex -> adJobFutureResponse.completeExceptionally(ex))
            );

        // Retrieve and return AD Job response
        AnomalyDetectorJobResponse response = adJobFutureResponse
            .orTimeout(AnomalyDetectorSettings.REQUEST_TIMEOUT.get(settings).getMillis(), TimeUnit.MILLISECONDS)
            .join();
        return new ExtensionRestResponse(
            request,
            RestStatus.OK,
            response.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS)
        );
    }

    private DetectionDateRange parseDetectionDateRange(ExtensionRestRequest request) throws IOException {
        if (!request.hasContent()) {
            return null;
        }
        XContentParser parser = request.contentParser(namedXContentRegistry.getRegistry());
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        DetectionDateRange dateRange = DetectionDateRange.parse(parser);
        return dateRange;
    }

    /*
    @Override
    public List<Route> routes() {
        return ImmutableList.of();
    }
    
    @Override
    public List<ReplacedRoute> replacedRoutes() {
        return ImmutableList
            .of(
                // start AD Job
                new ReplacedRoute(
                    RestRequest.Method.POST,
                    String.format(Locale.ROOT, "%s/{%s}/%s", AnomalyDetectorPlugin.AD_BASE_DETECTORS_URI, DETECTOR_ID, START_JOB),
                    RestRequest.Method.POST,
                    String.format(Locale.ROOT, "%s/{%s}/%s", AnomalyDetectorPlugin.LEGACY_OPENDISTRO_AD_BASE_URI, DETECTOR_ID, START_JOB)
                ),
                // stop AD Job
                new ReplacedRoute(
                    RestRequest.Method.POST,
                    String.format(Locale.ROOT, "%s/{%s}/%s", AnomalyDetectorPlugin.AD_BASE_DETECTORS_URI, DETECTOR_ID, STOP_JOB),
                    RestRequest.Method.POST,
                    String.format(Locale.ROOT, "%s/{%s}/%s", AnomalyDetectorPlugin.LEGACY_OPENDISTRO_AD_BASE_URI, DETECTOR_ID, STOP_JOB)
                )
            );
    }
    */
}
