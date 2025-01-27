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

package org.opensearch.timeseries;

import static org.apache.hc.core5.http.ContentType.APPLICATION_JSON;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.cluster.node.DiscoveryNodeRole.BUILT_IN_ROLES;
import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;
import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.opensearch.test.OpenSearchTestCase.buildNewFakeTransportAddress;
import static org.opensearch.test.OpenSearchTestCase.randomAlphaOfLength;
import static org.opensearch.test.OpenSearchTestCase.randomBoolean;
import static org.opensearch.test.OpenSearchTestCase.randomDouble;
import static org.opensearch.test.OpenSearchTestCase.randomDoubleBetween;
import static org.opensearch.test.OpenSearchTestCase.randomFloat;
import static org.opensearch.test.OpenSearchTestCase.randomInt;
import static org.opensearch.test.OpenSearchTestCase.randomIntBetween;
import static org.opensearch.test.OpenSearchTestCase.randomLong;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.apache.lucene.search.TotalHits;
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.opensearch.action.admin.indices.mapping.get.GetFieldMappingsResponse.FieldMappingMetadata;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.ml.ThresholdingResult;
import org.opensearch.ad.mock.model.MockSimpleLog;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskType;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyDetectorExecutionInput;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.model.AnomalyResultBucket;
import org.opensearch.ad.model.DetectorInternalState;
import org.opensearch.ad.model.ExpectedValueList;
import org.opensearch.ad.model.Rule;
import org.opensearch.ad.ratelimit.ADResultWriteRequest;
import org.opensearch.client.AdminClient;
import org.opensearch.client.Client;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.client.WarningsHandler;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.Priority;
import org.opensearch.common.Randomness;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.forecast.model.ForecastTask;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.forecast.ratelimit.ForecastResultWriteRequest;
import org.opensearch.index.get.GetResult;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.jobscheduler.spi.schedule.Schedule;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchModule;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.search.profile.SearchProfileShardResults;
import org.opensearch.search.suggest.Suggest;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.rest.OpenSearchRestTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.constant.CommonValue;
import org.opensearch.timeseries.dataprocessor.ImputationMethod;
import org.opensearch.timeseries.dataprocessor.ImputationOption;
import org.opensearch.timeseries.feature.Features;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.ConfigValidationIssue;
import org.opensearch.timeseries.model.DataByFeatureId;
import org.opensearch.timeseries.model.DateRange;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.Feature;
import org.opensearch.timeseries.model.FeatureData;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.model.TaskState;
import org.opensearch.timeseries.model.TimeConfiguration;
import org.opensearch.timeseries.model.ValidationAspect;
import org.opensearch.timeseries.model.ValidationIssueType;
import org.opensearch.timeseries.ratelimit.RequestPriority;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class TestHelpers {

    public static final String LEGACY_OPENDISTRO_AD_BASE_DETECTORS_URI = "/_opendistro/_anomaly_detection/detectors";
    public static final String AD_BASE_DETECTORS_URI = "/_plugins/_anomaly_detection/detectors";
    public static final String AD_BASE_RESULT_URI = AD_BASE_DETECTORS_URI + "/results";
    public static final String AD_BASE_PREVIEW_URI = AD_BASE_DETECTORS_URI + "/%s/_preview";
    public static final String AD_BASE_STATS_URI = "/_plugins/_anomaly_detection/stats";
    public static final String AD_BASE_START_DETECTOR_URL = AD_BASE_DETECTORS_URI + "/%s/_start";
    public static ImmutableSet<String> HISTORICAL_ANALYSIS_RUNNING_STATS = ImmutableSet
        .of(TaskState.CREATED.name(), TaskState.INIT.name(), TaskState.RUNNING.name());
    // Task may fail if memory circuit breaker triggered.
    public static final Set<String> HISTORICAL_ANALYSIS_FINISHED_FAILED_STATS = ImmutableSet
        .of(TaskState.FINISHED.name(), TaskState.FAILED.name());
    public static ImmutableSet<String> HISTORICAL_ANALYSIS_DONE_STATS = ImmutableSet
        .of(TaskState.FAILED.name(), TaskState.FINISHED.name(), TaskState.STOPPED.name());
    private static final Logger logger = LogManager.getLogger(TestHelpers.class);
    public static final Random random = new Random(42);

    public static Response makeRequest(
        RestClient client,
        String method,
        String endpoint,
        Map<String, String> params,
        String jsonEntity,
        List<Header> headers
    ) throws IOException {
        HttpEntity httpEntity = Strings.isBlank(jsonEntity) ? null : new StringEntity(jsonEntity, ContentType.APPLICATION_JSON);
        return makeRequest(client, method, endpoint, params, httpEntity, headers);
    }

    public static Response makeRequest(
        RestClient client,
        String method,
        String endpoint,
        Map<String, String> params,
        HttpEntity entity,
        List<Header> headers
    ) throws IOException {
        return makeRequest(client, method, endpoint, params, entity, headers, false);
    }

    public static Response makeRequest(
        RestClient client,
        String method,
        String endpoint,
        Map<String, String> params,
        HttpEntity entity,
        List<Header> headers,
        boolean strictDeprecationMode
    ) throws IOException {
        Request request = new Request(method, endpoint);

        RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
        if (headers != null) {
            headers.forEach(header -> options.addHeader(header.getName(), header.getValue()));
        }
        options.setWarningsHandler(strictDeprecationMode ? WarningsHandler.STRICT : WarningsHandler.PERMISSIVE);
        request.setOptions(options.build());

        if (params != null) {
            params.entrySet().forEach(it -> request.addParameter(it.getKey(), it.getValue()));
        }
        if (entity != null) {
            request.setEntity(entity);
        }
        return client.performRequest(request);
    }

    public static String xContentBuilderToString(XContentBuilder builder) {
        return BytesReference.bytes(builder).utf8ToString();
    }

    public static XContentBuilder builder() throws IOException {
        return XContentBuilder.builder(XContentType.JSON.xContent());
    }

    public static XContentParser parser(String xc) throws IOException {
        return parser(xc, true);
    }

    public static XContentParser parser(String xc, boolean skipFirstToken) throws IOException {
        XContentParser parser = XContentType.JSON.xContent().createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, xc);
        if (skipFirstToken) {
            parser.nextToken();
        }
        return parser;
    }

    public static Map<String, Object> XContentBuilderToMap(XContentBuilder builder) {
        return XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
    }

    public static NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    public static AnomalyDetector randomAnomalyDetector(Map<String, Object> uiMetadata, Instant lastUpdateTime) throws IOException {
        return randomAnomalyDetector(ImmutableList.of(randomFeature()), uiMetadata, lastUpdateTime, null);
    }

    public static AnomalyDetector randomAnomalyDetector(Map<String, Object> uiMetadata, Instant lastUpdateTime, boolean featureEnabled)
        throws IOException {
        return randomAnomalyDetector(ImmutableList.of(randomFeature(featureEnabled)), uiMetadata, lastUpdateTime, null);
    }

    public static AnomalyDetector randomAnomalyDetector(List<Feature> features, Map<String, Object> uiMetadata, Instant lastUpdateTime)
        throws IOException {
        return randomAnomalyDetector(features, uiMetadata, lastUpdateTime, null);
    }

    public static AnomalyDetector randomAnomalyDetector(
        List<Feature> features,
        Map<String, Object> uiMetadata,
        Instant lastUpdateTime,
        String detectorType
    ) throws IOException {
        return randomAnomalyDetector(features, uiMetadata, lastUpdateTime, true, null);
    }

    public static AnomalyDetector randomAnomalyDetector(
        List<Feature> features,
        Map<String, Object> uiMetadata,
        Instant lastUpdateTime,
        boolean withUser,
        List<String> categoryFields
    ) throws IOException {
        return randomAnomalyDetector(
            ImmutableList.of(randomAlphaOfLength(10).toLowerCase(Locale.ROOT)),
            features,
            uiMetadata,
            lastUpdateTime,
            OpenSearchRestTestCase.randomLongBetween(1, 1000),
            withUser,
            categoryFields
        );
    }

    public static AnomalyDetector randomAnomalyDetector(
        List<String> indices,
        List<Feature> features,
        Map<String, Object> uiMetadata,
        Instant lastUpdateTime,
        long detectionIntervalInMinutes,
        boolean withUser,
        List<String> categoryFields
    ) throws IOException {
        User user = withUser ? randomUser() : null;
        return new AnomalyDetector(
            randomAlphaOfLength(10),
            randomLong(),
            randomAlphaOfLength(20),
            randomAlphaOfLength(30),
            randomAlphaOfLength(5),
            indices,
            features,
            randomQuery(),
            new IntervalTimeConfiguration(detectionIntervalInMinutes, ChronoUnit.MINUTES),
            randomIntervalTimeConfiguration(),
            // our test's heap allowance is very small (20 MB heap usage would cause OOM)
            // reduce size to not cause issue.
            randomIntBetween(1, 20),
            uiMetadata,
            randomInt(),
            lastUpdateTime,
            categoryFields,
            user,
            null,
            TestHelpers.randomImputationOption(features),
            // timeDecay (reverse of recencyEmphasis) should be less than 1.
            // so we start with 2.
            randomIntBetween(2, 10000),
            randomIntBetween(1, TimeSeriesSettings.MAX_SHINGLE_SIZE * 2),
            randomIntBetween(1, 1000),
            new ArrayList<Rule>(),
            null,
            null,
            null,
            null,
            lastUpdateTime
        );
    }

    public static AnomalyDetector randomDetector(List<Feature> features, String indexName, int detectionIntervalInMinutes, String timeField)
        throws IOException {
        return randomDetector(features, indexName, detectionIntervalInMinutes, timeField, null);
    }

    public static AnomalyDetector randomDetector(
        List<Feature> features,
        String indexName,
        int detectionIntervalInMinutes,
        String timeField,
        List<String> categoryFields
    ) throws IOException {
        return randomDetector(features, indexName, detectionIntervalInMinutes, timeField, categoryFields, null);
    }

    public static AnomalyDetector randomDetector(
        List<Feature> features,
        String indexName,
        int detectionIntervalInMinutes,
        String timeField,
        List<String> categoryFields,
        String resultIndex
    ) throws IOException {
        return new AnomalyDetector(
            randomAlphaOfLength(10),
            randomLong(),
            randomAlphaOfLength(20),
            randomAlphaOfLength(30),
            timeField,
            ImmutableList.of(indexName),
            features,
            randomQuery("{\"bool\":{\"filter\":[{\"exists\":{\"field\":\"value\"}}]}}"),
            new IntervalTimeConfiguration(detectionIntervalInMinutes, ChronoUnit.MINUTES),
            new IntervalTimeConfiguration(OpenSearchRestTestCase.randomLongBetween(1, 5), ChronoUnit.MINUTES),
            8,
            null,
            randomInt(),
            Instant.now(),
            categoryFields,
            null,
            resultIndex,
            TestHelpers.randomImputationOption(features),
            // timeDecay (reverse of recencyEmphasis) should be less than 1.
            // so we start with 2.
            randomIntBetween(2, 10000),
            randomInt(TimeSeriesSettings.MAX_SHINGLE_SIZE / 2),
            randomIntBetween(1, 1000),
            null,
            null,
            null,
            null,
            null,
            Instant.now()
        );
    }

    public static DateRange randomDetectionDateRange() {
        return new DateRange(
            Instant.now().truncatedTo(ChronoUnit.SECONDS).minus(10, ChronoUnit.DAYS),
            Instant.now().truncatedTo(ChronoUnit.SECONDS)
        );
    }

    public static AnomalyDetector randomAnomalyDetectorUsingCategoryFields(String detectorId, List<String> categoryFields)
        throws IOException {
        return randomAnomalyDetectorUsingCategoryFields(
            detectorId,
            randomAlphaOfLength(5),
            ImmutableList.of(randomAlphaOfLength(10).toLowerCase(Locale.ROOT)),
            categoryFields
        );
    }

    public static AnomalyDetector randomAnomalyDetectorUsingCategoryFields(
        String detectorId,
        String timeField,
        List<String> indices,
        List<String> categoryFields
    ) throws IOException {
        return randomAnomalyDetectorUsingCategoryFields(detectorId, timeField, indices, categoryFields, null);
    }

    public static AnomalyDetector randomAnomalyDetectorUsingCategoryFields(
        String detectorId,
        String timeField,
        List<String> indices,
        List<String> categoryFields,
        String resultIndex
    ) throws IOException {
        List<Feature> features = ImmutableList.of(randomFeature(true));
        return new AnomalyDetector(
            detectorId,
            randomLong(),
            randomAlphaOfLength(20),
            randomAlphaOfLength(30),
            timeField,
            indices,
            features,
            randomQuery(),
            randomIntervalTimeConfiguration(),
            new IntervalTimeConfiguration(0, ChronoUnit.MINUTES),
            randomIntBetween(1, TimeSeriesSettings.MAX_SHINGLE_SIZE),
            null,
            randomInt(),
            Instant.now(),
            categoryFields,
            randomUser(),
            resultIndex,
            TestHelpers.randomImputationOption(features),
            // timeDecay (reverse of recencyEmphasis) should be less than 1.
            // so we start with 2.
            randomIntBetween(2, 10000),
            randomInt(TimeSeriesSettings.MAX_SHINGLE_SIZE / 2),
            randomIntBetween(1, 1000),
            null,
            null,
            null,
            null,
            null,
            Instant.now()
        );
    }

    public static AnomalyDetector randomAnomalyDetector(List<Feature> features) throws IOException {
        return randomAnomalyDetector(randomAlphaOfLength(5), randomAlphaOfLength(10).toLowerCase(Locale.ROOT), features);
    }

    public static AnomalyDetector randomAnomalyDetector(String timefield, String indexName) throws IOException {
        return randomAnomalyDetector(timefield, indexName, ImmutableList.of(randomFeature(true)));
    }

    public static AnomalyDetector randomAnomalyDetector(String timefield, String indexName, List<Feature> features) throws IOException {
        return new AnomalyDetector(
            randomAlphaOfLength(10),
            randomLong(),
            randomAlphaOfLength(20),
            randomAlphaOfLength(30),
            timefield,
            ImmutableList.of(indexName.toLowerCase(Locale.ROOT)),
            features,
            randomQuery(),
            randomIntervalTimeConfiguration(),
            randomIntervalTimeConfiguration(),
            randomIntBetween(1, TimeSeriesSettings.MAX_SHINGLE_SIZE),
            null,
            randomInt(),
            Instant.now(),
            null,
            randomUser(),
            null,
            TestHelpers.randomImputationOption(features),
            // timeDecay (reverse of recencyEmphasis) should be less than 1.
            // so we start with 2.
            randomIntBetween(2, 10000),
            randomInt(TimeSeriesSettings.MAX_SHINGLE_SIZE / 2),
            randomIntBetween(1, 1000),
            null,
            null,
            null,
            null,
            null,
            Instant.now()
        );
    }

    public static AnomalyDetector randomAnomalyDetectorWithFlattenResultIndex(String timefield, String indexName, List<Feature> features)
        throws IOException {
        return new AnomalyDetector(
            randomAlphaOfLength(10),
            randomLong(),
            randomAlphaOfLength(20),
            randomAlphaOfLength(30),
            timefield,
            ImmutableList.of(indexName.toLowerCase(Locale.ROOT)),
            features,
            randomQuery(),
            randomIntervalTimeConfiguration(),
            randomIntervalTimeConfiguration(),
            randomIntBetween(1, TimeSeriesSettings.MAX_SHINGLE_SIZE),
            null,
            randomInt(),
            Instant.now(),
            null,
            randomUser(),
            ADCommonName.CUSTOM_RESULT_INDEX_PREFIX + "test",
            TestHelpers.randomImputationOption(features),
            // timeDecay (reverse of recencyEmphasis) should be less than 1.
            // so we start with 2.
            randomIntBetween(2, 10000),
            randomInt(TimeSeriesSettings.MAX_SHINGLE_SIZE / 2),
            randomIntBetween(1, 1000),
            null,
            null,
            null,
            null,
            true,
            Instant.now()
        );
    }

    public static AnomalyDetector randomAnomalyDetectorWithEmptyFeature() throws IOException {
        return new AnomalyDetector(
            randomAlphaOfLength(10),
            randomLong(),
            randomAlphaOfLength(20),
            randomAlphaOfLength(30),
            randomAlphaOfLength(5),
            ImmutableList.of(randomAlphaOfLength(10).toLowerCase(Locale.ROOT)),
            ImmutableList.of(),
            randomQuery(),
            randomIntervalTimeConfiguration(),
            randomIntervalTimeConfiguration(),
            randomIntBetween(1, TimeSeriesSettings.MAX_SHINGLE_SIZE),
            null,
            randomInt(),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            null,
            randomUser(),
            null,
            null,
            // timeDecay (reverse of recencyEmphasis) should be less than 1.
            // so we start with 2.
            randomIntBetween(2, 10000),
            randomInt(TimeSeriesSettings.MAX_SHINGLE_SIZE / 2),
            randomIntBetween(1, 1000),
            null,
            null,
            null,
            null,
            null,
            Instant.now().truncatedTo(ChronoUnit.SECONDS)
        );
    }

    public static AnomalyDetector randomAnomalyDetectorWithInterval(TimeConfiguration interval) throws IOException {
        return randomAnomalyDetectorWithInterval(interval, false);
    }

    public static AnomalyDetector randomAnomalyDetectorWithInterval(TimeConfiguration interval, boolean hcDetector) throws IOException {
        List<String> categoryField = hcDetector ? ImmutableList.of(randomAlphaOfLength(5)) : null;
        Feature feature = randomFeature();
        List<Feature> featureList = ImmutableList.of(feature);
        return new AnomalyDetector(
            randomAlphaOfLength(10),
            randomLong(),
            randomAlphaOfLength(20),
            randomAlphaOfLength(30),
            randomAlphaOfLength(5),
            ImmutableList.of(randomAlphaOfLength(10).toLowerCase(Locale.ROOT)),
            featureList,
            randomQuery(),
            interval,
            randomIntervalTimeConfiguration(),
            randomIntBetween(1, TimeSeriesSettings.MAX_SHINGLE_SIZE),
            null,
            randomInt(),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            categoryField,
            randomUser(),
            null,
            TestHelpers.randomImputationOption(featureList),
            // timeDecay (reverse of recencyEmphasis) should be less than 1.
            // so we start with 2.
            randomIntBetween(2, 10000),
            randomInt(TimeSeriesSettings.MAX_SHINGLE_SIZE / 2),
            randomIntBetween(1, 1000),
            null,
            null,
            null,
            null,
            null,
            Instant.now().truncatedTo(ChronoUnit.SECONDS)
        );
    }

    public static AnomalyResultBucket randomAnomalyResultBucket() {
        Map<String, Object> map = new HashMap<>();
        map.put(randomAlphaOfLength(5), randomAlphaOfLength(5));
        return new AnomalyResultBucket(map, randomInt(), randomDouble());
    }

    public static class AnomalyDetectorBuilder {
        private String detectorId = randomAlphaOfLength(10);
        private Long version = randomLong();
        private String name = randomAlphaOfLength(20);
        private String description = randomAlphaOfLength(30);
        private String timeField = randomAlphaOfLength(5);
        private List<String> indices = ImmutableList.of(randomAlphaOfLength(10).toLowerCase(Locale.ROOT));
        private List<Feature> featureAttributes = ImmutableList.of(randomFeature(true));
        private QueryBuilder filterQuery;
        private TimeConfiguration detectionInterval = randomIntervalTimeConfiguration();
        private TimeConfiguration windowDelay = randomIntervalTimeConfiguration();
        private Integer shingleSize = randomIntBetween(1, TimeSeriesSettings.MAX_SHINGLE_SIZE);
        private Map<String, Object> uiMetadata = null;
        private Integer schemaVersion = randomInt();
        private Instant lastUpdateTime = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        private List<String> categoryFields = null;
        private User user = randomUser();
        private String resultIndex = null;
        private ImputationOption imputationOption = null;
        private List<Rule> rules = null;

        // transform decay (reverse of recencyEmphasis) has to be [0, 1). So we cannot use 1.
        private int recencyEmphasis = randomIntBetween(2, 10000);

        public static AnomalyDetectorBuilder newInstance(int numberOfFeatures) throws IOException {
            return new AnomalyDetectorBuilder(numberOfFeatures);
        }

        private AnomalyDetectorBuilder(int numberOfFeatures) throws IOException {
            filterQuery = randomQuery();
            List<Feature> featureAttributes = new ArrayList<>();
            for (int i = 0; i < numberOfFeatures; i++) {
                featureAttributes.add(randomFeature(true));
            }
            this.featureAttributes = featureAttributes;
        }

        public AnomalyDetectorBuilder setDetectorId(String detectorId) {
            this.detectorId = detectorId;
            return this;
        }

        public AnomalyDetectorBuilder setVersion(Long version) {
            this.version = version;
            return this;
        }

        public AnomalyDetectorBuilder setName(String name) {
            this.name = name;
            return this;
        }

        public AnomalyDetectorBuilder setDescription(String description) {
            this.description = description;
            return this;
        }

        public AnomalyDetectorBuilder setTimeField(String timeField) {
            this.timeField = timeField;
            return this;
        }

        public AnomalyDetectorBuilder setIndices(List<String> indices) {
            this.indices = indices;
            return this;
        }

        public AnomalyDetectorBuilder setFeatureAttributes(List<Feature> featureAttributes) {
            this.featureAttributes = featureAttributes;
            return this;
        }

        public AnomalyDetectorBuilder setFilterQuery(QueryBuilder filterQuery) {
            this.filterQuery = filterQuery;
            return this;
        }

        public AnomalyDetectorBuilder setDetectionInterval(TimeConfiguration detectionInterval) {
            this.detectionInterval = detectionInterval;
            return this;
        }

        public AnomalyDetectorBuilder setWindowDelay(TimeConfiguration windowDelay) {
            this.windowDelay = windowDelay;
            return this;
        }

        public AnomalyDetectorBuilder setShingleSize(Integer shingleSize) {
            this.shingleSize = shingleSize;
            return this;
        }

        public AnomalyDetectorBuilder setUiMetadata(Map<String, Object> uiMetadata) {
            this.uiMetadata = uiMetadata;
            return this;
        }

        public AnomalyDetectorBuilder setSchemaVersion(Integer schemaVersion) {
            this.schemaVersion = schemaVersion;
            return this;
        }

        public AnomalyDetectorBuilder setLastUpdateTime(Instant lastUpdateTime) {
            this.lastUpdateTime = lastUpdateTime;
            return this;
        }

        public AnomalyDetectorBuilder setCategoryFields(List<String> categoryFields) {
            this.categoryFields = categoryFields;
            return this;
        }

        public AnomalyDetectorBuilder setUser(User user) {
            this.user = user;
            return this;
        }

        public AnomalyDetectorBuilder setResultIndex(String resultIndex) {
            this.resultIndex = resultIndex;
            return this;
        }

        public AnomalyDetectorBuilder setImputationOption(ImputationMethod method, Map<String, Double> defaultFill) {
            this.imputationOption = new ImputationOption(method, defaultFill);
            return this;
        }

        public AnomalyDetectorBuilder setRules(List<Rule> rules) {
            this.rules = rules;
            return this;
        }

        public AnomalyDetectorBuilder setRecencyEmphasis(int recencyEmphasis) {
            this.recencyEmphasis = recencyEmphasis;
            return this;
        }

        public AnomalyDetector build() {
            return new AnomalyDetector(
                detectorId,
                version,
                name,
                description,
                timeField,
                indices,
                featureAttributes,
                filterQuery,
                detectionInterval,
                windowDelay,
                shingleSize,
                uiMetadata,
                schemaVersion,
                lastUpdateTime,
                categoryFields,
                user,
                resultIndex,
                imputationOption,
                recencyEmphasis,
                randomIntBetween(1, TimeSeriesSettings.MAX_SHINGLE_SIZE * 2),
                // make history intervals at least TimeSeriesSettings.NUM_MIN_SAMPLES.
                // Otherwise, tests like EntityColdStarterTests.testTwoSegments may fail
                // as ModelColdStart.selectNumberOfSamples will select the smaller of
                // 32 and historical intervals.
                randomIntBetween(TimeSeriesSettings.NUM_MIN_SAMPLES, 1000),
                rules,
                null,
                null,
                null,
                null,
                lastUpdateTime
            );
        }
    }

    public static AnomalyDetector randomAnomalyDetectorWithInterval(TimeConfiguration interval, boolean hcDetector, boolean featureEnabled)
        throws IOException {
        List<String> categoryField = hcDetector ? ImmutableList.of(randomAlphaOfLength(5)) : null;
        List<Feature> features = ImmutableList.of(randomFeature(featureEnabled));
        return new AnomalyDetector(
            randomAlphaOfLength(10),
            randomLong(),
            randomAlphaOfLength(20),
            randomAlphaOfLength(30),
            randomAlphaOfLength(5),
            ImmutableList.of(randomAlphaOfLength(10).toLowerCase(Locale.ROOT)),
            features,
            randomQuery(),
            interval,
            randomIntervalTimeConfiguration(),
            randomIntBetween(1, TimeSeriesSettings.MAX_SHINGLE_SIZE),
            null,
            randomInt(),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            categoryField,
            randomUser(),
            null,
            TestHelpers.randomImputationOption(features),
            // timeDecay (reverse of recencyEmphasis) should be less than 1.
            // so we start with 2.
            randomIntBetween(2, 10000),
            randomInt(TimeSeriesSettings.MAX_SHINGLE_SIZE / 2),
            randomIntBetween(1, 1000),
            null,
            null,
            null,
            null,
            null,
            Instant.now().truncatedTo(ChronoUnit.SECONDS)
        );
    }

    public static SearchSourceBuilder randomFeatureQuery() throws IOException {
        String query = "{\"query\":{\"match\":{\"user\":{\"query\":\"kimchy\",\"operator\":\"OR\",\"prefix_length\":0,"
            + "\"max_expansions\":50,\"fuzzy_transpositions\":true,\"lenient\":false,\"zero_terms_query\":\"NONE\","
            + "\"auto_generate_synonyms_phrase_query\":true,\"boost\":1}}}}";
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        XContentParser parser = XContentType.JSON
            .xContent()
            .createParser(new NamedXContentRegistry(searchModule.getNamedXContents()), LoggingDeprecationHandler.INSTANCE, query);
        searchSourceBuilder.parseXContent(parser);
        return searchSourceBuilder;
    }

    public static QueryBuilder randomQuery() throws IOException {
        String query = "{\"bool\":{\"must\":{\"term\":{\"user\":\"kimchy\"}},\"filter\":{\"term\":{\"tag\":"
            + "\"tech\"}},\"must_not\":{\"range\":{\"age\":{\"gte\":10,\"lte\":20}}},\"should\":[{\"term\":"
            + "{\"tag\":\"wow\"}},{\"term\":{\"tag\":\"elasticsearch\"}}],\"minimum_should_match\":1,\"boost\":1}}";
        return randomQuery(query);
    }

    public static QueryBuilder randomQuery(String query) throws IOException {
        XContentParser parser = TestHelpers.parser(query);
        return parseInnerQueryBuilder(parser);
    }

    public static AggregationBuilder randomAggregation() throws IOException {
        return randomAggregation(randomAlphaOfLength(5));
    }

    public static AggregationBuilder randomAggregation(String aggregationName) throws IOException {
        XContentParser parser = parser("{\"" + aggregationName + "\":{\"value_count\":{\"field\":\"ok\"}}}");

        AggregatorFactories.Builder parsed = AggregatorFactories.parseAggregators(parser);
        return parsed.getAggregatorFactories().iterator().next();
    }

    /**
     * Parse string aggregation query into {@link AggregationBuilder}
     * Sample input:
     * "{\"test\":{\"value_count\":{\"field\":\"ok\"}}}"
     *
     * @param aggregationQuery aggregation builder
     * @return aggregation builder
     * @throws IOException IO exception
     */
    public static AggregationBuilder parseAggregation(String aggregationQuery) throws IOException {
        XContentParser parser = parser(aggregationQuery);

        AggregatorFactories.Builder parsed = AggregatorFactories.parseAggregators(parser);
        return parsed.getAggregatorFactories().iterator().next();
    }

    public static Map<String, Object> randomUiMetadata() {
        return ImmutableMap.of(randomAlphaOfLength(5), randomFeature());
    }

    public static TimeConfiguration randomIntervalTimeConfiguration() {
        return new IntervalTimeConfiguration(OpenSearchRestTestCase.randomLongBetween(1, 1000), ChronoUnit.MINUTES);
    }

    public static IntervalSchedule randomIntervalSchedule() {
        return new IntervalSchedule(
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            OpenSearchRestTestCase.randomIntBetween(1, 1000),
            ChronoUnit.MINUTES
        );
    }

    public static Feature randomFeature() {
        return randomFeature(randomAlphaOfLength(5), randomAlphaOfLength(5));
    }

    public static Feature randomFeature(String featureName, String aggregationName) {
        return randomFeature(featureName, aggregationName, randomBoolean());
    }

    public static Feature randomFeature(boolean enabled) {
        return randomFeature(randomAlphaOfLength(5), randomAlphaOfLength(5), enabled);
    }

    public static Feature randomFeature(String featureName, String aggregationName, boolean enabled) {
        AggregationBuilder testAggregation = null;
        try {
            testAggregation = randomAggregation(aggregationName);
        } catch (IOException e) {
            logger.error("Fail to generate test aggregation");
            throw new RuntimeException();
        }
        return new Feature(randomAlphaOfLength(5), featureName, enabled, testAggregation);
    }

    public static Feature randomFeature(String featureName, String fieldName, String aggregationMethod, boolean enabled)
        throws IOException {
        XContentParser parser = parser("{\"" + featureName + "\":{\"" + aggregationMethod + "\":{\"field\":\"" + fieldName + "\"}}}");
        AggregatorFactories.Builder aggregators = AggregatorFactories.parseAggregators(parser);
        AggregationBuilder testAggregation = aggregators.getAggregatorFactories().iterator().next();
        return new Feature(randomAlphaOfLength(5), featureName, enabled, testAggregation);
    }

    public static Features randomFeatures() {
        List<Map.Entry<Long, Long>> ranges = Arrays.asList(new AbstractMap.SimpleEntry<>(0L, 1L));
        double[][] unprocessed = new double[][] { { randomDouble(), randomDouble() } };

        return new Features(ranges, unprocessed);
    }

    public static List<ThresholdingResult> randomThresholdingResults() {
        double grade = 1.;
        double confidence = 0.5;
        double score = 1.;

        ThresholdingResult thresholdingResult = new ThresholdingResult(grade, confidence, score);
        List<ThresholdingResult> results = new ArrayList<>();
        results.add(thresholdingResult);
        return results;
    }

    public static User randomUser() {
        return new User(
            randomAlphaOfLength(8),
            ImmutableList.of(randomAlphaOfLength(10)),
            ImmutableList.of("all_access"),
            ImmutableList.of("attribute=test")
        );
    }

    public static <S, T> void assertFailWith(Class<S> clazz, Callable<T> callable) throws Exception {
        assertFailWith(clazz, null, callable);
    }

    public static <S, T> void assertFailWith(Class<S> clazz, String message, Callable<T> callable) throws Exception {
        try {
            callable.call();
        } catch (Throwable e) {
            if (e.getClass() != clazz) {
                throw e;
            }
            if (message != null && !e.getMessage().contains(message)) {
                throw e;
            }
        }
    }

    public static FeatureData randomFeatureData() {
        return new FeatureData(randomAlphaOfLength(5), randomAlphaOfLength(5), randomDouble());
    }

    public static AnomalyResult randomAnomalyDetectResult() {
        return randomAnomalyDetectResult(randomDouble(), randomAlphaOfLength(5), null);
    }

    public static AnomalyResult randomAnomalyDetectResult(double score) {
        return randomAnomalyDetectResult(score, null, null);
    }

    public static AnomalyResult randomAnomalyDetectResult(String error) {
        return randomAnomalyDetectResult(Double.NaN, error, null);
    }

    public static AnomalyResult randomAnomalyDetectResult(double score, String error, String taskId) {
        return randomAnomalyDetectResult(score, error, taskId, true);
    }

    public static AnomalyResult randomAnomalyDetectResult(double score, String error, String taskId, boolean withUser) {
        User user = withUser ? randomUser() : null;
        List<DataByFeatureId> relavantAttribution = new ArrayList<DataByFeatureId>();
        relavantAttribution.add(new DataByFeatureId(randomAlphaOfLength(5), randomDoubleBetween(0, 1.0, true)));
        relavantAttribution.add(new DataByFeatureId(randomAlphaOfLength(5), randomDoubleBetween(0, 1.0, true)));

        List<DataByFeatureId> pastValues = new ArrayList<DataByFeatureId>();
        pastValues.add(new DataByFeatureId(randomAlphaOfLength(5), randomDouble()));
        pastValues.add(new DataByFeatureId(randomAlphaOfLength(5), randomDouble()));

        List<ExpectedValueList> expectedValuesList = new ArrayList<ExpectedValueList>();
        List<DataByFeatureId> expectedValues = new ArrayList<DataByFeatureId>();
        expectedValues.add(new DataByFeatureId(randomAlphaOfLength(5), randomDouble()));
        expectedValues.add(new DataByFeatureId(randomAlphaOfLength(5), randomDouble()));
        expectedValuesList.add(new ExpectedValueList(randomDoubleBetween(0, 1.0, true), expectedValues));

        return new AnomalyResult(
            randomAlphaOfLength(5),
            taskId,
            score,
            randomDouble(),
            randomDouble(),
            ImmutableList.of(randomFeatureData(), randomFeatureData()),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            error,
            Optional.empty(),
            user,
            CommonValue.NO_SCHEMA_VERSION,
            null,
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            relavantAttribution,
            pastValues,
            expectedValuesList,
            randomDoubleBetween(1.1, 10.0, true),
            null
        );
    }

    public static AnomalyResult randomHCADAnomalyDetectResult(double score, double grade) {
        return randomHCADAnomalyDetectResult(score, grade, null);
    }

    public static ADResultWriteRequest randomADResultWriteRequest(String detectorId, double score, double grade) {
        ADResultWriteRequest resultWriteRequest = new ADResultWriteRequest(
            Instant.now().plus(10, ChronoUnit.MINUTES).toEpochMilli(),
            detectorId,
            RequestPriority.MEDIUM,
            randomHCADAnomalyDetectResult(score, grade),
            null
        );
        return resultWriteRequest;
    }

    public static AnomalyResult randomHCADAnomalyDetectResult(double score, double grade, String error) {
        return randomHCADAnomalyDetectResult(null, null, score, grade, error, null, null);
    }

    public static AnomalyResult randomHCADAnomalyDetectResult(
        String detectorId,
        String taskId,
        double score,
        double grade,
        String error,
        Long startTimeEpochMillis,
        Long endTimeEpochMillis
    ) {
        return randomHCADAnomalyDetectResult(detectorId, taskId, null, score, grade, error, startTimeEpochMillis, endTimeEpochMillis);
    }

    public static AnomalyResult randomHCADAnomalyDetectResult(
        String detectorId,
        String taskId,
        Map<String, Object> entityAttrs,
        double score,
        double grade,
        String error,
        Long startTimeEpochMillis,
        Long endTimeEpochMillis
    ) {
        List<DataByFeatureId> relavantAttribution = new ArrayList<DataByFeatureId>();
        relavantAttribution.add(new DataByFeatureId(randomAlphaOfLength(5), randomDoubleBetween(0, 1.0, true)));
        relavantAttribution.add(new DataByFeatureId(randomAlphaOfLength(5), randomDoubleBetween(0, 1.0, true)));

        List<DataByFeatureId> pastValues = new ArrayList<DataByFeatureId>();
        pastValues.add(new DataByFeatureId(randomAlphaOfLength(5), randomDouble()));
        pastValues.add(new DataByFeatureId(randomAlphaOfLength(5), randomDouble()));

        List<ExpectedValueList> expectedValuesList = new ArrayList<ExpectedValueList>();
        List<DataByFeatureId> expectedValues = new ArrayList<DataByFeatureId>();
        expectedValues.add(new DataByFeatureId(randomAlphaOfLength(5), randomDouble()));
        expectedValues.add(new DataByFeatureId(randomAlphaOfLength(5), randomDouble()));
        expectedValuesList.add(new ExpectedValueList(randomDoubleBetween(0, 1.0, true), expectedValues));

        return new AnomalyResult(
            detectorId == null ? randomAlphaOfLength(5) : detectorId,
            taskId,
            score,
            grade,
            randomDouble(),
            ImmutableList.of(randomFeatureData(), randomFeatureData()),
            startTimeEpochMillis == null ? Instant.now().truncatedTo(ChronoUnit.SECONDS) : Instant.ofEpochMilli(startTimeEpochMillis),
            endTimeEpochMillis == null ? Instant.now().truncatedTo(ChronoUnit.SECONDS) : Instant.ofEpochMilli(endTimeEpochMillis),
            startTimeEpochMillis == null ? Instant.now().truncatedTo(ChronoUnit.SECONDS) : Instant.ofEpochMilli(startTimeEpochMillis),
            endTimeEpochMillis == null ? Instant.now().truncatedTo(ChronoUnit.SECONDS) : Instant.ofEpochMilli(endTimeEpochMillis),
            error,
            entityAttrs == null
                ? Optional.ofNullable(Entity.createSingleAttributeEntity(randomAlphaOfLength(5), randomAlphaOfLength(5)))
                : Optional.ofNullable(Entity.createEntityByReordering(entityAttrs)),
            randomUser(),
            CommonValue.NO_SCHEMA_VERSION,
            null,
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            relavantAttribution,
            pastValues,
            expectedValuesList,
            randomDoubleBetween(1.1, 10.0, true),
            null
        );
    }

    public static Job randomJob() {
        return randomJob(true);
    }

    public static Job randomJob(boolean enabled, Instant enabledTime, Instant disabledTime) {
        return new Job(
            randomAlphaOfLength(10),
            randomIntervalSchedule(),
            randomIntervalTimeConfiguration(),
            enabled,
            enabledTime,
            disabledTime,
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            60L,
            randomUser(),
            null,
            AnalysisType.AD
        );
    }

    public static Job randomJob(boolean enabled) {
        return randomJob(enabled, Instant.now().truncatedTo(ChronoUnit.SECONDS), Instant.now().truncatedTo(ChronoUnit.SECONDS));
    }

    public static AnomalyDetectorExecutionInput randomAnomalyDetectorExecutionInput() throws IOException {
        return new AnomalyDetectorExecutionInput(
            randomAlphaOfLength(5),
            Instant.now().minus(10, ChronoUnit.MINUTES).truncatedTo(ChronoUnit.SECONDS),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            randomAnomalyDetector(null, Instant.now().truncatedTo(ChronoUnit.SECONDS))
        );
    }

    public static ActionListener<CreateIndexResponse> createActionListener(
        CheckedConsumer<CreateIndexResponse, ? extends Exception> consumer,
        Consumer<Exception> failureConsumer
    ) {
        return ActionListener.wrap(consumer, failureConsumer);
    }

    public static void waitForIndexCreationToComplete(Client client, final String indexName) {
        ClusterHealthResponse clusterHealthResponse = client
            .admin()
            .cluster()
            .prepareHealth(indexName)
            .setWaitForEvents(Priority.URGENT)
            .get();
        logger.info("Status of " + indexName + ": " + clusterHealthResponse.getStatus());
    }

    public static ClusterService createClusterService(ThreadPool threadPool, ClusterSettings clusterSettings) {
        DiscoveryNode discoveryNode = new DiscoveryNode(
            "node",
            OpenSearchRestTestCase.buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            BUILT_IN_ROLES,
            Version.CURRENT
        );
        return ClusterServiceUtils.createClusterService(threadPool, discoveryNode, clusterSettings);
    }

    public static ClusterState createIndexBlockedState(String indexName, Settings hackedSettings, String alias) {
        ClusterState blockedClusterState = null;
        IndexMetadata.Builder builder = IndexMetadata.builder(indexName);
        if (alias != null) {
            builder.putAlias(AliasMetadata.builder(alias));
        }
        IndexMetadata indexMetaData = builder
            .settings(
                Settings
                    .builder()
                    .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(hackedSettings)
            )
            .build();
        Metadata metaData = Metadata.builder().put(indexMetaData, false).build();
        blockedClusterState = ClusterState
            .builder(new ClusterName("test cluster"))
            .metadata(metaData)
            .blocks(ClusterBlocks.builder().addBlocks(indexMetaData))
            .build();
        return blockedClusterState;
    }

    public static ThreadContext createThreadContext() {
        Settings build = Settings.builder().put("request.headers.default", "1").build();
        ThreadContext context = new ThreadContext(build);
        context.putHeader("foo", "bar");
        context.putTransient("x", 1);
        return context;
    }

    public static ThreadPool createThreadPool() {
        ThreadPool pool = mock(ThreadPool.class);
        when(pool.getThreadContext()).thenReturn(createThreadContext());
        return pool;
    }

    public static CreateIndexResponse createIndex(AdminClient adminClient, String indexName, String indexMapping) {
        CreateIndexRequest request = new CreateIndexRequest(indexName).mapping(indexMapping);
        return adminClient.indices().create(request).actionGet(10_000);
    }

    public static void createIndex(RestClient client, String indexName, HttpEntity data) throws IOException {
        TestHelpers
            .makeRequest(
                client,
                "POST",
                "/" + indexName + "/_doc/" + randomAlphaOfLength(5) + "?refresh=true",
                ImmutableMap.of(),
                data,
                null
            );
    }

    public static void createIndexWithTimeField(RestClient client, String indexName, String timeField) throws IOException {
        createIndexWithTimeField(client, indexName, timeField, false);
    }

    public static void createIndexWithTimeField(RestClient client, String indexName, String timeField, boolean useDateNanos)
        throws IOException {
        StringBuilder indexMappings = new StringBuilder();
        indexMappings.append("{\"properties\":{");
        if (useDateNanos) {
            indexMappings.append("\"" + timeField + "\":{\"type\":\"date_nanos\"}");
        } else {
            indexMappings.append("\"" + timeField + "\":{\"type\":\"date\"}");
        }
        indexMappings.append("}}");
        createIndex(client, indexName.toLowerCase(Locale.ROOT), TestHelpers.toHttpEntity("{\"name\": \"test\"}"));
        createIndexMapping(client, indexName.toLowerCase(Locale.ROOT), TestHelpers.toHttpEntity(indexMappings.toString()));
    }

    public static void createEmptyIndexWithTimeField(RestClient client, String indexName, String timeField) throws IOException {
        StringBuilder indexMappings = new StringBuilder();
        indexMappings.append("{\"properties\":{");
        indexMappings.append("\"" + timeField + "\":{\"type\":\"date\"}");
        indexMappings.append("}}");
        createEmptyIndex(client, indexName.toLowerCase(Locale.ROOT));
        createIndexMapping(client, indexName.toLowerCase(Locale.ROOT), TestHelpers.toHttpEntity(indexMappings.toString()));
    }

    public static void createIndexWithHCADFields(RestClient client, String indexName, Map<String, String> categoryFieldsAndTypes)
        throws IOException {
        StringBuilder indexMappings = new StringBuilder();
        indexMappings.append("{\"properties\":{");
        for (Map.Entry<String, String> entry : categoryFieldsAndTypes.entrySet()) {
            indexMappings.append("\"" + entry.getKey() + "\":{\"type\":\"" + entry.getValue() + "\"},");
        }
        indexMappings.append("\"timestamp\":{\"type\":\"date\"}");
        indexMappings.append("}}");
        createEmptyIndex(client, indexName);
        createIndexMapping(client, indexName, TestHelpers.toHttpEntity(indexMappings.toString()));
    }

    public static void createEmptyIndexMapping(RestClient client, String indexName, Map<String, String> fieldsAndTypes) throws IOException {
        StringBuilder indexMappings = new StringBuilder();
        indexMappings.append("{\"properties\":{");
        for (Map.Entry<String, String> entry : fieldsAndTypes.entrySet()) {
            indexMappings.append("\"" + entry.getKey() + "\":{\"type\":\"" + entry.getValue() + "\"},");
        }
        indexMappings.append("}}");
        createEmptyIndex(client, indexName);
        createIndexMapping(client, indexName, TestHelpers.toHttpEntity(indexMappings.toString()));
    }

    public static void createEmptyAnomalyResultIndex(RestClient client) throws IOException {
        createEmptyIndex(client, ADCommonName.ANOMALY_RESULT_INDEX_ALIAS);
        createIndexMapping(client, ADCommonName.ANOMALY_RESULT_INDEX_ALIAS, toHttpEntity(ADIndexManagement.getResultMappings()));
    }

    public static void createEmptyIndex(RestClient client, String indexName) throws IOException {
        TestHelpers.makeRequest(client, "PUT", "/" + indexName, ImmutableMap.of(), "", null);
    }

    public static void createIndexMapping(RestClient client, String indexName, HttpEntity mappings) throws IOException {
        TestHelpers.makeRequest(client, "POST", "/" + indexName + "/_mapping", ImmutableMap.of(), mappings, null);
    }

    public static void ingestDataToIndex(RestClient client, String indexName, HttpEntity data) throws IOException {
        TestHelpers
            .makeRequest(
                client,
                "POST",
                "/" + indexName + "/_doc/" + randomAlphaOfLength(5) + "?refresh=true",
                ImmutableMap.of(),
                data,
                null
            );
    }

    public static GetResponse createGetResponse(ToXContentObject o, String id, String indexName) throws IOException {
        XContentBuilder content = o.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS);
        return new GetResponse(
            new GetResult(
                indexName,
                id,
                UNASSIGNED_SEQ_NO,
                0,
                -1,
                true,
                BytesReference.bytes(content),
                Collections.emptyMap(),
                Collections.emptyMap()
            )
        );
    }

    public static GetResponse createBrokenGetResponse(String id, String indexName) throws IOException {
        ByteBuffer[] buffers = new ByteBuffer[0];
        return new GetResponse(
            new GetResult(
                indexName,
                id,
                UNASSIGNED_SEQ_NO,
                0,
                -1,
                true,
                BytesReference.fromByteBuffers(buffers),
                Collections.emptyMap(),
                Collections.emptyMap()
            )
        );
    }

    public static GetResponse createGetResponse(Map<String, ?> source, String id, String indexName) throws IOException {
        XContentBuilder xContent = XContentFactory.jsonBuilder();
        xContent.map(source);
        BytesReference documentSource = BytesReference.bytes(xContent);
        return new GetResponse(
            new GetResult(indexName, id, UNASSIGNED_SEQ_NO, 0, -1, true, documentSource, Collections.emptyMap(), Collections.emptyMap())
        );
    }

    public static SearchResponse createSearchResponse(ToXContentObject o) throws IOException {
        XContentBuilder content = o.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS);

        SearchHit[] hits = new SearchHit[1];
        hits[0] = new SearchHit(0).sourceRef(BytesReference.bytes(content));

        return new SearchResponse(
            new InternalSearchResponse(
                new SearchHits(hits, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0f),
                InternalAggregations.EMPTY,
                new Suggest(Collections.emptyList()),
                new SearchProfileShardResults(Collections.emptyMap()),
                false,
                false,
                1
            ),
            "",
            5,
            5,
            0,
            100,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );
    }

    public static SearchResponse createEmptySearchResponse() throws IOException {
        return new SearchResponse(
            new InternalSearchResponse(
                new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 1.0f),
                InternalAggregations.EMPTY,
                new Suggest(Collections.emptyList()),
                new SearchProfileShardResults(Collections.emptyMap()),
                false,
                false,
                1
            ),
            "",
            5,
            5,
            0,
            100,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );
    }

    public static DetectorInternalState randomDetectState(String error) {
        return randomDetectState(error, Instant.now());
    }

    public static DetectorInternalState randomDetectState(Instant lastUpdateTime) {
        return randomDetectState(randomAlphaOfLength(5), lastUpdateTime);
    }

    public static DetectorInternalState randomDetectState(String error, Instant lastUpdateTime) {
        return new DetectorInternalState.Builder().lastUpdateTime(lastUpdateTime).error(error).build();
    }

    public static Map<String, Map<String, GetFieldMappingsResponse.FieldMappingMetadata>> createFieldMappings(
        String index,
        String fieldName,
        String fieldType
    ) throws IOException {
        Map<String, Map<String, GetFieldMappingsResponse.FieldMappingMetadata>> mappings = new HashMap<>();
        FieldMappingMetadata fieldMappingMetadata = new FieldMappingMetadata(
            fieldName,
            new BytesArray("{\"" + fieldName + "\":{\"type\":\"" + fieldType + "\"}}")
        );
        mappings.put(index, Collections.singletonMap(fieldName, fieldMappingMetadata));
        return mappings;
    }

    public static ADTask randomAdTask() throws IOException {
        return randomAdTask(
            randomAlphaOfLength(5),
            TaskState.RUNNING,
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            randomAlphaOfLength(5),
            true
        );
    }

    public static ADTask randomAdTask(ADTaskType adTaskType) throws IOException {
        return randomAdTask(
            randomAlphaOfLength(5),
            TaskState.RUNNING,
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            randomAlphaOfLength(5),
            true,
            adTaskType
        );
    }

    public static ADTask randomAdTask(
        String taskId,
        TaskState state,
        Instant executionEndTime,
        String stoppedBy,
        String detectorId,
        AnomalyDetector detector,
        ADTaskType adTaskType
    ) {
        executionEndTime = executionEndTime == null ? null : executionEndTime.truncatedTo(ChronoUnit.SECONDS);
        Entity entity = null;
        if (ADTaskType.HISTORICAL_HC_ENTITY == adTaskType) {
            List<String> categoryField = detector.getCategoryFields();
            if (categoryField != null) {
                if (categoryField.size() == 1) {
                    entity = Entity.createSingleAttributeEntity(categoryField.get(0), randomAlphaOfLength(5));
                } else if (categoryField.size() == 2) {
                    entity = Entity
                        .createEntityByReordering(
                            ImmutableMap.of(categoryField.get(0), randomAlphaOfLength(5), categoryField.get(1), randomAlphaOfLength(5))
                        );
                }
            }
        }
        ADTask task = ADTask
            .builder()
            .taskId(taskId)
            .taskType(adTaskType.name())
            .configId(detectorId)
            .detector(detector)
            .state(state.name())
            .taskProgress(0.5f)
            .initProgress(1.0f)
            .currentPiece(Instant.now().truncatedTo(ChronoUnit.SECONDS).minus(randomIntBetween(1, 100), ChronoUnit.MINUTES))
            .executionStartTime(Instant.now().truncatedTo(ChronoUnit.SECONDS).minus(100, ChronoUnit.MINUTES))
            .executionEndTime(executionEndTime)
            .isLatest(true)
            .error(randomAlphaOfLength(5))
            .checkpointId(randomAlphaOfLength(5))
            .lastUpdateTime(Instant.now().truncatedTo(ChronoUnit.SECONDS))
            .startedBy(randomAlphaOfLength(5))
            .stoppedBy(stoppedBy)
            .entity(entity)
            .build();
        return task;
    }

    public static ADTask randomAdTask(String taskId, TaskState state, Instant executionEndTime, String stoppedBy, boolean withDetector)
        throws IOException {
        return randomAdTask(taskId, state, executionEndTime, stoppedBy, withDetector, ADTaskType.HISTORICAL_SINGLE_ENTITY);
    }

    public static ADTask randomAdTask(
        String taskId,
        TaskState state,
        Instant executionEndTime,
        String stoppedBy,
        boolean withDetector,
        ADTaskType adTaskType
    ) throws IOException {
        AnomalyDetector detector = withDetector
            ? randomAnomalyDetector(ImmutableMap.of(), Instant.now().truncatedTo(ChronoUnit.SECONDS), true)
            : null;
        Entity entity = null;
        if (withDetector && adTaskType.name().startsWith("HISTORICAL_HC")) {
            String categoryField = randomAlphaOfLength(5);
            detector = TestHelpers
                .randomDetector(
                    detector.getFeatureAttributes(),
                    detector.getIndices().get(0),
                    randomIntBetween(1, 10),
                    MockSimpleLog.TIME_FIELD,
                    ImmutableList.of(categoryField)
                );
            if (adTaskType.name().equals(ADTaskType.HISTORICAL_HC_ENTITY.name())) {
                entity = Entity.createSingleAttributeEntity(categoryField, randomAlphaOfLength(5));
            }

        }

        executionEndTime = executionEndTime == null ? null : executionEndTime.truncatedTo(ChronoUnit.SECONDS);
        ADTask task = ADTask
            .builder()
            .taskId(taskId)
            .taskType(adTaskType.name())
            .configId(randomAlphaOfLength(5))
            .detector(detector)
            .entity(entity)
            .state(state.name())
            .taskProgress(0.5f)
            .initProgress(1.0f)
            .currentPiece(Instant.now().truncatedTo(ChronoUnit.SECONDS).minus(randomIntBetween(1, 100), ChronoUnit.MINUTES))
            .executionStartTime(Instant.now().truncatedTo(ChronoUnit.SECONDS).minus(100, ChronoUnit.MINUTES))
            .executionEndTime(executionEndTime)
            .isLatest(true)
            .error(randomAlphaOfLength(5))
            .checkpointId(randomAlphaOfLength(5))
            .lastUpdateTime(Instant.now().truncatedTo(ChronoUnit.SECONDS))
            .startedBy(randomAlphaOfLength(5))
            .stoppedBy(stoppedBy)
            .lastUpdateTime(Instant.now().truncatedTo(ChronoUnit.SECONDS))
            .build();
        return task;
    }

    public static ADTask randomAdTask(
        String taskId,
        TaskState state,
        Instant executionEndTime,
        String stoppedBy,
        AnomalyDetector detector
    ) {
        executionEndTime = executionEndTime == null ? null : executionEndTime.truncatedTo(ChronoUnit.SECONDS);
        Entity entity = randomEntity(detector);
        String taskType = entity == null ? ADTaskType.HISTORICAL_SINGLE_ENTITY.name() : ADTaskType.HISTORICAL_HC_ENTITY.name();
        ADTask task = ADTask
            .builder()
            .taskId(taskId)
            .taskType(taskType)
            .configId(randomAlphaOfLength(5))
            .detector(detector)
            .state(state.name())
            .taskProgress(0.5f)
            .initProgress(1.0f)
            .currentPiece(Instant.now().truncatedTo(ChronoUnit.SECONDS).minus(randomIntBetween(1, 100), ChronoUnit.MINUTES))
            .executionStartTime(Instant.now().truncatedTo(ChronoUnit.SECONDS).minus(100, ChronoUnit.MINUTES))
            .executionEndTime(executionEndTime)
            .isLatest(true)
            .error(randomAlphaOfLength(5))
            .checkpointId(randomAlphaOfLength(5))
            .lastUpdateTime(Instant.now().truncatedTo(ChronoUnit.SECONDS))
            .startedBy(randomAlphaOfLength(5))
            .stoppedBy(stoppedBy)
            .lastUpdateTime(Instant.now().truncatedTo(ChronoUnit.SECONDS))
            .entity(entity)
            .build();
        return task;
    }

    /**
     * Generates a random Entity based on the provided configuration.
     *
     * If the configuration has multiple categories, a new Entity is created with attributes
     * populated with random alphanumeric strings of length 5.
     *
     * If the configuration is marked as high cardinality and does not have multiple categories,
     * a new Entity is created with a single attribute using the first category field and a random
     * alphanumeric string of length 5.
     *
     * @param config The configuration object containing information about a time series analysis.
     * @return A randomly generated Entity based on the configuration, or null if the config is null.
     */
    public static Entity randomEntity(Config config) {
        Entity entity = null;
        if (config != null) {
            if (config.hasMultipleCategories()) {
                Map<String, Object> attrMap = new HashMap<>();
                config.getCategoryFields().stream().forEach(f -> attrMap.put(f, randomAlphaOfLength(5)));
                entity = Entity.createEntityByReordering(attrMap);
            } else if (config.isHighCardinality()) {
                entity = Entity.createEntityByReordering(ImmutableMap.of(config.getCategoryFields().get(0), randomAlphaOfLength(5)));
            }
        }
        return entity;
    }

    private static Entity randomEntity() {
        String name = randomAlphaOfLength(10);
        List<String> values = new ArrayList<>();
        int size = random.nextInt(3) + 1; // At least one value
        for (int i = 0; i < size; i++) {
            values.add(randomAlphaOfLength(10));
        }
        return Entity.createEntityByReordering(ImmutableMap.of(name, values));
    }

    public static HttpEntity toHttpEntity(ToXContentObject object) throws IOException {
        return new StringEntity(toJsonString(object), APPLICATION_JSON);
    }

    public static HttpEntity toHttpEntity(String jsonString) throws IOException {
        return new StringEntity(jsonString, APPLICATION_JSON);
    }

    public static String toJsonString(ToXContentObject object) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        return TestHelpers.xContentBuilderToString(object.toXContent(builder, ToXContent.EMPTY_PARAMS));
    }

    public static RestStatus restStatus(Response response) {
        return RestStatus.fromCode(response.getStatusLine().getStatusCode());
    }

    public static SearchHits createSearchHits(int totalHits) {
        List<SearchHit> hitList = new ArrayList<>();
        IntStream.range(0, totalHits).forEach(i -> hitList.add(new SearchHit(i)));
        SearchHit[] hitArray = new SearchHit[hitList.size()];
        return new SearchHits(hitList.toArray(hitArray), new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO), 1.0F);
    }

    public static DiscoveryNode randomDiscoveryNode() {
        return new DiscoveryNode(UUIDs.randomBase64UUID(), buildNewFakeTransportAddress(), Version.CURRENT);
    }

    public static SearchRequest matchAllRequest() {
        BoolQueryBuilder query = new BoolQueryBuilder().filter(new MatchAllQueryBuilder());
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(query);
        return new SearchRequest().source(searchSourceBuilder);
    }

    public static Map<String, Object> parseStatsResult(String statsResult) throws IOException {
        XContentParser parser = TestHelpers.parser(statsResult);
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        Map<String, Object> adStats = new HashMap<>();
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();
            if (fieldName.equals("nodes")) {
                Map<String, Object> nodesAdStats = new HashMap<>();
                adStats.put("nodes", nodesAdStats);
                while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                    String nodeId = parser.currentName();
                    Map<String, Object> nodeAdStats = new HashMap<>();
                    nodesAdStats.put(nodeId, nodeAdStats);
                    parser.nextToken();
                    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                        String nodeStatName = parser.currentName();
                        XContentParser.Token token = parser.nextToken();
                        if (nodeStatName.equals("models")) {
                            parser.skipChildren();
                        } else if (nodeStatName.contains("_count")) {
                            nodeAdStats.put(nodeStatName, parser.longValue());
                        } else {
                            nodeAdStats.put(nodeStatName, parser.text());
                        }
                    }
                }
            } else if (fieldName.contains("_count")) {
                adStats.put(fieldName, parser.longValue());
            } else {
                adStats.put(fieldName, parser.text());
            }
        }
        return adStats;
    }

    public static ConfigValidationIssue randomDetectorValidationIssue() {
        ConfigValidationIssue issue = new ConfigValidationIssue(
            ValidationAspect.DETECTOR,
            ValidationIssueType.NAME,
            randomAlphaOfLength(5)
        );
        return issue;
    }

    public static ConfigValidationIssue randomDetectorValidationIssueWithSubIssues(Map<String, String> subIssues) {
        ConfigValidationIssue issue = new ConfigValidationIssue(
            ValidationAspect.DETECTOR,
            ValidationIssueType.NAME,
            randomAlphaOfLength(5),
            subIssues,
            null
        );
        return issue;
    }

    public static ConfigValidationIssue randomDetectorValidationIssueWithDetectorIntervalRec(long intervalRec) {
        ConfigValidationIssue issue = new ConfigValidationIssue(
            ValidationAspect.MODEL,
            ValidationIssueType.DETECTION_INTERVAL,
            CommonMessages.INTERVAL_REC + intervalRec,
            null,
            new IntervalTimeConfiguration(intervalRec, ChronoUnit.MINUTES)
        );
        return issue;
    }

    public static ClusterState createClusterState() {
        final Map<String, IndexMetadata> mappings = new HashMap<>();

        mappings
            .put(
                CommonName.JOB_INDEX,
                IndexMetadata
                    .builder("test")
                    .settings(
                        Settings
                            .builder()
                            .put("index.number_of_shards", 1)
                            .put("index.number_of_replicas", 1)
                            .put("index.version.created", Version.CURRENT.id)
                    )
                    .build()
            );

        // The usage of Collections.unmodifiableMap is due to replacing ImmutableOpenMap
        // with java.util.Map in the core (refer to https://tinyurl.com/5fjdccs3 and https://tinyurl.com/5fjdccs3)
        // The meaning and logic of the code stay the same.
        Metadata metaData = Metadata.builder().indices(Collections.unmodifiableMap(mappings)).build();
        ClusterState clusterState = new ClusterState(
            new ClusterName("test_name"),
            1l,
            "uuid",
            metaData,
            null,
            null,
            null,
            new HashMap<>(),
            1,
            true
        );
        return clusterState;
    }

    public static Map<String, Double> randomFixedValue(List<Feature> features) {
        Map<String, Double> map = new HashMap<>();
        if (features == null) {
            return map;
        }

        Random random = new Random();

        for (int i = 0; i < features.size(); i++) {
            if (features.get(i).getEnabled()) {
                double randomValue = random.nextDouble(); // generate a random double value
                map.put(features.get(i).getName(), randomValue);
            }
        }

        return map;
    }

    public static ImputationOption randomImputationOption(List<Feature> features) {
        Map<String, Double> randomFixedValue = randomFixedValue(features);

        List<ImputationOption> options = new ArrayList<>();
        if (randomFixedValue.size() != 0) {
            options.add(new ImputationOption(ImputationMethod.FIXED_VALUES, randomFixedValue));
        }

        options.add(new ImputationOption(ImputationMethod.ZERO));
        options.add(new ImputationOption(ImputationMethod.PREVIOUS));

        // Select a random option
        int randomIndex = Randomness.get().nextInt(options.size());
        return options.get(randomIndex);
    }

    public static class ForecasterBuilder {
        String forecasterId;
        Long version;
        String name;
        String description;
        String timeField;
        List<String> indices;
        List<Feature> features;
        QueryBuilder filterQuery;
        TimeConfiguration forecastInterval;
        TimeConfiguration windowDelay;
        Integer shingleSize;
        Map<String, Object> uiMetadata;
        Integer schemaVersion;
        Instant lastUpdateTime;
        List<String> categoryFields;
        User user;
        String resultIndex;
        Integer horizon;
        ImputationOption imputationOption;
        Integer customResultIndexMinSize;
        Integer customResultIndexMinAge;
        Integer customResultIndexTTL;
        Boolean flattenResultIndexMapping;

        ForecasterBuilder() throws IOException {
            forecasterId = randomAlphaOfLength(10);
            version = randomLong();
            name = randomAlphaOfLength(10);
            description = randomAlphaOfLength(20);
            timeField = randomAlphaOfLength(5);
            indices = ImmutableList.of(randomAlphaOfLength(10));
            features = ImmutableList.of(randomFeature(true));
            filterQuery = randomQuery();
            forecastInterval = randomIntervalTimeConfiguration();
            windowDelay = randomIntervalTimeConfiguration();
            shingleSize = randomIntBetween(4, 20);
            uiMetadata = ImmutableMap.of(randomAlphaOfLength(5), randomAlphaOfLength(10));
            schemaVersion = randomInt();
            lastUpdateTime = Instant.now().truncatedTo(ChronoUnit.SECONDS);
            categoryFields = ImmutableList.of(randomAlphaOfLength(5));
            user = randomUser();
            resultIndex = null;
            horizon = randomIntBetween(1, 20);
            imputationOption = randomImputationOption(features);
            customResultIndexMinSize = null;
            customResultIndexMinAge = null;
            customResultIndexTTL = null;
            flattenResultIndexMapping = null;
        }

        public static ForecasterBuilder newInstance() throws IOException {
            return new ForecasterBuilder();
        }

        public ForecasterBuilder setConfigId(String configId) {
            this.forecasterId = configId;
            return this;
        }

        public ForecasterBuilder setVersion(Long version) {
            this.version = version;
            return this;
        }

        public ForecasterBuilder setName(String name) {
            this.name = name;
            return this;
        }

        public ForecasterBuilder setDescription(String description) {
            this.description = description;
            return this;
        }

        public ForecasterBuilder setTimeField(String timeField) {
            this.timeField = timeField;
            return this;
        }

        public ForecasterBuilder setIndices(List<String> indices) {
            this.indices = indices;
            return this;
        }

        public ForecasterBuilder setFeatureAttributes(List<Feature> featureAttributes) {
            this.features = featureAttributes;
            return this;
        }

        public ForecasterBuilder setFilterQuery(QueryBuilder filterQuery) {
            this.filterQuery = filterQuery;
            return this;
        }

        public ForecasterBuilder setDetectionInterval(TimeConfiguration forecastInterval) {
            this.forecastInterval = forecastInterval;
            return this;
        }

        public ForecasterBuilder setWindowDelay(TimeConfiguration windowDelay) {
            this.windowDelay = windowDelay;
            return this;
        }

        public ForecasterBuilder setShingleSize(Integer shingleSize) {
            this.shingleSize = shingleSize;
            return this;
        }

        public ForecasterBuilder setUiMetadata(Map<String, Object> uiMetadata) {
            this.uiMetadata = uiMetadata;
            return this;
        }

        public ForecasterBuilder setSchemaVersion(Integer schemaVersion) {
            this.schemaVersion = schemaVersion;
            return this;
        }

        public ForecasterBuilder setLastUpdateTime(Instant lastUpdateTime) {
            this.lastUpdateTime = lastUpdateTime;
            return this;
        }

        public ForecasterBuilder setCategoryFields(List<String> categoryFields) {
            this.categoryFields = categoryFields;
            return this;
        }

        public ForecasterBuilder setUser(User user) {
            this.user = user;
            return this;
        }

        public ForecasterBuilder setCustomResultIndex(String resultIndex) {
            this.resultIndex = resultIndex;
            return this;
        }

        public ForecasterBuilder setCustomResultIndexMinSize(Integer customResultIndexMinSize) {
            this.customResultIndexMinSize = customResultIndexMinSize;
            return this;
        }

        public ForecasterBuilder setCustomResultIndexMinAge(Integer customResultIndexMinAge) {
            this.customResultIndexMinAge = customResultIndexMinAge;
            return this;
        }

        public ForecasterBuilder setCustomResultIndexTTL(Integer customResultIndexTTL) {
            this.customResultIndexTTL = customResultIndexTTL;
            return this;
        }

        public ForecasterBuilder setFlattenResultIndexMapping(Boolean flattenResultIndexMapping) {
            this.flattenResultIndexMapping = flattenResultIndexMapping;
            return this;
        }

        public ForecasterBuilder setNullImputationOption() {
            this.imputationOption = null;
            return this;
        }

        public ForecasterBuilder setHorizon(Integer horizon) {
            this.horizon = horizon;
            return this;
        }

        public Forecaster build() {
            return new Forecaster(
                forecasterId,
                version,
                name,
                description,
                timeField,
                indices,
                features,
                filterQuery,
                forecastInterval,
                windowDelay,
                shingleSize,
                uiMetadata,
                schemaVersion,
                lastUpdateTime,
                categoryFields,
                user,
                resultIndex,
                horizon,
                imputationOption,
                // Recency emphasis must be an integer greater than 1
                randomIntBetween(2, 1000),
                randomIntBetween(1, 128),
                randomIntBetween(1, 1000),
                customResultIndexMinSize,
                customResultIndexMinAge,
                customResultIndexTTL,
                flattenResultIndexMapping,
                lastUpdateTime
            );
        }
    }

    public static Forecaster randomForecaster() throws IOException {
        Feature feature = randomFeature();
        List<Feature> featureList = ImmutableList.of(feature);
        return new Forecaster(
            randomAlphaOfLength(10),
            randomLong(),
            randomAlphaOfLength(10),
            randomAlphaOfLength(20),
            randomAlphaOfLength(5),
            ImmutableList.of(randomAlphaOfLength(10)),
            featureList,
            randomQuery(),
            randomIntervalTimeConfiguration(),
            randomIntervalTimeConfiguration(),
            randomIntBetween(4, 20),
            ImmutableMap.of(randomAlphaOfLength(5), randomAlphaOfLength(10)),
            randomInt(),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            ImmutableList.of(randomAlphaOfLength(5)),
            randomUser(),
            null,
            randomIntBetween(1, 20),
            randomImputationOption(featureList),
            // Recency emphasis must be an integer greater than 1
            randomIntBetween(2, 1000),
            randomIntBetween(1, 128),
            randomIntBetween(1, 1000),
            null,
            null,
            null,
            null,
            Instant.now().truncatedTo(ChronoUnit.SECONDS)
        );
    }

    public static class ForecastTaskBuilder {
        private String configId = "config123";
        private String taskId = "task123";
        private String taskType = "FORECAST_HISTORICAL_HC_ENTITY";
        private String state = "Running";
        private Float taskProgress = 0.5f;
        private Float initProgress = 0.1f;
        private Instant currentPiece = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        private Instant executionStartTime = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        private Instant executionEndTime = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        private Boolean isLatest = true;
        private String error = "No errors";
        private String checkpointId = "checkpoint1";
        private Instant lastUpdateTime = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        private String startedBy = "user1";
        private String stoppedBy = "user2";
        private String coordinatingNode = "node1";
        private String workerNode = "node2";
        private Forecaster forecaster = TestHelpers.randomForecaster();
        private Entity entity = TestHelpers.randomEntity(forecaster);
        private String parentTaskId = "parentTask1";
        private Integer estimatedMinutesLeft = 10;
        protected User user = TestHelpers.randomUser();

        private DateRange dateRange = new DateRange(Instant.ofEpochMilli(123), Instant.ofEpochMilli(456));

        public ForecastTaskBuilder() throws IOException {}

        public static ForecastTaskBuilder newInstance() throws IOException {
            return new ForecastTaskBuilder();
        }

        public ForecastTaskBuilder setForecaster(Forecaster associatedForecaster) {
            this.forecaster = associatedForecaster;
            return this;
        }

        public ForecastTaskBuilder setUser(User associatedUser) {
            this.user = associatedUser;
            return this;
        }

        public ForecastTaskBuilder setDateRange(DateRange associatedRange) {
            this.dateRange = associatedRange;
            return this;
        }

        public ForecastTaskBuilder setEntity(Entity associatedEntity) {
            this.entity = associatedEntity;
            return this;
        }

        public ForecastTask build() {
            return new ForecastTask.Builder()
                .configId(configId)
                .taskId(taskId)
                .lastUpdateTime(lastUpdateTime)
                .startedBy(startedBy)
                .stoppedBy(stoppedBy)
                .error(error)
                .state(state)
                .taskProgress(taskProgress)
                .initProgress(initProgress)
                .currentPiece(currentPiece)
                .executionStartTime(executionStartTime)
                .executionEndTime(executionEndTime)
                .isLatest(isLatest)
                .taskType(taskType)
                .checkpointId(checkpointId)
                .coordinatingNode(coordinatingNode)
                .workerNode(workerNode)
                .entity(entity)
                .parentTaskId(parentTaskId)
                .estimatedMinutesLeft(estimatedMinutesLeft)
                .user(user)
                .forecaster(forecaster)
                .dateRange(dateRange)
                .build();
        }
    }

    public static class ForecastResultBuilder {
        private String forecasterId = randomAlphaOfLength(5);
        private String taskId = randomAlphaOfLength(5);
        private Double dataQuality = randomDouble();
        private List<FeatureData> featureData = ImmutableList.of(randomFeatureData(), randomFeatureData());
        private Instant dataStartTime = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        private Instant dataEndTime = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        private Instant executionStartTime = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        private Instant executionEndTime = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        private String error = "";
        private Optional<Entity> entity = Optional.empty();
        private User user = randomUser();
        private Integer schemaVersion = randomIntBetween(1, 10);
        private String featureId = randomAlphaOfLength(5);
        private Float forecastValue = randomFloat();
        private Float lowerBound = randomFloat();
        private Float upperBound = randomFloat();
        private Instant forecastDataStartTime = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        private Instant forecastDataEndTime = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        private Integer horizonIndex = randomIntBetween(1, 10);

        public ForecastResultBuilder() {

        }

        public static ForecastResultBuilder newInstance() {
            return new ForecastResultBuilder();
        }

        public ForecastResult build() {
            return new ForecastResult(
                forecasterId,
                taskId,
                dataQuality,
                featureData,
                dataStartTime,
                dataEndTime,
                executionStartTime,
                executionEndTime,
                error,
                entity,
                user,
                schemaVersion,
                featureId,
                forecastValue,
                lowerBound,
                upperBound,
                forecastDataStartTime,
                forecastDataEndTime,
                horizonIndex
            );
        }
    }

    public static class JobBuilder {
        private String name = randomAlphaOfLength(10);
        private Schedule schedule = randomIntervalSchedule();
        private TimeConfiguration windowDelay = randomIntervalTimeConfiguration();
        private Boolean isEnabled = true;
        private Instant enabledTime = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        private Instant disabledTime = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        private Instant lastUpdateTime = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        private Long lockDurationSeconds = 60L;
        private User user = randomUser();
        private String resultIndex = null;
        private AnalysisType analysisType = AnalysisType.AD;

        public JobBuilder() {

        }

        public static JobBuilder newInstance() {
            return new JobBuilder();
        }

        public JobBuilder name(String name) {
            this.name = name;
            return this;
        }

        public JobBuilder schedule(Schedule schedule) {
            this.schedule = schedule;
            return this;
        }

        public JobBuilder windowDelay(TimeConfiguration windowDelay) {
            this.windowDelay = windowDelay;
            return this;
        }

        public JobBuilder isEnabled(Boolean isEnabled) {
            this.isEnabled = isEnabled;
            return this;
        }

        public JobBuilder enabledTime(Instant enabledTime) {
            this.enabledTime = enabledTime;
            return this;
        }

        public JobBuilder disabledTime(Instant disabledTime) {
            this.disabledTime = disabledTime;
            return this;
        }

        public JobBuilder lastUpdateTime(Instant lastUpdateTime) {
            this.lastUpdateTime = lastUpdateTime;
            return this;
        }

        public JobBuilder lockDurationSeconds(Long lockDurationSeconds) {
            this.lockDurationSeconds = lockDurationSeconds;
            return this;
        }

        public JobBuilder user(User user) {
            this.user = user;
            return this;
        }

        public JobBuilder resultIndex(String resultIndex) {
            this.resultIndex = resultIndex;
            return this;
        }

        public JobBuilder analysisType(AnalysisType analysisType) {
            this.analysisType = analysisType;
            return this;
        }

        public Job build() {
            return new Job(
                name,
                schedule,
                windowDelay,
                isEnabled,
                enabledTime,
                disabledTime,
                lastUpdateTime,
                lockDurationSeconds,
                user,
                resultIndex,
                analysisType
            );
        }
    }

    public static ForecastResultWriteRequest randomForecastResultWriteRequest() {
        // Generate random values for required fields
        long expirationEpochMs = Instant.now().plusSeconds(random.nextInt(3600)).toEpochMilli(); // Expire within the next hour
        String forecasterId = randomAlphaOfLength(10);
        RequestPriority priority = RequestPriority.MEDIUM; // Use NORMAL priority for testing
        ForecastResult result = randomForecastResult(forecasterId);
        String resultIndex = random.nextBoolean() ? randomAlphaOfLength(10) : null; // Randomly decide to set resultIndex or not

        return new ForecastResultWriteRequest(expirationEpochMs, forecasterId, priority, result, resultIndex);
    }

    public static ForecastResult randomForecastResult(String forecasterId) {
        String taskId = randomAlphaOfLength(10);
        Double dataQuality = random.nextDouble();
        List<FeatureData> featureData = ImmutableList.of(randomFeatureData());
        Instant dataStartTime = Instant.now().minusSeconds(random.nextInt(3600));
        Instant dataEndTime = Instant.now();
        Instant executionStartTime = Instant.now().minusSeconds(random.nextInt(3600));
        Instant executionEndTime = Instant.now();
        String error = random.nextBoolean() ? randomAlphaOfLength(20) : null;
        Optional<Entity> entity = random.nextBoolean() ? Optional.of(randomEntity()) : Optional.empty();
        User user = random.nextBoolean() ? randomUser() : null;
        Integer schemaVersion = random.nextInt(10);
        String featureId = randomAlphaOfLength(10);
        Float forecastValue = random.nextFloat();
        Float lowerBound = forecastValue - random.nextFloat();
        Float upperBound = forecastValue + random.nextFloat();
        Instant forecastDataStartTime = dataEndTime.plusSeconds(random.nextInt(3600));
        Instant forecastDataEndTime = forecastDataStartTime.plusSeconds(random.nextInt(3600));
        Integer horizonIndex = random.nextInt(100);

        return new ForecastResult(
            forecasterId,
            taskId,
            dataQuality,
            featureData,
            dataStartTime,
            dataEndTime,
            executionStartTime,
            executionEndTime,
            error,
            entity,
            user,
            schemaVersion,
            featureId,
            forecastValue,
            lowerBound,
            upperBound,
            forecastDataStartTime,
            forecastDataEndTime,
            horizonIndex
        );
    }
}
