package org.opensearch.ad.model;

import org.opensearch.ad.auth.UserIdentity;
import org.opensearch.index.query.QueryBuilder;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.opensearch.ad.settings.AnomalyDetectorSettings.DEFAULT_SHINGLE_SIZE;

/**
 * JSON Object class required by opensearch-java client
 */
public class IndexAnomalyDetector {

    private String detectorId;
    private Long version;
    private String name;
    private String description;
    private String timeField;
    private List<String> indices;
    private List<Feature> featureAttributes;
    private QueryBuilder filterQuery;
    private TimeConfiguration detectionInterval;
    private TimeConfiguration windowDelay;
    private Integer shingleSize;
    private Map<String, Object> uiMetadata;
    private Integer schemaVersion;
    private Instant lastUpdateTime;
    private List<String> categoryFields;
    private UserIdentity user;
    private String detectorType;
    private String resultIndex;

    private DetectionDateRange detectionDateRange;

    public String getDetectorId() {
        return detectorId;
    }

    public void setDetectorId(String detectorId) {
        this.detectorId = detectorId;
    }

    public Long getVersion() {
        return version;
    }

    public void setVersion(Long version) {
        this.version = version;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getTimeField() {
        return timeField;
    }

    public void setTimeField(String timeField) {
        this.timeField = timeField;
    }

    public List<String> getIndices() {
        return indices;
    }

    public void setIndices(List<String> indices) {
        this.indices = indices;
    }

    public List<Feature> getFeatureAttributes() {
        return featureAttributes;
    }

    public void setFeatureAttributes(List<Feature> featureAttributes) {
        this.featureAttributes = featureAttributes;
    }

    public QueryBuilder getFilterQuery() {
        return filterQuery;
    }

    public void setFilterQuery(QueryBuilder filterQuery) {
        this.filterQuery = filterQuery;
    }

//    public List<String> getEnabledFeatureIds() {
//        return featureAttributes.stream().filter(Feature::getEnabled).map(Feature::getId).collect(Collectors.toList());
//    }

//    public List<String> getEnabledFeatureNames() {
//        return featureAttributes.stream().filter(Feature::getEnabled).map(Feature::getName).collect(Collectors.toList());
//    }

    public TimeConfiguration getDetectionInterval() {
        return detectionInterval;
    }

    public void setDetectionInterval(TimeConfiguration detectionInterval) {
        this.detectionInterval = detectionInterval;
    }

    public TimeConfiguration getWindowDelay() {
        return windowDelay;
    }

    public void setWindowDelay(TimeConfiguration windowDelay) {
        this.windowDelay = windowDelay;
    }

    public Integer getShingleSize() {
        return shingleSize;
    }

    public void setShingleSize(Integer shingleSize) {
        this.shingleSize = shingleSize;
    }

    public Integer getShingleSize(Integer customShingleSize) {
        return customShingleSize == null ? DEFAULT_SHINGLE_SIZE : customShingleSize;
    }


    public Map<String, Object> getUiMetadata() {
        return uiMetadata;
    }

    public void setUiMetadata(Map<String, Object> uiMetadata) {
        this.uiMetadata = uiMetadata;
    }

    public Integer getSchemaVersion() {
        return schemaVersion;
    }

    public void setSchemaVersion(Integer schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

    public Instant getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(Instant lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public List<String> getCategoryField() {
        return this.categoryFields;
    }

    public void setCategoryFields(List<String> categoryFields) {
        this.categoryFields = categoryFields;
    }

//    public long getDetectorIntervalInMilliseconds() {
//        return ((IntervalTimeConfiguration) getDetectionInterval()).toDuration().toMillis();
//    }

//    public long getDetectorIntervalInSeconds() {
//        return getDetectorIntervalInMilliseconds() / 1000;
//    }
//
//    public long getDetectorIntervalInMinutes() {
//        return getDetectorIntervalInMilliseconds() / 1000 / 60;
//    }

//    public Duration getDetectionIntervalDuration() {
//        return ((IntervalTimeConfiguration) getDetectionInterval()).toDuration();
//    }

    public UserIdentity getUser() {
        return user;
    }

    public void setUser(UserIdentity user) {
        this.user = user;
    }

    public String getDetectorType() {
        return detectorType;
    }

    public void setDetectorType(String detectorType) {
        this.detectorType = detectorType;
    }

    public void setDetectionDateRange(DetectionDateRange detectionDateRange) {
        this.detectionDateRange = detectionDateRange;
    }

    public DetectionDateRange getDetectionDateRange() {
        return detectionDateRange;
    }

    public String getResultIndex() {
        return resultIndex;
    }

    public void setResultIndex(String resultIndex) {
        this.resultIndex = resultIndex;
    }
}
