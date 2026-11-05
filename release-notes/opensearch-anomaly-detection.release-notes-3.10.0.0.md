## Version 3.10.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.10.0

### Enhancements

* Deprecate `filter_by_backend_roles` setting, targeting removal in 4.0 in favor of security plugin resource sharing ([#1781](https://github.com/opensearch-project/anomaly-detection/pull/1781))

### Bug Fixes

* Prevent historical high-cardinality anomaly detection analyses from remaining stuck in RUNNING state after task-slot scale-down ([#1782](https://github.com/opensearch-project/anomaly-detection/pull/1782))

### Infrastructure

* Fix flaky `testCreateAnomalyDetectorWithEmptyIndices` integration test by mapping the time field as `date` in the empty source index ([#1787](https://github.com/opensearch-project/anomaly-detection/pull/1787))
