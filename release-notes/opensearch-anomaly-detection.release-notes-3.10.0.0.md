## Version 3.10.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.10.0

### Enhancements

* Deprecate `filter_by_backend_roles` setting, targeting removal in 4.0 in favor of the security plugin's resource sharing and access control ([#1781](https://github.com/opensearch-project/anomaly-detection/pull/1781))

### Bug Fixes

* Prevent historical high-cardinality anomaly detection analyses from remaining stuck in RUNNING state after task-slot scale-down ([#1782](https://github.com/opensearch-project/anomaly-detection/pull/1782))
* Fix PPL transport wire format mismatch with SQL plugin and correct invalid test role permissions causing integration test failures ([#1798](https://github.com/opensearch-project/anomaly-detection/pull/1798))

### Infrastructure

* Fix flaky `testCreateAnomalyDetectorWithEmptyIndices` integration test by ensuring the empty source index has the detector's time field mapped as `date` ([#1787](https://github.com/opensearch-project/anomaly-detection/pull/1787))
