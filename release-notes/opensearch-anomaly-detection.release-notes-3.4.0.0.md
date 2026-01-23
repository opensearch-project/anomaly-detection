## Version 3.4.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.4.0

### Enhancements
* Adds capability to automatically switch to old access-control if model-group is excluded from protected resources setting ([#1569](https://github.com/opensearch-project/anomaly-detection/pull/1569))
* Adding suggest and validate transport actions to node client ([#1605](https://github.com/opensearch-project/anomaly-detection/pull/1605))
* Adding auto create as an optional field on detectors ([#1602](https://github.com/opensearch-project/anomaly-detection/pull/1602))

### Bug Fixes
* Fix(forecast): auto-expand replicas for default results index on 3AZ domains ([#1615](https://github.com/opensearch-project/anomaly-detection/pull/1615))

### Infrastructure
* Test: Prevent oversized bulk requests in synthetic data test ([#1603](https://github.com/opensearch-project/anomaly-detection/pull/1603))
* Update CI to JDK 25 and gradle to 9.2 ([#1623](https://github.com/opensearch-project/anomaly-detection/pull/1623))