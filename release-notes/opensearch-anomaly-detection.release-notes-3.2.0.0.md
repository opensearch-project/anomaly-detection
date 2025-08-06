## Version 3.2.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.2.0

### Enhancements
* Support >1 hr intervals ([#1513](https://github.com/opensearch-project/anomaly-detection/pull/1513))

### Bug Fixes
* Fixing concurrency bug on writer ([#1508](https://github.com/opensearch-project/anomaly-detection/pull/1508))
* Fix(forecast): advance past current interval & anchor on now ([#1528](https://github.com/opensearch-project/anomaly-detection/pull/1528))
* Changing search calls on interval calculation ([#1535](https://github.com/opensearch-project/anomaly-detection/pull/1535))

### Infrastructure
* Bumping gradle and nebula versions ([#1537](https://github.com/opensearch-project/anomaly-detection/pull/1537))