## Version 2.17.0.0 Release Notes

Compatible with OpenSearch 2.17.0

### Feature
* Add Support for Handling Missing Data in Anomaly Detection ([#1274](https://github.com/opensearch-project/anomaly-detection/pull/1274))
* Adding remote index and multi-index checks in validation ([#1290](https://github.com/opensearch-project/anomaly-detection/pull/1290))

### Enhancements
* Fix inference logic and standardize config index mapping ([#1284](https://github.com/opensearch-project/anomaly-detection/pull/1284))

### Big Fixes
* Prevent resetting the latest flag of real-time when starting historical analysis ([#1287](https://github.com/opensearch-project/anomaly-detection/pull/1287))
* Correct handling of null max aggregation values in SearchResponse ([#1292](https://github.com/opensearch-project/anomaly-detection/pull/1292))
