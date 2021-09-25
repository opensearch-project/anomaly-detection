## Version 1.1.0.0 Release Notes

Compatible with OpenSearch 1.1.0

### Features
* multi-category support, rate limiting, and pagination ([#121](https://github.com/opensearch-project/anomaly-detection/pull/121))
* Single flow feature change ([#147](https://github.com/opensearch-project/anomaly-detection/pull/147))
* Compact rcf integration ([#149](https://github.com/opensearch-project/anomaly-detection/pull/149))

### Enhancements
* Disable model splitting in single-stream detectors ([#162](https://github.com/opensearch-project/anomaly-detection/pull/162))
* Handle more AD exceptions thrown over the wire/network ([#157](https://github.com/opensearch-project/anomaly-detection/pull/157))
* support historical analysis for multi-category HC ([#159](https://github.com/opensearch-project/anomaly-detection/pull/159))
* Limit the max models shown on the stats and profile API ([#182](https://github.com/opensearch-project/anomaly-detection/pull/182))
* Enable shingle in HCAD ([#187](https://github.com/opensearch-project/anomaly-detection/pull/187))
* add min score for labeling anomalies to thresholding ([#193](https://github.com/opensearch-project/anomaly-detection/pull/193))
* support backward compatibility of historical analysis and realtime task ([#195](https://github.com/opensearch-project/anomaly-detection/pull/195))

### Bug Fixes
* don't replace detector user when update ([#126](https://github.com/opensearch-project/anomaly-detection/pull/126))
* avoid sending back verbose error message and wrong 500 error to user; fix hard code query size of historical analysis ([#150](https://github.com/opensearch-project/anomaly-detection/pull/150))
* Bug fixes and unit tests ([#177](https://github.com/opensearch-project/anomaly-detection/pull/177))

### Infrastructure
* add deprecated detector type for bwc; add more test cases for historical analysis ([#197](https://github.com/opensearch-project/anomaly-detection/pull/197))

### Documentation
* Add themed logo to README ([#134](https://github.com/opensearch-project/anomaly-detection/pull/134))
