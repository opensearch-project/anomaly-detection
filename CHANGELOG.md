# CHANGELOG
All notable changes to this project are documented in this file.

Inspired from [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)

## [Unreleased 3.x](https://github.com/opensearch-project/anomaly-detection/compare/3.7...HEAD)

### Features
- Add runtime PPL-backed anomaly detector source support ([#1718](https://github.com/opensearch-project/anomaly-detection/pull/1718))
### Enhancements
### Bug Fixes
- Return detector statistics when the detector-type field is unmapped instead of failing with an aggregation cast error ([#1775](https://github.com/opensearch-project/anomaly-detection/pull/1775))
- Align PPL transport requests with the SQL plugin's analyze and partial-result fields ([#1775](https://github.com/opensearch-project/anomaly-detection/pull/1775))
- Prevent historical high-cardinality analyses from remaining stuck in `RUNNING` after task-slot scale-down and support nullable absolute-threshold rule operators during remote dispatch ([#1782](https://github.com/opensearch-project/anomaly-detection/pull/1782))
### Infrastructure
- Cover historical PPL analysis and verify serialized feature results in integration tests ([#1775](https://github.com/opensearch-project/anomaly-detection/pull/1775))
- Close regular and admin HTTPS test connections before shutting down REST clients to prevent selector assertions and thread leaks; provide valid timestamp mappings for featureless detector tests ([#1775](https://github.com/opensearch-project/anomaly-detection/pull/1775))
### Documentation
### Maintenance
- Deprecate `filter_by_backend_roles` for anomaly detection and forecasting, targeting removal in 4.0 ([#1781](https://github.com/opensearch-project/anomaly-detection/pull/1781))
- Rename the resource sharing feature flag to the non-experimental key ([#1778](https://github.com/opensearch-project/anomaly-detection/pull/1778))
- Increment version to 3.9.0-SNAPSHOT and declare the Jackson 2 core dependency required by PPL response parsing and Random Cut Forest serialization ([#1775](https://github.com/opensearch-project/anomaly-detection/pull/1775))
### Security
### Refactoring
