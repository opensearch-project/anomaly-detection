# CHANGELOG
All notable changes to this project are documented in this file.

Inspired from [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)

## [Unreleased 3.x](https://github.com/opensearch-project/anomaly-detection/compare/3.0...HEAD)
### Features
### Enhancements
- Adds capability to automatically switch to old access-control if model-group is excluded from protected resources setting ([#1569](https://github.com/opensearch-project/anomaly-detection/pull/1569))
- Adding suggest and validate transport actions to node client ([#1605](https://github.com/opensearch-project/anomaly-detection/pull/1605))
- Adding auto create as an optional field on detectors ([#1602](https://github.com/opensearch-project/anomaly-detection/pull/1602))
- Adding create and start to AD node client ([#1611](https://github.com/opensearch-project/anomaly-detection/pull/1611))

### Bug Fixes
- fix(forecast): auto-expand replicas for default results index on 3AZ domains ([#1615](https://github.com/opensearch-project/anomaly-detection/pull/1615))

### Infrastructure
- Test: Prevent oversized bulk requests in synthetic data test ([#1603](https://github.com/opensearch-project/anomaly-detection/pull/1603))
- Update CI to JDK 25 and gradle to 9.2 ([#1623](https://github.com/opensearch-project/anomaly-detection/pull/1623))

### Documentation
### Maintenance

### Refactoring

## [Unreleased 2.x](https://github.com/opensearch-project/anomaly-detection/compare/2.19...2.x)
### Features
### Enhancements
### Bug Fixes
### Infrastructure
### Documentation
### Maintenance
### Refactoring
