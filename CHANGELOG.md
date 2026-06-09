# CHANGELOG
All notable changes to this project are documented in this file.

Inspired from [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)

## [Unreleased 3.x](https://github.com/opensearch-project/anomaly-detection/compare/3.7...HEAD)

### Features
### Enhancements
- Support wildcard cluster prefixes (e.g. `*:idx`, `cluster*:idx`, `*cluster*:idx`) in detector and forecaster data source indices; time-field and categorical-field validation pass if at least one matching remote cluster has the index ([1606](https://github.com/opensearch-project/anomaly-detection/issues/1606))
### Bug Fixes
### Infrastructure
### Documentation
### Maintenance
### Security
### Refactoring
