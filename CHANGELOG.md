# CHANGELOG
All notable changes to this project are documented in this file.

Inspired from [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)

## [Unreleased 3.x](https://github.com/opensearch-project/anomaly-detection/compare/3.7...HEAD)

### Features
- Add runtime PPL-backed anomaly detector source support ([#1718](https://github.com/opensearch-project/anomaly-detection/pull/1718))
### Enhancements
### Bug Fixes
- Align PPL transport requests with the SQL plugin's analyze and partial-result fields ([#1760](https://github.com/opensearch-project/anomaly-detection/pull/1760))
### Infrastructure
- Close HTTPS test connections before shutting down REST clients to prevent selector assertions and thread leaks ([#1760](https://github.com/opensearch-project/anomaly-detection/pull/1760))
### Documentation
### Maintenance
- Increment version to 3.9.0-SNAPSHOT and declare the Jackson 2 core dependency required by PPL response parsing and Random Cut Forest serialization ([#1760](https://github.com/opensearch-project/anomaly-detection/pull/1760))
### Security
### Refactoring
