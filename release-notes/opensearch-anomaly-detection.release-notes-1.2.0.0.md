## Version 1.2.0.0 Release Notes

Compatible with OpenSearch 1.2.0

### Features

* Add multi-category top anomaly results API ([#261](https://github.com/opensearch-project/anomaly-detection/pull/261))
* Validation API - "Blocker" level validation ([#231](https://github.com/opensearch-project/anomaly-detection/pull/231))
* support storing anomaly result to custom index ([#276](https://github.com/opensearch-project/anomaly-detection/pull/276))
* support only searching results in custom result index ([#292](https://github.com/opensearch-project/anomaly-detection/pull/292))

### Enhancements

* Improve HCAD cold start ([#272](https://github.com/opensearch-project/anomaly-detection/pull/272))
* Support custom result indices in multi-category filtering API ([#281](https://github.com/opensearch-project/anomaly-detection/pull/281))
* Add extra fields to anomaly result index ([#268](https://github.com/opensearch-project/anomaly-detection/pull/268))
* Skipping checking create index permission for Validate API ([#285](https://github.com/opensearch-project/anomaly-detection/pull/285))

### Bug Fixes

* fix flaky REST IT test ([#259](https://github.com/opensearch-project/anomaly-detection/pull/259))
* Fixed a bug when door keepers unnecessarily reset their states ([#262](https://github.com/opensearch-project/anomaly-detection/pull/262))
* Fix task cache expiration bug ([#269](https://github.com/opensearch-project/anomaly-detection/pull/269))
* fixed unit test by changing name of method to most up to date ([#287](https://github.com/opensearch-project/anomaly-detection/pull/287))
* Fix Instant parsing bug in multi category filtering API ([#289](https://github.com/opensearch-project/anomaly-detection/pull/289))
* wait for some time to get semaphore when set HC detector task as done ([#300](https://github.com/opensearch-project/anomaly-detection/pull/300))
* added switch case for general settings issues ([#305](https://github.com/opensearch-project/anomaly-detection/pull/305))

### Infrastructure

* Adding support for integration tests with remote cluster. ([#298](https://github.com/opensearch-project/anomaly-detection/pull/298))

### Documentation

* correct copyright notice; remove old copyright from ODFE ([#257](https://github.com/opensearch-project/anomaly-detection/pull/257))
* Add DCO Check Workflow ([#273](https://github.com/opensearch-project/anomaly-detection/pull/273))

### Maintenance

* Bump anomaly-detection version to 1.2 ([#286](https://github.com/opensearch-project/anomaly-detection/pull/286))
