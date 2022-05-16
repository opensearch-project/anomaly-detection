## Version 2.0.0.0 Release Notes

Compatible with OpenSearch 2.0.0


### Enhancements

* changed usages of "master" to "clusterManager" in variable names ([#504](https://github.com/opensearch-project/anomaly-detection/pull/504))

### Bug Fixes

* Changed default description to empty string instead of null ([#438](https://github.com/opensearch-project/anomaly-detection/pull/438))
* Fixed ADTaskProfile toXContent bug and added to .gitignore ([#447](https://github.com/opensearch-project/anomaly-detection/pull/447))
* Fix restart HCAD detector bug ([#460](https://github.com/opensearch-project/anomaly-detection/pull/460))
* Check if indices exist in the presence of empty search results ([#495](https://github.com/opensearch-project/anomaly-detection/pull/495))

### Infrastructure

* Reduced jacoco exclusions and added more tests ([#446](https://github.com/opensearch-project/anomaly-detection/pull/446))
* refactor SearchADResultTransportAction to be more testable ([#517](https://github.com/opensearch-project/anomaly-detection/pull/517))
* Remove oss flavor ([#449](https://github.com/opensearch-project/anomaly-detection/pull/449))
* Add auto labeler workflow ([#455](https://github.com/opensearch-project/anomaly-detection/pull/455))
* Gradle 7 and Opensearch 2.0 upgrade ([#464](https://github.com/opensearch-project/anomaly-detection/pull/464))
* Adding test-retry plugin ([#456](https://github.com/opensearch-project/anomaly-detection/pull/456))
* Updated issue templates from .github. ([#488](https://github.com/opensearch-project/anomaly-detection/pull/488))
* removing job-scheduler zip and replacing with distribution build ([#487](https://github.com/opensearch-project/anomaly-detection/pull/487))
* JDK 17 support ([#489](https://github.com/opensearch-project/anomaly-detection/pull/489))
* Moving script file in scripts folder  for file location standardization ([#494](https://github.com/opensearch-project/anomaly-detection/pull/494))
* Removed rcf jar for 3.0-rc1 and fixed zip fetching for AD and JS ([#500](https://github.com/opensearch-project/anomaly-detection/pull/500))
* Remove BWC zips for dynamic dependency  ([#505](https://github.com/opensearch-project/anomaly-detection/pull/505))
* bump rcf to 3.0-rc2.1 ([#519](https://github.com/opensearch-project/anomaly-detection/pull/519))
* Increase more coverage and reduce jacocoExclusions ([#533](https://github.com/opensearch-project/anomaly-detection/pull/533))

### Documentation

* Add Visualization integration RFC docs ([#477](https://github.com/opensearch-project/anomaly-detection/pull/477))
