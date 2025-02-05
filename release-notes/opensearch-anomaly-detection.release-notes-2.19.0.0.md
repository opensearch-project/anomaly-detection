## Version 2.19.0.0 Release Notes

Compatible with OpenSearch 2.19.0

### Features
* Allow triggering anomaly only on drop or rise of features ([#1358](https://github.com/opensearch-project/anomaly-detection/pull/1358))
* Add flattens custom result index when enabled ([#1401](https://github.com/opensearch-project/anomaly-detection/pull/1401)), ([#1409](https://github.com/opensearch-project/anomaly-detection/pull/1409))

### Enhancements
* Changing replica count to up 2 for custom result index ([#1362](https://github.com/opensearch-project/anomaly-detection/pull/1362))

### Bug Fixes
* Not blocking detector creation on unknown feature validation error ([#1366](https://github.com/opensearch-project/anomaly-detection/pull/1366))
* Fix exceptions in IntervalCalculation and ResultIndexingHandler ([#1379](https://github.com/opensearch-project/anomaly-detection/pull/1379))

### Infrastructure
* Bump codecov/codecov-action from 4 to 5 ([#1369](https://github.com/opensearch-project/anomaly-detection/pull/1369))
* Bump com.google.code.gson:gson from 2.8.9 to 2.11.0 ([#1375](https://github.com/opensearch-project/anomaly-detection/pull/1375))
* Bump jackson from 2.18.0 to 2.18.2 ([#1376](https://github.com/opensearch-project/anomaly-detection/pull/1376))
* Bump org.apache.commons:commons-lang3 from 3.13.0 to 3.17.0 ([#1377](https://github.com/opensearch-project/anomaly-detection/pull/1377))
* Bump org.objenesis:objenesis from 3.3 to 3.4 ([#1393](https://github.com/opensearch-project/anomaly-detection/pull/1393))
* Updating several dependencies ([#1368](https://github.com/opensearch-project/anomaly-detection/pull/1368))
* Update recency_emphasis to be greater than 1 in test cases ([#1406](https://github.com/opensearch-project/anomaly-detection/pull/1406))
