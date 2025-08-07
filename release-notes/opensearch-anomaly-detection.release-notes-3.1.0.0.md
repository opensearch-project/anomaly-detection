## Version 3.1.0.0 Release Notes

Compatible with OpenSearch 3.1.0

### Enhancements
- Use Centralized Resource Access Control framework provided by security plugin ([#1400](https://github.com/opensearch-project/anomaly-detection/pull/1400))
- Introduce state machine, separate config index, improve suggest/validate APIs, and persist cold-start results for run-once visualization ([#1479](https://github.com/opensearch-project/anomaly-detection/pull/1479))

### Bug Fixes
- Fix incorrect task state handling in ForecastRunOnceTransportAction ([#1489](https://github.com/opensearch-project/anomaly-detection/pull/1489))
- Fix incorrect task state handling in ForecastRunOnceTransportAction ([#1493](https://github.com/opensearch-project/anomaly-detection/pull/1493))
- Refine cold-start, window delay, and task updates ([#1496](https://github.com/opensearch-project/anomaly-detection/pull/1496))
- Fix stopping issue when forecaster is in FORECAST_FAILURE state ([#1502](https://github.com/opensearch-project/anomaly-detection/pull/1502))

