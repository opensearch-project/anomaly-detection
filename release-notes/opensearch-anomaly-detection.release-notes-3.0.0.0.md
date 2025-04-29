## Version 3.0.0.0 Release Notes

Compatible with OpenSearch 3.0.0

### Enhancements
- Use testclusters when testing with security ([#1414](https://github.com/opensearch-project/anomaly-detection/pull/1414))
- Add AWS SAM template for WAF log analysis and anomaly detection ([#1460](https://github.com/opensearch-project/anomaly-detection/pull/1460))

### Bug Fixes
- Distinguish local cluster when local name is same as remote ([#1446](https://github.com/opensearch-project/anomaly-detection/pull/1446))

### Infrastructure
- Adding dual cluster arg to gradle run ([#1441](https://github.com/opensearch-project/anomaly-detection/pull/1441))
- Fix build due to phasing off SecurityManager usage in favor of Java Agent ([#1450](https://github.com/opensearch-project/anomaly-detection/pull/1450))
- Using java-agent gradle plugin to phase off Security Manager in favor of Java-agent ([#1454](https://github.com/opensearch-project/anomaly-detection/pull/1454))
- Add integtest.sh to specifically run integTestRemote task ([#1456](https://github.com/opensearch-project/anomaly-detection/pull/1456))

### Maintenance
- Fix breaking changes for 3.0.0 release ([#1424](https://github.com/opensearch-project/anomaly-detection/pull/1424))

