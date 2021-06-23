[![AD Test](https://github.com/opensearch-project/anomaly-detection/workflows/Build%20and%20Test%20Anomaly%20detection/badge.svg)](https://github.com/opensearch-project/anomaly-detection/actions?query=workflow%3A%22Build+and+Test+Anomaly+detection%22+branch%3A%22main%22)
[![codecov](https://codecov.io/gh/opensearch-project/anomaly-detection/branch/main/graph/badge.svg?flag=plugin)](https://codecov.io/gh/opensearch-project/anomaly-detection)
[![Documentation](https://img.shields.io/badge/doc-reference-blue)](https://docs-beta.opensearch.org/monitoring-plugins/ad/index/)
[![Forum](https://img.shields.io/badge/chat-on%20forums-blue)](https://discuss.opendistrocommunity.dev/c/Use-this-category-for-all-questions-around-machine-learning-plugins)
![PRs welcome!](https://img.shields.io/badge/PRs-welcome!-success)

<!-- TOC -->

- [OpenSearch Anomaly Detection](#opensearch-anomaly-detection)
- [Highlights](#highlights)
- [Documentation](#documentation)
- [Contributing](#contributing)
- [Code of Conduct](#code-of-conduct)
- [Security](#security)
- [Licensing](#licensing)
- [Copyright](#copyright)

<!-- /TOC -->

## OpenSearch Anomaly Detection

The OpenSearch Anomaly Detection plugin enables you to leverage Machine Learning based algorithms to automatically detect anomalies as your log data is ingested. Combined with [OpenSearch Alerting](https://github.com/opensearch-project/alerting), you can monitor your data in near real time and automatically send alert notifications . With an intuitive OpenSearch Dashboards interface, and a powerful API, it is easy to set up, tune, and monitor your anomaly detectors.

## Highlights

Anomaly detection is using the [Random Cut Forest (RCF) algorithm](https://github.com/aws/random-cut-forest-by-aws) for detecting anomalous data points.

Anomaly detections run a scheduled job using [job-scheduler](https://github.com/opensearch-project/job-scheduler).

You should use anomaly detection plugin with the same version of [OpenSearch Alerting](https://github.com/opensearch-project/alerting). You can also create a monitor based on the anomaly detector. A scheduled monitor run checks the anomaly detection results regularly, and collects anomalies to trigger alerts based on custom trigger conditions.
  
## Documentation

Please see [our documentation](https://docs-beta.opensearch.org/monitoring-plugins/ad/index/).

## Contributing

We welcome you to get involved in development, documentation, and testing of the anomaly detection plugin.

See our [contribution guidelines](CONTRIBUTING.md) and the [developer guide](DEVELOPER_GUIDE.md) to get started.

If you are looking for a quick contribution, we still don't have 100% unit test coverage for now. Check out [GitHub issues](https://github.com/opensearch-project/anomaly-detection/issues) for other ideas.

## Code of Conduct

This project has adopted an [Open Source Code of Conduct](CODE_OF_CONDUCT.md).

## Security

If you discover a potential security issue in this project we ask that you notify AWS/Amazon Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do **not** create a public GitHub issue.

## Licensing

See the [LICENSE](LICENSE.txt) file for our project's licensing. We will ask you to confirm the licensing of your contribution.

## Copyright

Copyright 2021 OpenSearch Contributors
