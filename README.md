[![AD Test](https://github.com/opensearch-project/anomaly-detection/workflows/Build%20and%20Test%20Anomaly%20detection/badge.svg)](https://github.com/opensearch-project/anomaly-detection/actions?query=workflow%3A%22Build+and+Test+Anomaly+detection%22+branch%3A%22main%22)
[![codecov](https://codecov.io/gh/opensearch-project/anomaly-detection/branch/main/graph/badge.svg?flag=plugin)](https://codecov.io/gh/opensearch-project/anomaly-detection)
[![Documentation](https://img.shields.io/badge/doc-reference-blue)](https://docs-beta.opensearch.org/monitoring-plugins/ad/index/)
[![Forum](https://img.shields.io/badge/chat-on%20forums-blue)](https://discuss.opendistrocommunity.dev/c/Use-this-category-for-all-questions-around-machine-learning-plugins)
![PRs welcome!](https://img.shields.io/badge/PRs-welcome!-success)

<img src="https://opensearch.org/assets/brand/SVG/Logo/opensearch_logo_default.svg" height="64px"/>

<!-- TOC -->

- [OpenSearch Anomaly Detection](#opensearch-anomaly-detection)
- [Documentation](#documentation)
- [Contributing](#contributing)
- [Code of Conduct](#code-of-conduct)
- [Security](#security)
- [License](#license)
- [Copyright](#copyright)

<!-- /TOC -->

## OpenSearch Anomaly Detection

The OpenSearch Anomaly Detection plugin enables you to leverage Machine Learning based algorithms to automatically detect anomalies as your log data is ingested. Combined with [OpenSearch Alerting](https://github.com/opensearch-project/alerting), you can monitor your data in near real time and automatically send alert notifications . With an intuitive OpenSearch Dashboards interface, and a powerful API, it is easy to set up, tune, and monitor your anomaly detectors.

Anomaly detection is using the [Random Cut Forest (RCF) algorithm](https://github.com/aws/random-cut-forest-by-aws) for detecting anomalous data points.

Anomaly detections run a scheduled job using [job-scheduler](https://github.com/opensearch-project/job-scheduler).

You can use this plugin with the same version of the [OpenSearch Alerting Plugin](https://github.com/opensearch-project/alerting) to create monitors based on created anomaly detectors. A scheduled monitor run checks the anomaly detection results regularly, and collects anomalies to trigger alerts based on custom trigger conditions.
  
## Documentation

Please see [our documentation](https://docs-beta.opensearch.org/monitoring-plugins/ad/index/).


## Contributing

See [developer guide](DEVELOPER_GUIDE.md) and [how to contribute to this project](CONTRIBUTING.md).

## Code of Conduct

This project has adopted the [Amazon Open Source Code of Conduct](CODE_OF_CONDUCT.md). For more information see the [Code of Conduct FAQ](https://aws.github.io/code-of-conduct-faq), or contact [opensource-codeofconduct@amazon.com](mailto:opensource-codeofconduct@amazon.com) with any additional questions or comments.

## Security

If you discover a potential security issue in this project we ask that you notify AWS/Amazon Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do **not** create a public GitHub issue.

## License

This project is licensed under the [Apache v2.0 License](LICENSE.txt).

## Copyright

Copyright 2020-2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
