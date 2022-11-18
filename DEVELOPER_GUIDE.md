- [Developer Guide](#developer-guide)
  - [Forking and Cloning](#forking-and-cloning)
  - [Install Prerequisites](#install-prerequisites)
    - [JDK 14](#jdk-14)
  - [Setup](#setup)
  - [Build](#build)
    - [Building from the command line](#building-from-the-command-line)
    - [Building from the IDE](#building-from-the-ide)
    - [Debugging](#debugging)
    - [Advanced: Launching multi node clusters locally](#advanced-launching-multi-node-clusters-locally)
  - [Backports](#backports)
  - [Gradle Plugins](#gradle-plugins)
    - [Distribution Download Plugin](#distribution-download-plugin)

## Developer Guide

### Forking and Cloning

Fork this repository on GitHub, and clone locally with `git clone`.

### Install Prerequisites

#### JDK 14

OpenSearch components build using Java 14 at a minimum. This means you must have a JDK 14 installed with the environment variable `JAVA_HOME` referencing the path to Java home for your JDK 14 installation, e.g. `JAVA_HOME=/usr/lib/jvm/jdk-14`.

### Setup

1. Clone the repository (see [Forking and Cloning](#forking-and-cloning))
2. Make sure `JAVA_HOME` is pointing to a Java 14 JDK (see [Install Prerequisites](#install-prerequisites))
3. Launch Intellij IDEA, Choose Import Project and select the settings.gradle file in the root of this package.

### Build

This package uses the [Gradle](https://docs.gradle.org/current/userguide/userguide.html) build system. Gradle comes with excellent documentation that should be your first stop when trying to figure out how to operate or modify the build. we also use the OpenSearch build tools for Gradle. These tools are idiosyncratic and don't always follow the conventions and instructions for building regular Java code using Gradle. Not everything in this package will work the way it's described in the Gradle documentation. If you encounter such a situation, the OpenSearch build tools [source code](https://github.com/opensearch-project/OpenSearch/tree/main/buildSrc/src/main/groovy/org/opensearch/gradle) is your best bet for figuring out what's going on.

Currently we just put RCF jar in lib as dependency. Plan to publish to Maven and we can import it later. Before publishing to Maven, you can still build this package directly and find source code in RCF Github package.

#### Building from the command line

1. `./gradlew build` builds and tests
2. `./gradlew :run` launches a single node cluster with anomaly-detection (and job-scheduler) plugin installed
3. `./gradlew :integTest` launches a single node cluster with anomaly-detection (and job-scheduler) plugin installed and runs all integration tests except security
4. ` ./gradlew :integTest --tests="**.test execute foo"` runs a single integration test class or method
5. `./gradlew integTestRemote -Dtests.rest.cluster=localhost:9200 -Dtests.cluster=localhost:9200 -Dtests.clustername="docker-cluster" -Dhttps=true -Duser=admin -Dpassword=admin` launches integration tests against a local cluster and run tests with security
6. `./gradlew spotlessApply` formats code. And/or import formatting rules in `.eclipseformat.xml` with IDE.
7. `./gradlew adBwcCluster#mixedClusterTask -Dtests.security.manager=false` launches a cluster with three nodes of bwc version of OpenSearch with anomaly-detection and job-scheduler and tests backwards compatibility by upgrading one of the nodes with the current version of OpenSearch with anomaly-detection and job-scheduler creating a mixed cluster.
8. `./gradlew adBwcCluster#rollingUpgradeClusterTask -Dtests.security.manager=false` launches a cluster with three nodes of bwc version of OpenSearch with anomaly-detection and job-scheduler and tests backwards compatibility by performing rolling upgrade of each node with the current version of OpenSearch with anomaly-detection and job-scheduler.
9. `./gradlew adBwcCluster#fullRestartClusterTask -Dtests.security.manager=false` launches a cluster with three nodes of bwc version of OpenSearch with anomaly-detection and job-scheduler and tests backwards compatibility by performing a full restart on the cluster upgrading all the nodes with the current version of OpenSearch with anomaly-detection and job-scheduler.
10. `./gradlew bwcTestSuite -Dtests.security.manager=false` runs all the above bwc tests combined.
11. `./gradlew ':test' --tests "org.opensearch.ad.ml.HCADModelPerfTests" -Dtests.seed=2AEBDBBAE75AC5E0 -Dtests.security.manager=false -Dtests.locale=es-CU -Dtests.timezone=Chile/EasterIsland -Dtest.logs=true -Dmodel-benchmark=true` launches HCAD model performance tests and logs the result in the standard output
12. `./gradlew integTest --tests "org.opensearch.ad.e2e.SingleStreamModelPerfIT" -Dtests.seed=60CDDB34427ACD0C -Dtests.security.manager=false -Dtests.locale=kab-DZ -Dtests.timezone=Asia/Hebron -Dtest.logs=true -Dmodel-benchmark=true` launches single stream AD model performance tests and logs the result in the standard output

When launching a cluster using one of the above commands logs are placed in `/build/cluster/run node0/opensearch-<version>/logs`. Though the logs are teed to the console, in practices it's best to check the actual log file.

#### Building from the IDE

Currently, the only IDE we support is IntelliJ IDEA.  It's free, it's open source, it works. The gradle tasks above can also be launched from IntelliJ's Gradle toolbar and the extra parameters can be passed in via the Launch Configurations VM arguments. 

#### Debugging

Sometimes it's useful to attach a debugger to either the OpenSearch cluster or the integ tests to see what's going on. When running unit tests you can just hit 'Debug' from the IDE's gutter to debug the tests.  To debug code running in an actual server run:

```
./gradlew :integTest --debug-jvm # to start a cluster and run integ tests
OR
./gradlew :run --debug-jvm # to just start a cluster that can be debugged
```

The OpenSearch server JVM will launch suspended and wait for a debugger to attach to `localhost:8000` before starting the OpenSearch server.

To debug code running in an integ test (which exercises the server from a separate JVM) run:

```
./gradlew -Dtest.debug :integTest 
```

The test runner JVM will start suspended and wait for a debugger to attach to `localhost:5005` before running the tests.

#### Advanced: Launching multi node clusters locally

Sometimes you need to launch a cluster with more than one OpenSearch server process.

You can do this by running `./gradlew run -PnumNodes=<numberOfNodesYouWant>`

You can also debug a multi-node cluster, by using a combination of above multi-node and debug steps.
But, you must set up debugger configurations to listen on each port starting from `5005` and increasing by 1 for each node.

### Backports

The Github workflow in [`backport.yml`](.github/workflows/backport.yml) creates backport PRs automatically when the
original PR with an appropriate label `backport <backport-branch-name>` is merged to main with the backport workflow
run successfully on the PR. For example, if a PR on main needs to be backported to `1.x` branch, add a label
`backport 1.x` to the PR and make sure the backport workflow runs on the PR along with other checks. Once this PR is
merged to main, the workflow will create a backport PR to the `1.x` branch.

### Gradle Plugins

#### Distribution Download Plugin

The Distribution Download plugin downloads the latest version of OpenSearch by default, and supports overriding this behavior by setting `customDistributionUrl`. This will help to pull artifacts from custom location for testing during release process.
```
./gradlew integTest -PcustomDistributionUrl="https://ci.opensearch.org/ci/dbc/bundle-build/1.2.0/1127/linux/x64/dist/opensearch-1.2.0-linux-x64.tar.gz"
```
