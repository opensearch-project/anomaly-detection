# Developer Guide

## Setup

1. Checkout source code of this package from the Github repo.
1. Launch Intellij IDEA, Choose Import Project and select the settings.gradle file in the root of this package.
1. To build from command line set JAVA_HOME to point to a JDK 14 before running `./gradlew`.

  * Unix System
    * export JAVA_HOME=jdk-install-dir: Replace jdk-install-dir by the JAVA_HOME directory of your system.
    * export PATH=$JAVA_HOME/bin:$PATH
  * Windows System
    * Find **My Computers** from file directory, right click and select **properties**.
    * Select the **Advanced** tab, select **Environment variables**.
    * Edit **JAVA_HOME** to path of where JDK software is installed.

## Build

This package uses the [Gradle](https://docs.gradle.org/current/userguide/userguide.html) build system. Gradle comes with excellent documentation that should be your first stop when trying to figure out how to operate or modify the build. we also use the OpenSearch build tools for Gradle. These tools are idiosyncratic and don't always follow the conventions and instructions for building regular Java code using Gradle. Not everything in this package will work the way it's described in the Gradle documentation. If you encounter such a situation, the OpenSearch build tools [source code](https://github.com/opensearch-project/OpenSearch/tree/main/buildSrc/src/main/groovy/org/opensearch/gradle) is your best bet for figuring out what's going on.

Currently we just put RCF jar in lib as dependency. Plan to publish to Maven and we can import it later. Before publishing to Maven, you can still build this package directly and find source code in RCF Github package.

### Building from the command line

1. `./gradlew build` builds and tests
1. `./gradlew :run` launches a single node cluster with anomaly-detection (and job-scheduler) plugin installed
1. `./gradlew :integTest` launches a single node cluster with anomaly-detection (and job-scheduler) plugin installed and runs all integration tests except security
1. ` ./gradlew :integTest --tests="**.test execute foo"` runs a single integration test class or method
1. `./gradlew integTest -Dtests.rest.cluster=localhost:9200 -Dtests.cluster=localhost:9200 -Dtests.clustername="docker-cluster" -Dhttps=true -Duser=admin -Dpassword=admin` launches integration tests against a local cluster and run tests with security
1. `./gradlew spotlessApply` formats code. And/or import formatting rules in `.eclipseformat.xml` with IDE.

When launching a cluster using one of the above commands logs are placed in `/build/cluster/run node0/opensearch-<version>/logs`. Though the logs are teed to the console, in practices it's best to check the actual log file.

### Building from the IDE

Currently, the only IDE we support is IntelliJ IDEA.  It's free, it's open source, it works. The gradle tasks above can also be launched from IntelliJ's Gradle toolbar and the extra parameters can be passed in via the Launch Configurations VM arguments. 

### Debugging

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

### Advanced: Launching multi node clusters locally

Sometimes you need to launch a cluster with more than one OpenSearch server process.

You can do this by running `./gradlew run -PnumNodes=<numberOfNodesYouWant>`

You can also debug a multi-node cluster, by using a combination of above multi-node and debug steps.
But, you must set up debugger configurations to listen on each port starting from `5005` and increasing by 1 for each node.