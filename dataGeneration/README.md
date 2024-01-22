# OpenSearch Anomaly Detection Data Ingestion


## Introduction
The following code in this directory can be used to easily ingest data into an OpenSearch cluster that is fit for AD testing and benchmarking.

## Instal Prerequisites

### Python

Python 3.8 or above is required

### pip

Use pip to install the necessary requirements:

```
pip install -r requirements.txt
```

## Usage

### Quick Start

In order to execute the script you must have a running OpenSearch cluster, so you can supply an endpoint for the data to be ingested too.

The current iteration of this data script creates data in a cosine pattern with anomalies injected with a random seed.

The dataset created will have two categorical fields to test a multi-entity AD (`host` and `process` of type `keyword`) and two fields that can act as the two features fields (`cpuTime` and `jvmGcTime` of type `double`).

### Example Request:

`python3 generate-cosine-data-multi-entity.py -ep amit-test-cluster-0bf68dc1.elb.us-east-1.amazonaws.com -i test-index-1 -shards 5 -t 10 -p 30 --security`

- This will start data ingestion to the cluster with the given endpoint, creating an index called `test-index-1`, with 5 shards, utilizing 10 threads, for 30 points in time and with security turned on.
- The rest of the values not given in this example are set to the default explained below.
- To give further context there will be a 1 document created for every unique entity combination for every 'interval' which is defined at 600s (10 minutes) at default for 30 'intervals'.

### Ingestion Parameters

| Parameter Name | Description | Default |  Required
| ----------- | ----------- | ----------- | ----------- |
| --endpoint | Endpoint OpenSearch cluster is running on | No default | Yes
| --index-name | Name of index that will be created and ingested too | No default | Yes
| --threads | Number of threads to be used for data ingestion | No deafult | Yes
| --shards | Number of shards for given index | 5 | No
| --bulk-size | Number of documents per bulk request | 3000 | No
| --ingestion-frequency | How often each respective document is indexed (in seconds) | 600 | No
| --points | Total number of points in time ingested | 1008 | No
| --number-of-host | number of 'host' entities (host is one of the categorical field that an entity is defined by) | 1000 | No
| --number-of-process | number of 'process' entities (process is one of the categorical field that an entity is defined by)| 1000 | No
| --number-of-historical-days | number of day of historical data to ingest | 2 | No
| --username | username for authentication if security is true | admin | No
| --password | password for authentication if security is true | <admin password> | No


### Ingestion Commands

| Command Name | Description | Required
| ----------- | ----------- | ----------- |
| --security | sets security to true for creating client to index to cluster endpoint | NO
| --no-security | sets security to true for creating client to index to cluster endpoint | No

- If no command is given then the default is to set security to true


