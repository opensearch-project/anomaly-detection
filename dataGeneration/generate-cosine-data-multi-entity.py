'''

Python script for ingesting sample data into opensearch index

'''

#{"index": {"_index":"host-cloudwatch","_id":"1177"}}
#{"@timestamp":"2017-03-23T13:00:00","cpu":20.3, "memory":13,"host":"host1", "service": "service1"}

import numpy as np
from scipy.stats import uniform
import datetime
import time
import random
from random import Random
from retry import retry
import urllib3
import concurrent.futures
import argparse

from opensearchpy import OpenSearch, RequestsHttpConnection
from opensearchpy import helpers

# https://urllib3.readthedocs.io/en/latest/advanced-usage.html#ssl-warnings
urllib3.disable_warnings()


parser = argparse.ArgumentParser()

parser.add_argument("-ep", "--endpoint", help="cluster endpoint", required=True)
parser.add_argument("-i", "--index-name", help=" ",required=True)
parser.add_argument("-shards", "--shards", type=int, help="The number of shards for the given index", required=True)
parser.add_argument("-t", "--threads", type=int, help="The number of threads to be used for data ingestion, make sure given machine has enough", required=True)
parser.add_argument("-bulk", "--bulk-size", type=int, default=3000, help="Number of documents per bulk request, default to 3000", )
# parser.add_argument("-s", "--security", default=False, dest="security", action="store_true")
parser.add_argument("-ingest", "--ingestion-frequency", type=int, default=600, help="how often each respective document is indexed, for example the default is 600 seconds which equates to every 10 minutes")
parser.add_argument("-p", "--points", type=int, default=1008, help="total number of points ingested, for example with 1008 points and a frequency of 600s, there will be 7 days of data")

parser.add_argument('--security', action='store_true')
parser.add_argument('--no-security', dest='security', action='store_false')
parser.set_defaults(security=False)


parser.add_argument("-nh", "--number-of-host", type=int, default=1000, help="number of 'host' entities, deafult is set to 1000, there will be two keyword categories in this index")
parser.add_argument("-np", "--number-of-process", type=int, default=1000, help="number of 'process' entities, deafult is set to 1000, there will be two keyword categories in this index" )
parser.add_argument("-hd", "--number-of-historical-days", type=int, default=2, help="number of day of historical data to ingest, defaults to 2")
parser.add_argument("-u", "--username", type=str, default="admin", help="username for authentication if security is true")
parser.add_argument("-pass", "--password", type=str, default="admin", help="password for authentication if security is true")


args = parser.parse_args()


print(args)
URL = args.endpoint
SECURITY = args.security
INDEX_NAME = args.index_name
SHARD_NUMBER = args.shards
THREADS = args.threads

#deafult numbers of 1000 host and 1000 process mean a total of 1 million entities
HOST_NUMBER = args.number_of_host
PROCESS_NUMBER = args.number_of_process

#default of 1008 points with ingestion frequency set to 600 means there will basically be 1008 intervals = 7 days * 144 intervals/day
POINTS = args.points 
INGESTION_FREQUENCY = args.ingestion_frequency
BULK_SIZE = args.bulk_size
USERNAME = args.username
PASSWORD = args.password
NUMBER_OF_HISTORICAL_DAYS = args.number_of_historical_days


index_name = "_index"
timestamp_name = "@timestamp"
cpu_name = "cpuTime"
mem_name = "jvmGcTime"
host_name = "host"
host_prefix = "host"
process_name = "process"
process_prefix = "process"
client = []

'''
    Generate index INDEX_NAME
'''
def create_index(es, INDEX_NAME, shard_number):
    # First, delete the index if it exists
    print("Deleting index if it exists...")
    es.indices.delete(index=INDEX_NAME, ignore=[400, 404])

    # Next, create the index
    print("Creating index \"{}\"...".format(INDEX_NAME))
    request_body = {
        "settings":{
          "number_of_shards":shard_number,
          "number_of_replicas": 0, # increase this number after indexing
          "translog.durability":"async", # default: request
          "refresh_interval":-1, # default: 1, remember to change this after finishing indexing process or just _refresh once at least if index wont be changed again
       },
       "mappings":{
          "properties":{
             "@timestamp":{
                "type":"date"
             },
             "cpuTime":{
                "type":"double"
             },
             "jvmGcTime":{
                "type":"double"
             },
             "host":{
                "type":"keyword"
             },
             "process":{
                "type":"keyword"
             }
          }
       }
    }

    es.indices.create(index=INDEX_NAME, body=request_body)


'''
    Posts a document(s) to the index
'''
@retry(delay=2)
def post_log(bulk_payload, thread_index):
    global client
    helpers.bulk(client[thread_index], bulk_payload)

def generate_val(amp, phase, base_dimension, index, period, noise, noiseprg):
    data = np.empty(base_dimension, dtype=float)
    for j in range(0, base_dimension):
        # cos is [-1, 1], + 1 make it non-negative
        data[j] = amp[j] * (np.cos(2 * np.pi * (index + phase[j]) / period) + 1) + noise * noiseprg.random()
        if (noiseprg.random() < 0.01 and noiseprg.random() < 0.3):
            factor = 5 * (1 + noiseprg.random())
            change = factor * noise if noiseprg.random() < 0.5 else -factor * noise
            if data[j] + change >= 0:
                data[j] += change
    return data

'''
    Posts all documents to index in stream
'''
def post_log_stream(index_value, time_intervals, sample_per_interval, max_number, min_number, host_number, service_number, batch_size, thread_index, cosine_params):
    # For each file, post all the docs
    print("Posting logs...")
    bulk_payload = list()
    # give some data in the history for cold start
    dtFormat = "%Y-%m-%dT%H:%M:%S"
    startTs = datetime.datetime.utcnow() - datetime.timedelta(days=NUMBER_OF_HISTORICAL_DAYS)
    count = 0
    totalCount = 0
    lastTotalCount = 0

    keep_loop = True
    j = (int)(min_number / service_number)
    index = j * service_number - 1

    try:
        while keep_loop and j < host_number:
            host_str = host_prefix + str(j)
            for l in range(service_number):
                process_str = process_prefix + str(l)
                index += 1
                # index can be [min_number, max_number]
                if index < min_number:
                    continue
                if index > max_number:
                    keep_loop = False
                    break
                nextTs = startTs
                prb = Random()
                prb.seed(random.randint(0, 100000000))
                cosine_p = cosine_params[index]
                data_index = 0
                for i in range(0, time_intervals):
                    ts = nextTs.strftime(dtFormat)
                    for k in range(0, sample_per_interval):
                        data = generate_val(cosine_p[1], cosine_p[0], 2, data_index,
                                            50, 5, prb)
                        bulk_payload.append(
                            {
                                index_name: index_value,
                                "_source":
                                    {
                                        timestamp_name: ts,
                                        cpu_name: data[0],
                                        mem_name: data[1],
                                        host_name: host_str,
                                        process_name: process_str
                                    }
                            }
                        )
                        count += 1
                        data_index += 1
                        if count >= batch_size:
                            post_log(bulk_payload, thread_index)
                            bulk_payload = list()  # reset list
                            totalCount += count
                            count = 0
                    # increment by ingestion_frequency (in seconds) after looping through each host multiple samples
                    nextTs += datetime.timedelta(seconds=INGESTION_FREQUENCY)
                    if totalCount - lastTotalCount > 1000000:
                        # report progress every 1 million inserts
                        print("totalCount {} thread_index {}".format(totalCount,
                                                                     thread_index))
                        lastTotalCount = totalCount
            j += 1

        if len(bulk_payload) > 0:
            post_log(bulk_payload, thread_index)
    except Error as err:
        print("error: {0}".format(err))

def split(a, n):
    k, m = divmod(len(a), n)
    return (a[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(n))

# create an list of array of size total_entities, the inner array has 2 subarrays: phase, amp
def create_cosine(total_entities, base_dimension, period, amplitude):
    cosine_param = np.empty(total_entities, dtype=object)
    for i in range(0, total_entities):
        phase = np.empty(base_dimension, dtype=float)
        amp = np.empty(base_dimension, dtype=float)
        for j in range(0, base_dimension):
            phase[j] = random.randint(0, period)
            amp[j] = (1 + 0.2 * random.random()) * amplitude
        cosine_param[i] = np.array([phase, amp])
    return cosine_param

'''
    Main entry method for script
'''
def main():
    global client
    print("security", SECURITY)
    if SECURITY and URL.strip() == 'localhost':
        for i in range(0, THREADS):
          client.append(OpenSearch(
            hosts=[URL],
            use_ssl=True,
            verify_certs=False,
            http_auth=(USERNAME, PASSWORD),
            scheme="https",
            connection_class=RequestsHttpConnection
         ))
    elif SECURITY:
        for i in range(0, THREADS):
            client.append(OpenSearch(
                hosts=[{'host': URL, 'port': 443}],
                use_ssl=True,
                verify_certs=False,
                http_auth=(USERNAME, PASSWORD),
                scheme="https",
                port=443,
                connection_class=RequestsHttpConnection
            ))
    elif URL.strip() == 'localhost':
        for i in range(0, THREADS):
         client.append(OpenSearch(
            hosts=[{'host': URL, 'port': 9200}],
            use_ssl=False,
            verify_certs=False,
            connection_class=RequestsHttpConnection
        ))
    else:
        es = OpenSearch(
            hosts=[{'host': URL, 'port': 80}],
            use_ssl=False,
            verify_certs=False,
            connection_class=RequestsHttpConnection
        )
    print(client)
    create_index(client[0], INDEX_NAME, SHARD_NUMBER)

    total_entities = HOST_NUMBER * PROCESS_NUMBER
    # https://tinyurl.com/yeter98e
    # workload is a list of ranges like [range(0, 10000), range(10000, 20000)]
    workload = list(split(range(total_entities), THREADS))
    futures = []

    # we we have both cpuTime and jvmGcTime field, so 2 features
    cosine_params = create_cosine(total_entities, 2, 50, 100)
    start = time.monotonic()
    with concurrent.futures.ProcessPoolExecutor(max_workers=THREADS) as executor:
        futures = []
        for i in range(len(workload)):
            # Using 1 sample per interval to reason about the result easier.
            doc_per_interval = 1
            futures.append(executor.submit(post_log_stream, INDEX_NAME, POINTS, doc_per_interval, workload[i][-1], workload[i][0], HOST_NUMBER, PROCESS_NUMBER, BULK_SIZE, i, cosine_params))
        _ = concurrent.futures.as_completed(futures)

    print('Concurrent took: %.2f minutes.' % ((time.monotonic() - start)/60))

if __name__ == "__main__":
    main()

