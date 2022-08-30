/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
package org.opensearch.search.aggregations.metrics;


*/
/**
 * Run tests in ES package since InternalCardinality has only package private constructors
 * and we cannot mock it since it is a final class.
 *
 *//*
    
    public class CardinalityProfileTests extends AbstractProfileRunnerTests {
     enum ADResultStatus {
         NO_RESULT,
         EXCEPTION
     }
    
     enum CardinalityStatus {
         EXCEPTION,
         NORMAL
     }
    
     @SuppressWarnings("unchecked")
     private void setUpMultiEntityClientGet(DetectorStatus detectorStatus, JobStatus jobStatus, ErrorResultStatus errorResultStatus)
         throws IOException {
         detector = TestHelpers
             .randomAnomalyDetectorWithInterval(new IntervalTimeConfiguration(detectorIntervalMin, ChronoUnit.MINUTES), true);
         doAnswer(invocation -> {
             Object[] args = invocation.getArguments();
             GetRequest request = (GetRequest) args[0];
             ActionListener<GetResponse> listener = (ActionListener<GetResponse>) args[1];
    
             if (request.index().equals(ANOMALY_DETECTORS_INDEX)) {
                 switch (detectorStatus) {
                     case EXIST:
                         listener
                             .onResponse(
                                 TestHelpers.createGetResponse(detector, detector.getDetectorId(), AnomalyDetector.ANOMALY_DETECTORS_INDEX)
                             );
                         break;
                     default:
                         assertTrue("should not reach here", false);
                         break;
                 }
             } else if (request.index().equals(ANOMALY_DETECTOR_JOB_INDEX)) {
                 AnomalyDetectorJob job = null;
                 switch (jobStatus) {
                     case ENABLED:
                         job = TestHelpers.randomAnomalyDetectorJob(true);
                         listener
                             .onResponse(
                                 TestHelpers.createGetResponse(job, detector.getDetectorId(), AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX)
                             );
                         break;
                     default:
                         assertTrue("should not reach here", false);
                         break;
                 }
             } else if (request.index().equals(CommonName.DETECTION_STATE_INDEX)) {
                 switch (errorResultStatus) {
                     case NO_ERROR:
                         listener.onResponse(null);
                         break;
                     case NULL_POINTER_EXCEPTION:
                         GetResponse response = mock(GetResponse.class);
                         when(response.isExists()).thenReturn(true);
                         doThrow(NullPointerException.class).when(response).getSourceAsString();
                         listener.onResponse(response);
                         break;
                     default:
                         assertTrue("should not reach here", false);
                         break;
                 }
             }
             return null;
         }).when(client).get(any(), any());
     }
    
     @SuppressWarnings("unchecked")
     private void setUpMultiEntityClientSearch(ADResultStatus resultStatus, CardinalityStatus cardinalityStatus) {
         doAnswer(invocation -> {
             Object[] args = invocation.getArguments();
             ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) args[1];
             SearchRequest request = (SearchRequest) args[0];
             if (request.indices()[0].equals(CommonName.ANOMALY_RESULT_INDEX_ALIAS)) {
                 switch (resultStatus) {
                     case NO_RESULT:
                         SearchResponse mockResponse = mock(SearchResponse.class);
                         when(mockResponse.getHits()).thenReturn(TestHelpers.createSearchHits(0));
                         listener.onResponse(mockResponse);
                         break;
                     case EXCEPTION:
                         listener.onFailure(new RuntimeException());
                         break;
                     default:
                         assertTrue("should not reach here", false);
                         break;
                 }
             } else {
                 switch (cardinalityStatus) {
                     case EXCEPTION:
                         listener.onFailure(new RuntimeException());
                         break;
                     case NORMAL:
                         SearchResponse response = mock(SearchResponse.class);
                         List<InternalAggregation> aggs = new ArrayList<>(1);
                         HyperLogLogPlusPlus hyperLogLog = new HyperLogLogPlusPlus(
                             AbstractHyperLogLog.MIN_PRECISION,
                             BigArrays.NON_RECYCLING_INSTANCE,
                             0
                         );
                         for (int i = 0; i < 100; i++) {
                             hyperLogLog.collect(0, BitMixer.mix64(randomIntBetween(1, 100)));
                         }
                         aggs.add(new InternalCardinality(CommonName.TOTAL_ENTITIES, hyperLogLog, new HashMap<>()));
                         when(response.getAggregations()).thenReturn(InternalAggregations.from(aggs));
                         listener.onResponse(response);
                         break;
                     default:
                         assertTrue("should not reach here", false);
                         break;
                 }
    
             }
    
             return null;
         }).when(client).search(any(), any());
     }
    
     @SuppressWarnings("unchecked")
     private void setUpProfileAction() {
         doAnswer(invocation -> {
             Object[] args = invocation.getArguments();
    
             ActionListener<ProfileResponse> listener = (ActionListener<ProfileResponse>) args[2];
    
             ProfileNodeResponse profileNodeResponse1 = new ProfileNodeResponse(
                 discoveryNode1,
                 new HashMap<>(),
                 shingleSize,
                 0,
                 0,
                 new ArrayList<>(),
                 0
             );
             List<ProfileNodeResponse> profileNodeResponses = Arrays.asList(profileNodeResponse1);
             listener.onResponse(new ProfileResponse(new ClusterName(clusterName), profileNodeResponses, Collections.emptyList()));
    
             return null;
         }).when(client).execute(eq(ProfileAction.INSTANCE), any(), any());
     }
    
     public void testFailGetEntityStats() throws IOException, InterruptedException {
         setUpMultiEntityClientGet(DetectorStatus.EXIST, JobStatus.ENABLED, ErrorResultStatus.NO_ERROR);
         setUpMultiEntityClientSearch(ADResultStatus.NO_RESULT, CardinalityStatus.EXCEPTION);
         setUpProfileAction();
    
         final CountDownLatch inProgressLatch = new CountDownLatch(1);
    
         runner.profile(detector.getDetectorId(), ActionListener.wrap(response -> {
             assertTrue("Should not reach here ", false);
             inProgressLatch.countDown();
         }, exception -> {
             assertTrue(exception instanceof RuntimeException);
             // this means we don't exit with failImmediately. failImmediately can make we return early when there are other concurrent
             // requests
             assertTrue(exception.getMessage(), exception.getMessage().contains("Exceptions:"));
             inProgressLatch.countDown();
    
         }), totalInitProgress);
    
         assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
     }
    
     public void testNoResultsNoError() throws IOException, InterruptedException {
         setUpMultiEntityClientGet(DetectorStatus.EXIST, JobStatus.ENABLED, ErrorResultStatus.NO_ERROR);
         setUpMultiEntityClientSearch(ADResultStatus.NO_RESULT, CardinalityStatus.NORMAL);
         setUpProfileAction();
    
         final AtomicInteger called = new AtomicInteger(0);
    
         runner.profile(detector.getDetectorId(), ActionListener.wrap(response -> {
             assertTrue(response.getInitProgress() != null);
             called.getAndIncrement();
         }, exception -> {
             assertTrue("Should not reach here ", false);
             called.getAndIncrement();
         }), totalInitProgress);
    
         while (called.get() == 0) {
             Thread.sleep(100);
         }
         // should only call onResponse once
         assertEquals(1, called.get());
     }
    
     public void testFailConfirmInitted() throws IOException, InterruptedException {
         setUpMultiEntityClientGet(DetectorStatus.EXIST, JobStatus.ENABLED, ErrorResultStatus.NO_ERROR);
         setUpMultiEntityClientSearch(ADResultStatus.EXCEPTION, CardinalityStatus.NORMAL);
         setUpProfileAction();
    
         final CountDownLatch inProgressLatch = new CountDownLatch(1);
    
         runner.profile(detector.getDetectorId(), ActionListener.wrap(response -> {
             assertTrue("Should not reach here ", false);
             inProgressLatch.countDown();
         }, exception -> {
             assertTrue(exception instanceof RuntimeException);
             // this means we don't exit with failImmediately. failImmediately can make we return early when there are other concurrent
             // requests
             assertTrue(exception.getMessage(), exception.getMessage().contains("Exceptions:"));
             inProgressLatch.countDown();
    
         }), totalInitProgress);
    
         assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
     }
    }
    */
