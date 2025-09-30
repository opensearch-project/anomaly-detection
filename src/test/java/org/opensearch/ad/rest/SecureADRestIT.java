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

package org.opensearch.ad.rest;

import static org.opensearch.timeseries.TestHelpers.patchSharingInfo;
import static org.opensearch.timeseries.TestHelpers.shareConfig;
import static org.opensearch.timeseries.TestHelpers.shareWithUserPayload;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.message.BasicHeader;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.ad.AnomalyDetectorRestTestCase;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyDetectorExecutionInput;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.commons.authuser.User;
import org.opensearch.commons.rest.SecureRestClientBuilder;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.security.spi.resources.sharing.Recipient;
import org.opensearch.security.spi.resources.sharing.Recipients;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.model.DateRange;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

import com.google.common.collect.ImmutableList;

public class SecureADRestIT extends AnomalyDetectorRestTestCase {
    String aliceUser = "alice";
    RestClient aliceClient;
    String bobUser = "bob";
    RestClient bobClient;
    String catUser = "cat";
    RestClient catClient;
    String dogUser = "dog";
    RestClient dogClient;
    String elkUser = "elk";
    RestClient elkClient;
    String fishUser = "fish";
    RestClient fishClient;
    String goatUser = "goat";
    RestClient goatClient;
    String lionUser = "lion";
    RestClient lionClient;

    private static final String READ_ONLY_AG = "ad_read_only";
    private static final String READ_WRITE_AG = "ad_read_write";
    private static final String FULL_ACCESS_AG = "ad_full_access";

    String oceanUser = "ocean";
    RestClient oceanClient;

    @Before
    public void setupSecureTests() throws IOException {
        if (!isHttps()) {
            throw new IllegalArgumentException("Secure Tests are running but HTTPS is not set");
        }

        String indexAllAccessRole = "index_all_access";
        createIndexRole(indexAllAccessRole, "*");
        String indexSearchAccessRole = "index_all_search";
        createSearchRole(indexSearchAccessRole, "*");
        String alicePassword = generatePassword(aliceUser);
        createUser(aliceUser, alicePassword, new ArrayList<>(Arrays.asList("odfe")));
        aliceClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), aliceUser, alicePassword)
            .setSocketTimeout(60000)
            .build();

        String bobPassword = generatePassword(bobUser);
        createUser(bobUser, bobPassword, new ArrayList<>(Arrays.asList("odfe")));
        bobClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), bobUser, bobPassword)
            .setSocketTimeout(60000)
            .build();

        String catPassword = generatePassword(catUser);
        createUser(catUser, catPassword, new ArrayList<>(Arrays.asList("aes")));
        catClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), catUser, catPassword)
            .setSocketTimeout(60000)
            .build();

        String dogPassword = generatePassword(dogUser);
        createUser(dogUser, dogPassword, new ArrayList<>(Arrays.asList()));
        dogClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), dogUser, dogPassword)
            .setSocketTimeout(60000)
            .build();

        String elkPassword = generatePassword(elkUser);
        createUser(elkUser, elkPassword, new ArrayList<>(Arrays.asList("odfe")));
        elkClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), elkUser, elkPassword)
            .setSocketTimeout(60000)
            .build();

        String fishPassword = generatePassword(fishUser);
        createUser(fishUser, fishPassword, new ArrayList<>(Arrays.asList("odfe", "aes")));
        fishClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), fishUser, fishPassword)
            .setSocketTimeout(60000)
            .build();

        String goatPassword = generatePassword(goatUser);
        createUser(goatUser, goatPassword, new ArrayList<>(Arrays.asList("opensearch")));
        goatClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), goatUser, goatPassword)
            .setSocketTimeout(60000)
            .build();

        String lionPassword = generatePassword(lionUser);
        createUser(lionUser, lionPassword, new ArrayList<>(Arrays.asList("opensearch")));
        lionClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), lionUser, lionPassword)
            .setSocketTimeout(60000)
            .build();

        String oceanPassword = generatePassword(oceanUser);
        createUser(oceanUser, oceanPassword, new ArrayList<>(Arrays.asList("odfe")));
        oceanClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), oceanUser, oceanPassword)
            .setSocketTimeout(60000)
            .build();

        createRoleMapping("anomaly_read_access", new ArrayList<>(Arrays.asList(bobUser, oceanUser)));
        createRoleMapping("anomaly_full_access", new ArrayList<>(Arrays.asList(aliceUser, catUser, dogUser, elkUser, fishUser, goatUser)));
        createRoleMapping(indexAllAccessRole, new ArrayList<>(Arrays.asList(aliceUser, bobUser, catUser, dogUser, fishUser, lionUser)));
        createRoleMapping(indexSearchAccessRole, new ArrayList<>(Arrays.asList(goatUser)));
    }

    @After
    public void deleteUserSetup() throws IOException {
        aliceClient.close();
        bobClient.close();
        catClient.close();
        dogClient.close();
        elkClient.close();
        fishClient.close();
        goatClient.close();
        lionClient.close();
        oceanClient.close();
        deleteUser(aliceUser);
        deleteUser(bobUser);
        deleteUser(catUser);
        deleteUser(dogUser);
        deleteUser(elkUser);
        deleteUser(fishUser);
        deleteUser(goatUser);
        deleteUser(lionUser);
        deleteUser(oceanUser);
    }

    private void confirmingClientIsAdmin() throws IOException {
        Response resp = TestHelpers
            .makeRequest(
                client(),
                "GET",
                "_plugins/_security/api/account",
                null,
                "",
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "admin"))
            );
        Map<String, Object> responseMap = entityAsMap(resp);
        ArrayList<String> roles = (ArrayList<String>) responseMap.get("roles");
        assertTrue(roles.contains("all_access"));
    }

    public void testCreateAnomalyDetector() throws IOException {

        if (isResourceSharingFeatureEnabled()) {
            // If resource sharing is enabled, we allow creation of detectors regardless of the user's backend roles

            AnomalyDetector detectorOfDog = createRandomAnomalyDetector(false, false, dogClient);
            assertNotNull(detectorOfDog.getId());

            AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);
            // User ocean has AD full access, but has no write permission of index
            String indexName = aliceDetector.getIndices().getFirst();
            Exception exception = expectThrows(
                IOException.class,
                () -> { createRandomAnomalyDetector(false, false, indexName, oceanClient); }
            );
            Assert
                .assertTrue(
                    "actual: " + exception.getMessage(),
                    exception.getMessage().contains("no permissions for [cluster:admin/opendistro/ad/detector/write]")
                );
            // User Bob has AD read access, should not be able to create a detector
            exception = expectThrows(IOException.class, () -> { createRandomAnomalyDetector(false, false, bobClient); });
            Assert.assertTrue(exception.getMessage().contains("no permissions for [cluster:admin/opendistro/ad/detector/write]"));

            // With Custom Result Index
            String resultIndex = ADCommonName.CUSTOM_RESULT_INDEX_PREFIX + "test";
            AnomalyDetector detector = cloneDetector(aliceDetector, resultIndex);
            // User goat doesn't have permission to create index
            exception = expectThrows(IOException.class, () -> { createAnomalyDetector(detector, true, goatClient); });
            Assert
                .assertTrue(
                    "got " + exception.getMessage(),
                    exception.getMessage().contains("indices:admin/aliases")
                        && exception.getMessage().contains("indices:admin/create")
                        && exception.getMessage().contains("no permissions for")
                );

            // User cat has permission to create index
            resultIndex = ADCommonName.CUSTOM_RESULT_INDEX_PREFIX + "test2";
            TestHelpers.createIndexWithTimeField(client(), aliceDetector.getIndices().getFirst(), aliceDetector.getTimeField());
            AnomalyDetector detectorOfCat = createAnomalyDetector(cloneDetector(aliceDetector, resultIndex), true, catClient);
            assertEquals(resultIndex, detectorOfCat.getCustomResultIndexOrAlias());

        } else {

            // 1. NO Backend_role

            enableFilterBy();
            // User Dog has AD full access, but has no backend role
            // When filter by is enabled, we block creating Detectors
            Exception exception = expectThrows(IOException.class, () -> { createRandomAnomalyDetector(false, false, dogClient); });
            Assert
                .assertTrue(
                    exception
                        .getMessage()
                        .contains("Filter by backend roles is enabled and User dog does not have backend roles configured")
                );

            disableFilterBy();
            // When filter by is enabled, we allow creating Detector
            AnomalyDetector detectorOfDog = createRandomAnomalyDetector(false, false, dogClient);
            assertNotNull(detectorOfDog.getId());

            // 2. No read permission on index

            // 2.1
            enableFilterBy();
            // User alice has AD full access and index permission, so can create detector
            AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);
            // User ocean has AD full access, but has no write permission of index
            String indexName = aliceDetector.getIndices().getFirst();
            exception = expectThrows(IOException.class, () -> { createRandomAnomalyDetector(false, false, indexName, oceanClient); });
            Assert
                .assertTrue(
                    "actual: " + exception.getMessage(),
                    exception.getMessage().contains("no permissions for [cluster:admin/opendistro/ad/detector/write]")
                );
            // User Bob has AD read access, should not be able to create a detector
            exception = expectThrows(IOException.class, () -> { createRandomAnomalyDetector(false, false, bobClient); });
            Assert.assertTrue(exception.getMessage().contains("no permissions for [cluster:admin/opendistro/ad/detector/write]"));

            // 2.2
            disableFilterBy();
            // User Ocean is not allowed to create detector without write permission
            exception = expectThrows(IOException.class, () -> { createRandomAnomalyDetector(false, false, indexName, oceanClient); });
            Assert
                .assertTrue(
                    "actual: " + exception.getMessage(),
                    exception.getMessage().contains("no permissions for [cluster:admin/opendistro/ad/detector/write]")
                );

            // User Bob has AD read access, should not be able to create a detector
            exception = expectThrows(IOException.class, () -> { createRandomAnomalyDetector(false, false, bobClient); });
            Assert.assertTrue(exception.getMessage().contains("no permissions for [cluster:admin/opendistro/ad/detector/write]"));

            // 3. With Custom Result Index
            // 3.1
            enableFilterBy();
            String resultIndex = ADCommonName.CUSTOM_RESULT_INDEX_PREFIX + "test";
            AnomalyDetector detector = cloneDetector(aliceDetector, resultIndex);
            // User goat doesn't have permission to create index
            exception = expectThrows(IOException.class, () -> { createAnomalyDetector(detector, true, goatClient); });
            Assert
                .assertTrue(
                    "got " + exception.getMessage(),
                    exception.getMessage().contains("indices:admin/aliases")
                        && exception.getMessage().contains("indices:admin/create")
                        && exception.getMessage().contains("no permissions for")
                );

            // User cat has permission to create index
            resultIndex = ADCommonName.CUSTOM_RESULT_INDEX_PREFIX + "test2";
            TestHelpers.createIndexWithTimeField(client(), aliceDetector.getIndices().getFirst(), aliceDetector.getTimeField());
            AnomalyDetector detectorOfCat = createAnomalyDetector(cloneDetector(aliceDetector, resultIndex), true, catClient);
            assertEquals(resultIndex, detectorOfCat.getCustomResultIndexOrAlias());

            // 3.2
            disableFilterBy();
            // User goat doesn't have permission to create index
            exception = expectThrows(IOException.class, () -> { createAnomalyDetector(detector, true, goatClient); });
            Assert
                .assertTrue(
                    "got " + exception.getMessage(),
                    exception.getMessage().contains("indices:admin/aliases")
                        && exception.getMessage().contains("indices:admin/create")
                        && exception.getMessage().contains("no permissions for")
                );

            // User cat has permission to create index
            resultIndex = ADCommonName.CUSTOM_RESULT_INDEX_PREFIX + "test2";
            TestHelpers.createIndexWithTimeField(client(), aliceDetector.getIndices().getFirst(), aliceDetector.getTimeField());
            AnomalyDetector detectorOfCat2 = createAnomalyDetector(cloneDetector(aliceDetector, resultIndex), true, catClient);
            assertEquals(resultIndex, detectorOfCat2.getCustomResultIndexOrAlias());
        }
    }

    public void testGetDetector() throws IOException {
        // User Alice has AD full access, should be able to create a detector
        AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);

        // if resource sharing feature is enabled cat or admin client will not have access to aliceDetector regardless of backend role until
        // it is shared
        if (isResourceSharingFeatureEnabled()) {
            Exception exception = expectThrows(Exception.class, () -> { getConfig(aliceDetector.getId(), catClient); });
            assertTrue(exception.getMessage().contains("no permissions for [cluster:admin/opendistro/ad/detectors/get]"));

            exception = expectThrows(Exception.class, () -> { getConfig(aliceDetector.getId(), client()); });
            assertTrue(exception.getMessage().contains("no permissions for [cluster:admin/opendistro/ad/detectors/get]"));
            // test sharing

            // Share read-only with cat (by owner: alice)
            Response shareROWithCat = shareConfig(
                aliceClient,
                Map.of(),
                shareWithUserPayload(aliceDetector.getId(), ADCommonName.AD_RESOURCE_TYPE, READ_ONLY_AG, catUser)
            );
            assertEquals(200, shareROWithCat.getStatusLine().getStatusCode());
            waitForSharingVisibility(aliceDetector.getId(), catClient);

            // Cat can now GET
            AnomalyDetector detectorVisibleToCat = getConfig(aliceDetector.getId(), catClient);
            assertEquals(aliceDetector.getId(), detectorVisibleToCat.getId());

            // Non-owner (cat, only read-only) CANNOT share further
            Exception ex = expectThrows(Exception.class, () -> {
                shareConfig(
                    catClient,
                    Map.of(),
                    shareWithUserPayload(aliceDetector.getId(), ADCommonName.AD_RESOURCE_TYPE, READ_ONLY_AG, bobUser)
                );
            });
            assertTrue(ex.getMessage(), ex.getMessage().contains("no permissions") || ex.getMessage().contains("403"));

            // Grant full_access to elk (still by owner: alice)
            Response grantFullToElk = shareConfig(
                aliceClient,
                Map.of(),
                shareWithUserPayload(aliceDetector.getId(), ADCommonName.AD_RESOURCE_TYPE, FULL_ACCESS_AG, elkUser)
            );
            assertEquals(200, grantFullToElk.getStatusLine().getStatusCode());
            waitForSharingVisibility(aliceDetector.getId(), elkClient);

            // elk (now full_access) can PATCH sharing to backend_roles "aes" (cat belongs to it)
            Map<Recipient, Set<String>> recs = new HashMap<>();
            Set<String> backendRoles = new HashSet<>();
            backendRoles.add("aes");
            recs.put(Recipient.BACKEND_ROLES, backendRoles);
            Recipients recipients = new Recipients(recs);
            TestHelpers.PatchSharingInfoPayloadBuilder builder = new TestHelpers.PatchSharingInfoPayloadBuilder();
            builder.configId(aliceDetector.getId()).configType(ADCommonName.AD_RESOURCE_TYPE).share(recipients, READ_ONLY_AG);

            String patchShareAes = builder.build();

            Response elkAddsAes = patchSharingInfo(elkClient, Map.of(), patchShareAes);
            assertEquals(200, elkAddsAes.getStatusLine().getStatusCode());
            waitForSharingVisibility(aliceDetector.getId(), elkClient);

            // Revoke user-level share for cat (elk can revoke since elk has full_access)
            recs = new HashMap<>();
            Set<String> users = new HashSet<>();
            users.add(catUser);
            recs.put(Recipient.USERS, users);
            recipients = new Recipients(recs);
            builder = new TestHelpers.PatchSharingInfoPayloadBuilder();
            builder.configId(aliceDetector.getId()).configType(ADCommonName.AD_RESOURCE_TYPE).revoke(recipients, READ_ONLY_AG);

            String revokeUserCat = builder.build();

            Response elkRevokesUserCat = patchSharingInfo(elkClient, Map.of(), revokeUserCat);
            assertEquals(200, elkRevokesUserCat.getStatusLine().getStatusCode());

            // Cat still has access via backend_roles "aes"
            detectorVisibleToCat = getConfig(aliceDetector.getId(), catClient);
            assertEquals(aliceDetector.getId(), detectorVisibleToCat.getId());

            // Now revoke backend_roles "aes" — cat should lose access
            recs = new HashMap<>();
            backendRoles = new HashSet<>();
            backendRoles.add("aes");
            users = new HashSet<>();
            users.add(catUser);
            recs.put(Recipient.BACKEND_ROLES, backendRoles);
            recs.put(Recipient.USERS, users);
            recipients = new Recipients(recs);
            builder = new TestHelpers.PatchSharingInfoPayloadBuilder();
            builder.configId(aliceDetector.getId()).configType(ADCommonName.AD_RESOURCE_TYPE).revoke(recipients, READ_ONLY_AG);

            String revokeAes = builder.build();

            Response elkRevokesAes = patchSharingInfo(elkClient, Map.of(), revokeAes);
            assertEquals(200, elkRevokesAes.getStatusLine().getStatusCode());
            waitForRevokeNonVisibility(aliceDetector.getId(), catClient);

            // Cat loses access now
            ex = expectThrows(Exception.class, () -> { getConfig(aliceDetector.getId(), catClient); });
            assertTrue(ex.getMessage().contains("no permissions"));

        } else {
            enableFilterBy();
            // User Cat has AD full access, but is part of different backend role so Cat should not be able to access
            // Alice detector
            Exception exception = expectThrows(Exception.class, () -> { getConfig(aliceDetector.getId(), catClient); });
            assertTrue(exception.getMessage().contains("User does not have permissions to access config: " + aliceDetector.getId()));

            disableFilterBy();
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            // User Cat has AD full access, is part of different backend role but filter by is disabled, Cat can access Alice's detector
            AnomalyDetector detector = getConfig(aliceDetector.getId(), catClient);
            assertEquals(detector.getId(), aliceDetector.getId());

            enableFilterBy();
            confirmingClientIsAdmin();

            detector = getConfig(aliceDetector.getId(), client());
            Assert
                .assertArrayEquals(
                    "User backend role of detector doesn't change",
                    new String[] { "odfe" },
                    detector.getUser().getBackendRoles().toArray(new String[0])
                );

        }

    }

    public void testSearchDetector() throws IOException {
        // User Alice has AD full access, should be able to create a detector
        AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);

        String searchADRequestBody = """
            {
              "query": {
                "bool": {
                  "must": [
                    { "match_all": {} }
                  ]
                }
              }
            }
            """;
        // if resource sharing feature is enabled cat or admin client will not have access to aliceDetector regardless of backend role until
        // it is shared, alice client can search its own detector
        if (isResourceSharingFeatureEnabled()) {
            Response response = searchAnomalyDetectors(searchADRequestBody, aliceClient);
            SearchResponse searchResponse = SearchResponse
                .fromXContent(createParser(JsonXContent.jsonXContent, response.getEntity().getContent()));
            long total = searchResponse.getHits().getTotalHits().value();
            assertTrue("got: " + total, total == 1);

            response = searchAnomalyDetectors(searchADRequestBody, catClient);
            searchResponse = SearchResponse.fromXContent(createParser(JsonXContent.jsonXContent, response.getEntity().getContent()));
            total = searchResponse.getHits().getTotalHits().value();
            assertTrue("got: " + total, total == 0);

            response = searchAnomalyDetectors(searchADRequestBody, client());
            searchResponse = SearchResponse.fromXContent(createParser(JsonXContent.jsonXContent, response.getEntity().getContent()));
            total = searchResponse.getHits().getTotalHits().value();
            assertTrue("got: " + total, total == 0);
            // sharing
            // Owner shares read-only with cat
            Response shareROWithCat = shareConfig(
                aliceClient,
                Map.of(),
                shareWithUserPayload(aliceDetector.getId(), ADCommonName.AD_RESOURCE_TYPE, READ_ONLY_AG, catUser)
            );
            assertEquals(200, shareROWithCat.getStatusLine().getStatusCode());
            waitForSharingVisibility(aliceDetector.getId(), catClient);

            // Cat can now find 1 detector
            response = searchAnomalyDetectors(searchADRequestBody, catClient);
            searchResponse = SearchResponse.fromXContent(createParser(JsonXContent.jsonXContent, response.getEntity().getContent()));
            total = searchResponse.getHits().getTotalHits().value();
            assertEquals("cat should see shared detector(s)", 1, total);

            // Admin is still not shared with — should see 0
            response = searchAnomalyDetectors(searchADRequestBody, client());
            searchResponse = SearchResponse.fromXContent(createParser(JsonXContent.jsonXContent, response.getEntity().getContent()));
            total = searchResponse.getHits().getTotalHits().value();
            assertEquals("admin should not see unshared detectors", 0, total);

            // Now grant full_access to elk and let elk share with backend_roles "aes" via PATCH
            String detId = aliceDetector.getId();
            Response grantFullToElk = shareConfig(
                aliceClient,
                Map.of(),
                shareWithUserPayload(detId, ADCommonName.AD_RESOURCE_TYPE, FULL_ACCESS_AG, elkUser)
            );
            assertEquals(200, grantFullToElk.getStatusLine().getStatusCode());
            waitForSharingVisibility(detId, elkClient);

            Map<Recipient, Set<String>> recs = new HashMap<>();
            Set<String> backendRoles = new HashSet<>();
            backendRoles.add("aes");
            recs.put(Recipient.BACKEND_ROLES, backendRoles);
            Recipients recipients = new Recipients(recs);
            TestHelpers.PatchSharingInfoPayloadBuilder builder = new TestHelpers.PatchSharingInfoPayloadBuilder();
            builder.configId(detId).configType(ADCommonName.AD_RESOURCE_TYPE).share(recipients, READ_ONLY_AG);

            String patchShareAes = builder.build();
            Response elkAddsAes = patchSharingInfo(elkClient, Map.of(), patchShareAes);
            assertEquals(200, elkAddsAes.getStatusLine().getStatusCode());
            waitForSharingVisibility(detId, elkClient);

            // Cat (backend_roles aes) can still find it
            response = searchAnomalyDetectors(searchADRequestBody, catClient);
            searchResponse = SearchResponse.fromXContent(createParser(JsonXContent.jsonXContent, response.getEntity().getContent()));
            total = searchResponse.getHits().getTotalHits().value();
            assertEquals(1, total);

            // Revoke aes — cat loses visibility
            recs = new HashMap<>();
            backendRoles = new HashSet<>();
            backendRoles.add("aes");
            Set<String> users = new HashSet<>();
            users.add(catUser);
            recs.put(Recipient.BACKEND_ROLES, backendRoles);
            recs.put(Recipient.USERS, users);
            recipients = new Recipients(recs);
            builder = new TestHelpers.PatchSharingInfoPayloadBuilder();
            builder.configId(detId).configType(ADCommonName.AD_RESOURCE_TYPE).revoke(recipients, READ_ONLY_AG);

            String revokeAes = builder.build();
            Response elkRevokesAes = patchSharingInfo(elkClient, Map.of(), revokeAes);
            assertEquals(200, elkRevokesAes.getStatusLine().getStatusCode());
            waitForRevokeNonVisibility(detId, catClient);

            response = searchAnomalyDetectors(searchADRequestBody, catClient);
            searchResponse = SearchResponse.fromXContent(createParser(JsonXContent.jsonXContent, response.getEntity().getContent()));
            total = searchResponse.getHits().getTotalHits().value();
            assertEquals(0, total);

        } else {
            enableFilterBy();
            // User Cat has AD full access, but is part of different backend role so Cat should not be able to access
            // Alice detector
            Response response = searchAnomalyDetectors(searchADRequestBody, catClient);
            SearchResponse searchResponse = SearchResponse
                .fromXContent(createParser(JsonXContent.jsonXContent, response.getEntity().getContent()));
            long total = searchResponse.getHits().getTotalHits().value();
            assertTrue("got: " + total, total == 0);

            disableFilterBy();
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            // User Cat has AD full access, is part of different backend role but filter by is disabled, Cat can access Alice's detector
            response = searchAnomalyDetectors(searchADRequestBody, catClient);
            searchResponse = SearchResponse.fromXContent(createParser(JsonXContent.jsonXContent, response.getEntity().getContent()));
            total = searchResponse.getHits().getTotalHits().value();
            assertTrue("got: " + total, total == 1);

            enableFilterBy();
            confirmingClientIsAdmin();

            response = searchAnomalyDetectors(searchADRequestBody, client());
            searchResponse = SearchResponse.fromXContent(createParser(JsonXContent.jsonXContent, response.getEntity().getContent()));
            total = searchResponse.getHits().getTotalHits().value();
            assertTrue("got: " + total, total == 1);

        }

    }

    public void testUpdateDetector() throws IOException {
        // User Alice has AD full access, should be able to create a detector and has backend role "odfe"
        AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);

        AnomalyDetector newDetector = new AnomalyDetector(
            aliceDetector.getId(),
            aliceDetector.getVersion(),
            aliceDetector.getName(),
            randomAlphaOfLength(10),
            aliceDetector.getTimeField(),
            aliceDetector.getIndices(),
            aliceDetector.getFeatureAttributes(),
            aliceDetector.getFilterQuery(),
            aliceDetector.getInterval(),
            aliceDetector.getWindowDelay(),
            aliceDetector.getShingleSize(),
            aliceDetector.getUiMetadata(),
            aliceDetector.getSchemaVersion(),
            Instant.now(),
            aliceDetector.getCategoryFields(),
            new User(
                randomAlphaOfLength(5),
                ImmutableList.of("odfe", randomAlphaOfLength(5)),
                ImmutableList.of(randomAlphaOfLength(5)),
                ImmutableList.of("attrKey=attrVal")
            ),
            null,
            aliceDetector.getImputationOption(),
            randomIntBetween(2, 10000),
            randomInt(TimeSeriesSettings.MAX_SHINGLE_SIZE / 2),
            randomIntBetween(1, 1000),
            null,
            null,
            null,
            null,
            null,
            Instant.now(),
            aliceDetector.getFrequency()
        );

        if (isResourceSharingFeatureEnabled()) {
            String noWritePermsMessage = "no permissions for [cluster:admin/opendistro/ad/detector/write]";
            // User admin has all access however, since resource is owned by alice and is not shared with admin, admin will not be able to
            // update the resource
            Exception exception = expectThrows(
                Exception.class,
                () -> { updateAnomalyDetector(aliceDetector.getId(), newDetector, client()); }
            );
            assertTrue(exception.getMessage().contains(noWritePermsMessage));
            // neither should admin be able to get the resource
            exception = expectThrows(Exception.class, () -> { getConfig(aliceDetector.getId(), client()); });
            assertTrue(exception.getMessage().contains("no permissions for [cluster:admin/opendistro/ad/detectors/get]"));
            // nor any other client should be able to access alice's detector
            exception = expectThrows(Exception.class, () -> { updateAnomalyDetector(aliceDetector.getId(), newDetector, fishClient); });
            assertTrue(exception.getMessage().contains(noWritePermsMessage));
            exception = expectThrows(Exception.class, () -> { updateAnomalyDetector(aliceDetector.getId(), newDetector, catClient); });
            assertTrue(exception.getMessage().contains(noWritePermsMessage));

            // Sharing

            // Grant read-only to cat — cat still cannot update
            Response shareROWithCat = shareConfig(
                aliceClient,
                Map.of(),
                shareWithUserPayload(aliceDetector.getId(), ADCommonName.AD_RESOURCE_TYPE, READ_ONLY_AG, catUser)
            );
            assertEquals(200, shareROWithCat.getStatusLine().getStatusCode());
            waitForSharingVisibility(aliceDetector.getId(), catClient);

            Exception ex = expectThrows(Exception.class, () -> { updateAnomalyDetector(aliceDetector.getId(), newDetector, catClient); });
            assertTrue(ex.getMessage(), ex.getMessage().contains("no permissions"));

            // Grant full_access to elk — elk can update
            Response grantFullToElk = shareConfig(
                aliceClient,
                Map.of(),
                shareWithUserPayload(aliceDetector.getId(), ADCommonName.AD_RESOURCE_TYPE, FULL_ACCESS_AG, elkUser)
            );
            assertEquals(200, grantFullToElk.getStatusLine().getStatusCode());
            waitForSharingVisibility(aliceDetector.getId(), elkClient);

            Response elkUpdate = updateAnomalyDetector(aliceDetector.getId(), newDetector, elkClient);
            assertEquals(200, elkUpdate.getStatusLine().getStatusCode());

            // Non-owner without full_access cannot share/revoke: cat tries to PATCH -> forbidden
            Map<Recipient, Set<String>> recs = new HashMap<>();
            Set<String> users = new HashSet<>();
            users.add(bobUser);
            recs.put(Recipient.USERS, users);
            Recipients recipients = new Recipients(recs);
            TestHelpers.PatchSharingInfoPayloadBuilder builder = new TestHelpers.PatchSharingInfoPayloadBuilder();
            builder.configId(aliceDetector.getId()).configType(ADCommonName.AD_RESOURCE_TYPE).share(recipients, READ_ONLY_AG);

            String catPatchAttempt = builder.build();

            ex = expectThrows(Exception.class, () -> { patchSharingInfo(catClient, Map.of(), catPatchAttempt); });
            assertTrue(ex.getMessage(), ex.getMessage().contains("no permissions") || ex.getMessage().contains("403"));

            // Owner revokes elk's full_access — elk loses update ability
            recs = new HashMap<>();
            users = new HashSet<>();
            users.add(elkUser);
            recs.put(Recipient.USERS, users);
            recipients = new Recipients(recs);
            builder = new TestHelpers.PatchSharingInfoPayloadBuilder();
            builder.configId(aliceDetector.getId()).configType(ADCommonName.AD_RESOURCE_TYPE).revoke(recipients, FULL_ACCESS_AG);

            String revokeElkFull = builder.build();
            Response ownerRevokes = patchSharingInfo(aliceClient, Map.of(), revokeElkFull);
            assertEquals(200, ownerRevokes.getStatusLine().getStatusCode());
            waitForRevokeNonVisibility(aliceDetector.getId(), elkClient);

            ex = expectThrows(Exception.class, () -> { updateAnomalyDetector(aliceDetector.getId(), newDetector, elkClient); });
            assertTrue(ex.getMessage().contains("no permissions"));

        } else {
            enableFilterBy();
            // User client has admin all access, and has "opensearch" backend role so client should be able to update detector
            // But the detector's backend role should not be replaced as client's backend roles (all_access).
            Response response = updateAnomalyDetector(aliceDetector.getId(), newDetector, client());
            Assert.assertEquals(200, response.getStatusLine().getStatusCode());
            AnomalyDetector anomalyDetector = getConfig(aliceDetector.getId(), client());
            Assert
                .assertArrayEquals(
                    "odfe is still the backendrole, not opensearch",
                    new String[] { "odfe" },
                    anomalyDetector.getUser().getBackendRoles().toArray(new String[0])
                );
            // User Fish has AD full access, and has "odfe" backend role which is one of Alice's backend role, so
            // Fish should be able to update detectors created by Alice. But the detector's backend role should
            // not be replaced as Fish's backend roles.
            response = updateAnomalyDetector(aliceDetector.getId(), newDetector, fishClient);
            assertEquals(200, response.getStatusLine().getStatusCode());
            anomalyDetector = getConfig(aliceDetector.getId(), fishClient);
            Assert
                .assertArrayEquals(
                    "Wrong user roles",
                    new String[] { "odfe" },
                    anomalyDetector.getUser().getBackendRoles().toArray(new String[0])
                );

            // User Cat has AD full access, but is part of different backend role so Cat should not be able to access
            // Alice detector
            Exception exception = expectThrows(
                IOException.class,
                () -> { updateAnomalyDetector(aliceDetector.getId(), newDetector, catClient); }
            );
            assertTrue(exception.getMessage().contains("User does not have permissions to access config: " + aliceDetector.getId()));

            disableFilterBy();
            // User Cat has AD full access, is part of different backend role but filter by is disabled, Cat can access Alice's detector
            // But the detector's backend role should not be replaced as Cat's backend roles (all_access).
            response = updateAnomalyDetector(aliceDetector.getId(), newDetector, catClient);
            assertEquals(200, response.getStatusLine().getStatusCode());
            anomalyDetector = getConfig(aliceDetector.getId(), catClient);
            Assert
                .assertArrayEquals(
                    "Wrong user roles",
                    new String[] { "odfe" },
                    anomalyDetector.getUser().getBackendRoles().toArray(new String[0])
                );
        }

    }

    public void testStartAndStopDetector() throws IOException {
        // User Alice has AD full access, should be able to create a detector
        AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);

        // User Cat has AD full access, but is part of different backend role so Cat should not be able to access
        // Alice detector
        final Instant now = Instant.now();

        if (isResourceSharingFeatureEnabled()) {
            String noPermsMessage = "no permissions for [cluster:admin/opendistro/ad/detector/jobmanagement]";
            // since resource sharing is enabled and detector is not shared no one other than alice should be able to start or stop the
            // detector
            Exception exception = expectThrows(IOException.class, () -> {
                startAnomalyDetector(aliceDetector.getId(), new DateRange(now.minus(10, ChronoUnit.DAYS), now), catClient);
            });
            Assert.assertTrue(exception.getMessage().contains(noPermsMessage));

            exception = expectThrows(IOException.class, () -> { startAnomalyDetector(aliceDetector.getId(), null, elkClient); });
            Assert.assertTrue(exception.getMessage().contains(noPermsMessage));

            exception = expectThrows(IOException.class, () -> { stopAnomalyDetector(aliceDetector.getId(), elkClient, true); });
            Assert.assertTrue(exception.getMessage().contains(noPermsMessage));

            exception = expectThrows(IOException.class, () -> { stopAnomalyDetector(aliceDetector.getId(), catClient, true); });
            Assert.assertTrue(exception.getMessage().contains(noPermsMessage));

            exception = expectThrows(IOException.class, () -> {
                startAnomalyDetector(aliceDetector.getId(), new DateRange(now.minus(10, ChronoUnit.DAYS), now), elkClient);
            });
            Assert.assertTrue(exception.getMessage().contains(noPermsMessage));

            // Sharing
            // Read-only to cat: cannot start/stop
            Response shareROWithCat = shareConfig(
                aliceClient,
                Map.of(),
                shareWithUserPayload(aliceDetector.getId(), ADCommonName.AD_RESOURCE_TYPE, READ_ONLY_AG, catUser)
            );
            assertEquals(200, shareROWithCat.getStatusLine().getStatusCode());
            waitForSharingVisibility(aliceDetector.getId(), catClient);

            Exception ex = expectThrows(IOException.class, () -> {
                startAnomalyDetector(aliceDetector.getId(), new DateRange(now.minus(10, ChronoUnit.DAYS), now), catClient);
            });
            assertTrue(ex.getMessage().contains("no permissions"));

            // Full access to elk: can start/stop
            Response grantFullToElk = shareConfig(
                aliceClient,
                Map.of(),
                shareWithUserPayload(aliceDetector.getId(), ADCommonName.AD_RESOURCE_TYPE, FULL_ACCESS_AG, elkUser)
            );
            assertEquals(200, grantFullToElk.getStatusLine().getStatusCode());
            waitForSharingVisibility(aliceDetector.getId(), elkClient);

            Response resp = startAnomalyDetector(aliceDetector.getId(), new DateRange(now.minus(10, ChronoUnit.DAYS), now), elkClient);
            MatcherAssert.assertThat(resp.getStatusLine().toString(), CoreMatchers.containsString("200 OK"));

            resp = stopAnomalyDetector(aliceDetector.getId(), elkClient, true);
            assertEquals(200, resp.getStatusLine().getStatusCode());

            // Owner revokes elk full access — elk loses ability to start/stop
            Map<Recipient, Set<String>> recs = new HashMap<>();
            Set<String> users = new HashSet<>();
            users.add(elkUser);
            recs.put(Recipient.USERS, users);
            Recipients recipients = new Recipients(recs);
            TestHelpers.PatchSharingInfoPayloadBuilder builder = new TestHelpers.PatchSharingInfoPayloadBuilder();
            builder.configId(aliceDetector.getId()).configType(ADCommonName.AD_RESOURCE_TYPE).revoke(recipients, FULL_ACCESS_AG);
            String revokeElkFull = builder.build();
            Response revokeResp = patchSharingInfo(aliceClient, Map.of(), revokeElkFull);
            assertEquals(200, revokeResp.getStatusLine().getStatusCode());
            waitForRevokeNonVisibility(aliceDetector.getId(), elkClient);

            ex = expectThrows(IOException.class, () -> {
                startAnomalyDetector(aliceDetector.getId(), new DateRange(now.minus(10, ChronoUnit.DAYS), now), elkClient);
            });
            assertTrue(ex.getMessage().contains("no permissions"));

        } else {
            enableFilterBy();
            Exception exception = expectThrows(IOException.class, () -> {
                startAnomalyDetector(aliceDetector.getId(), new DateRange(now.minus(10, ChronoUnit.DAYS), now), catClient);
            });
            Assert.assertTrue(exception.getMessage().contains("User does not have permissions to access config: " + aliceDetector.getId()));

            // User Bob has AD read access, should not be able to modify a detector
            Assert.assertNotNull(aliceDetector.getId());
            exception = expectThrows(IOException.class, () -> { startAnomalyDetector(aliceDetector.getId(), null, bobClient); });
            Assert.assertTrue(exception.getMessage().contains("no permissions for [cluster:admin/opendistro/ad/detector/jobmanagement]"));

            // User elk shares backend_role with alice and has AD full access, it should be able to modify alice's detector
            Response response = startAnomalyDetector(aliceDetector.getId(), new DateRange(now.minus(10, ChronoUnit.DAYS), now), elkClient);
            MatcherAssert.assertThat(response.getStatusLine().toString(), CoreMatchers.containsString("200 OK"));

            // User elk should also be able to stop detector
            response = stopAnomalyDetector(aliceDetector.getId(), elkClient, true);
            assertEquals(200, response.getStatusLine().getStatusCode());

            // User Cat has AD full access, but is part of different backend role so Cat should not be able to stop
            // Alice detector
            exception = expectThrows(IOException.class, () -> { stopAnomalyDetector(aliceDetector.getId(), catClient, true); });
            Assert.assertTrue(exception.getMessage().contains("User does not have permissions to access config: " + aliceDetector.getId()));

            disableFilterBy();
            // User Cat has AD full access, but is part of different backend role so Cat should not be able to access
            // Alice detector
            response = startAnomalyDetector(aliceDetector.getId(), new DateRange(now.minus(10, ChronoUnit.DAYS), now), catClient);
            assertEquals(200, response.getStatusLine().getStatusCode());
            // User Cat should also be able to stop Alice's detector
            response = stopAnomalyDetector(aliceDetector.getId(), catClient, true);
            assertEquals(200, response.getStatusLine().getStatusCode());
        }
    }

    public void testPreviewAnomalyDetector() throws IOException {
        // User Alice has AD full access, should be able to create/preview a detector
        AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);
        AnomalyDetectorExecutionInput input = new AnomalyDetectorExecutionInput(
            aliceDetector.getId(),
            Instant.now().minusSeconds(60 * 10),
            Instant.now(),
            null
        );

        Response response = previewAnomalyDetector(aliceDetector.getId(), aliceClient, input);
        Assert.assertEquals(RestStatus.OK, TestHelpers.restStatus(response));

        String noPermsMessage = "no permissions for [cluster:admin/opendistro/ad/detector/preview]";
        // if resource sharing feature is enabled, any client other than alice will not have access to aliceDetector regardless of backend
        // role since detector is not shared
        if (isResourceSharingFeatureEnabled()) {
            Exception exception = expectThrows(Exception.class, () -> { previewAnomalyDetector(aliceDetector.getId(), catClient, input); });
            assertTrue(exception.getMessage().contains(noPermsMessage));

            exception = expectThrows(IOException.class, () -> { previewAnomalyDetector(aliceDetector.getId(), bobClient, input); });
            Assert.assertTrue(exception.getMessage().contains(noPermsMessage));

            exception = expectThrows(IOException.class, () -> { previewAnomalyDetector(aliceDetector.getId(), oceanClient, input); });
            Assert.assertTrue(exception.getMessage().contains(noPermsMessage));

            // Sharing
            // Share read-only with cat => preview should be allowed
            Response shareROWithCat = shareConfig(
                aliceClient,
                Map.of(),
                shareWithUserPayload(aliceDetector.getId(), ADCommonName.AD_RESOURCE_TYPE, READ_ONLY_AG, catUser)
            );
            assertEquals(200, shareROWithCat.getStatusLine().getStatusCode());
            waitForSharingVisibility(aliceDetector.getId(), catClient);

            Response ok = previewAnomalyDetector(aliceDetector.getId(), catClient, input);
            assertEquals(200, ok.getStatusLine().getStatusCode());

            // Non-owner with read-only cannot escalate sharing
            Exception ex = expectThrows(Exception.class, () -> {
                shareConfig(
                    catClient,
                    Map.of(),
                    shareWithUserPayload(aliceDetector.getId(), ADCommonName.AD_RESOURCE_TYPE, READ_ONLY_AG, bobUser)
                );
            });
            assertTrue(ex.getMessage().contains("no permissions") || ex.getMessage().contains("403"));

            // Grant full_access to elk; elk can patch revoke cat's user share
            Response grantFullToElk = shareConfig(
                aliceClient,
                Map.of(),
                shareWithUserPayload(aliceDetector.getId(), ADCommonName.AD_RESOURCE_TYPE, FULL_ACCESS_AG, elkUser)
            );
            assertEquals(200, grantFullToElk.getStatusLine().getStatusCode());
            waitForSharingVisibility(aliceDetector.getId(), elkClient);

            Map<Recipient, Set<String>> recs = new HashMap<>();
            Set<String> users = new HashSet<>();
            users.add(catUser);
            recs.put(Recipient.USERS, users);
            Recipients recipients = new Recipients(recs);
            TestHelpers.PatchSharingInfoPayloadBuilder builder = new TestHelpers.PatchSharingInfoPayloadBuilder();
            builder.configId(aliceDetector.getId()).configType(ADCommonName.AD_RESOURCE_TYPE).revoke(recipients, READ_ONLY_AG);
            String revokeCatUser = builder.build();

            Response revoked = patchSharingInfo(elkClient, Map.of(), revokeCatUser);
            assertEquals(200, revoked.getStatusLine().getStatusCode());
            waitForRevokeNonVisibility(aliceDetector.getId(), catClient);

            ex = expectThrows(Exception.class, () -> { previewAnomalyDetector(aliceDetector.getId(), catClient, input); });
            assertTrue(ex.getMessage().contains("no permissions"));

        } else {
            enableFilterBy();
            // User Cat has AD full access, but is part of different backend role so Cat should not be able to access
            // Alice detector
            Exception exception = expectThrows(
                IOException.class,
                () -> { previewAnomalyDetector(aliceDetector.getId(), catClient, input); }
            );
            Assert.assertTrue(exception.getMessage().contains("User does not have permissions to access config: " + aliceDetector.getId()));

            // User bob has AD read access, should not be able to preview a detector
            exception = expectThrows(IOException.class, () -> { previewAnomalyDetector(aliceDetector.getId(), bobClient, input); });
            Assert.assertTrue(exception.getMessage().contains(noPermsMessage));

            // User ocean has no read permission of index
            exception = expectThrows(Exception.class, () -> { previewAnomalyDetector(aliceDetector.getId(), oceanClient, input); });
            Assert.assertTrue("actual msg: " + exception.getMessage(), exception.getMessage().contains(noPermsMessage));

            disableFilterBy();
            // User Cat has AD full access, is part of different backend_role, but filter_by is disabled so Cat should be able to preview
            // Alice detector
            response = previewAnomalyDetector(aliceDetector.getId(), catClient, input);
            assertEquals(200, response.getStatusLine().getStatusCode());
        }

    }

    public void testValidateAnomalyDetector() throws IOException {
        // User Alice has AD full access, should be able to validate a detector
        AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);
        Response validateResponse = validateAnomalyDetector(aliceDetector, aliceClient);
        Assert.assertNotNull("User alice validated detector successfully", validateResponse);

        String noValidatePermsMessage = "no permissions for [cluster:admin/opendistro/ad/detector/validate]";

        // User Lion has no AD access at all, should not be able to validate a detector
        Exception exception = expectThrows(IOException.class, () -> { validateAnomalyDetector(aliceDetector, lionClient); });
        Assert.assertTrue(exception.getMessage().contains(noValidatePermsMessage));

        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(null, Instant.now());

        if (isResourceSharingFeatureEnabled()) {
            // User Bob has AD read access, resource sharing is enabled, but resource-sharing is not applied on validate request.
            // Bob should be able to validate detector
            validateResponse = validateAnomalyDetector(detector, bobClient);
            Assert.assertNotNull("User bob validated detector successfully", validateResponse);

            // User ocean has no read permission of index
            exception = expectThrows(Exception.class, () -> { validateAnomalyDetector(detector, oceanClient); });
            Assert
                .assertTrue(
                    "actual: " + exception.getMessage(),
                    exception.getMessage().contains("no permissions for [indices:data/read/search]")
                );

            // User Dog has AD full access, but has no backend role
            validateResponse = validateAnomalyDetector(detector, dogClient);
            Assert.assertNotNull("User dog validated detector successfully", validateResponse);

            Response ok = validateAnomalyDetector(aliceDetector, catClient);
            Assert.assertNotNull(ok);

            // Read-only share should allow validate (read action)
            Response shareROWithCat = shareConfig(
                aliceClient,
                Map.of(),
                shareWithUserPayload(aliceDetector.getId(), ADCommonName.AD_RESOURCE_TYPE, READ_ONLY_AG, catUser)
            );
            assertEquals(200, shareROWithCat.getStatusLine().getStatusCode());
            waitForSharingVisibility(aliceDetector.getId(), catClient);

            // show that resource-sharing doesn't affect validate flow as it is not marked as protected
            // anyone with validate permission will be able to validate the detector
            ok = validateAnomalyDetector(aliceDetector, catClient);
            Assert.assertNotNull(ok);

            // Non-owner (cat, read-only) cannot share/revoke
            Exception ex = expectThrows(Exception.class, () -> {
                shareConfig(
                    catClient,
                    Map.of(),
                    shareWithUserPayload(aliceDetector.getId(), ADCommonName.AD_RESOURCE_TYPE, READ_ONLY_AG, bobUser)
                );
            });
            assertTrue(ex.getMessage().contains("no permissions") || ex.getMessage().contains("403"));

            // Owner grants elk full_access -> elk can revoke cat
            Response grantFullToElk = shareConfig(
                aliceClient,
                Map.of(),
                shareWithUserPayload(aliceDetector.getId(), ADCommonName.AD_RESOURCE_TYPE, FULL_ACCESS_AG, elkUser)
            );
            assertEquals(200, grantFullToElk.getStatusLine().getStatusCode());
            waitForSharingVisibility(aliceDetector.getId(), elkClient);

            Map<Recipient, Set<String>> recs = new HashMap<>();
            Set<String> users = new HashSet<>();
            users.add(catUser);
            recs.put(Recipient.USERS, users);
            Recipients recipients = new Recipients(recs);
            TestHelpers.PatchSharingInfoPayloadBuilder builder = new TestHelpers.PatchSharingInfoPayloadBuilder();
            builder.configId(aliceDetector.getId()).configType(ADCommonName.AD_RESOURCE_TYPE).revoke(recipients, READ_ONLY_AG);
            String revokeCatUser = builder.build();

            Response revoked = patchSharingInfo(elkClient, Map.of(), revokeCatUser);
            assertEquals(200, revoked.getStatusLine().getStatusCode());
            waitForRevokeNonVisibility(aliceDetector.getId(), catClient);

            // show that resource-sharing doesn't affect validate flow as it is not marked as protected
            // anyone with validate permission will be able to validate the detector
            ok = validateAnomalyDetector(aliceDetector, catClient);
            Assert.assertNotNull(ok);

        } else {

            validateResponse = validateAnomalyDetector(detector, bobClient);
            Assert.assertNotNull("User bob validated detector successfully", validateResponse);

            // User ocean has no read permission of index, can't validate detector
            exception = expectThrows(Exception.class, () -> { validateAnomalyDetector(detector, oceanClient); });
            Assert
                .assertTrue(
                    "actual: " + exception.getMessage(),
                    exception.getMessage().contains("no permissions for [indices:data/read/search]")
                );

            enableFilterBy();
            // User Dog has AD full access, but has no backend role
            // When filter by is enabled, we block validating Detectors
            exception = expectThrows(IOException.class, () -> { validateAnomalyDetector(detector, dogClient); });
            Assert
                .assertTrue(
                    exception
                        .getMessage()
                        .contains("Filter by backend roles is enabled and User dog does not have backend roles configured")
                );

            disableFilterBy();
            // User Dog has AD full access, but has no backend role
            // When filter by is disabled, we allow validating Detectors
            validateResponse = validateAnomalyDetector(detector, dogClient);
            Assert.assertNotNull("User dog validated detector successfully", validateResponse);
        }
    }

    public void testDeleteDetector() throws IOException {
        // User Alice has AD full access, should be able to create a detector
        AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);
        // if resource sharing feature is enabled cat or client will not have access to aliceDetector regardless of backend role until
        // shared with
        if (isResourceSharingFeatureEnabled()) {
            Exception exception = expectThrows(Exception.class, () -> { deleteAnomalyDetector(aliceDetector.getId(), catClient); });
            assertTrue(exception.getMessage().contains("no permissions for [cluster:admin/opendistro/ad/detector/delete]"));

            exception = expectThrows(Exception.class, () -> { deleteAnomalyDetector(aliceDetector.getId(), client()); });
            assertTrue(exception.getMessage().contains("no permissions for [cluster:admin/opendistro/ad/detector/delete]"));

            // Owner grants full_access to cat -> cat can delete
            Response grantFullToCat = shareConfig(
                aliceClient,
                Map.of(),
                shareWithUserPayload(aliceDetector.getId(), ADCommonName.AD_RESOURCE_TYPE, FULL_ACCESS_AG, catUser)
            );
            assertEquals(200, grantFullToCat.getStatusLine().getStatusCode());
            waitForSharingVisibility(aliceDetector.getId(), catClient);

            Response del = deleteAnomalyDetector(aliceDetector.getId(), catClient);
            assertEquals(200, del.getStatusLine().getStatusCode());

        } else {
            enableFilterBy();
            // User Cat has AD full access, but is part of different backend role so Cat should not be able to access
            // Alice detector
            Exception exception = expectThrows(IOException.class, () -> { deleteAnomalyDetector(aliceDetector.getId(), catClient); });
            Assert.assertTrue(exception.getMessage().contains("User does not have permissions to access config: " + aliceDetector.getId()));

            disableFilterBy();
            // User Cat has AD full access, should be able to access
            // Alice detector
            Response response = deleteAnomalyDetector(aliceDetector.getId(), catClient);
            assertEquals(200, response.getStatusLine().getStatusCode());
        }

    }

}
