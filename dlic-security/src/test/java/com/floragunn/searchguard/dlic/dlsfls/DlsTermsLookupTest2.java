/*
 * Copyright 2021 by floragunn GmbH - All rights reserved
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * This software is free of charge for non-commercial and academic use.
 * For commercial use in a production environment you have to obtain a license
 * from https://floragunn.com
 *
 */

package com.floragunn.searchguard.dlic.dlsfls;

import static com.floragunn.searchguard.dlic.dlsfls.DlsTermsLookupAsserts.assertAccessCodesMatch;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.MultiSearchResponse.Item;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchguard.test.helper.cluster.TestSgConfig.Role;

public class DlsTermsLookupTest2 {

    @ClassRule 
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();
    
    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled()
            .setInSgConfig("sg_config.dynamic.do_not_fail_on_forbidden", true)
            .roles(new Role("sg_dls_tlq_lookup").clusterPermissions("*").indexPermissions("*").on("tlqdummy").indexPermissions("*").dls(
                    "{ \"terms\": { \"access_codes\": { \"index\": \"user_access_codes\", \"id\": \"${user.name}\", \"path\": \"access_codes\" } } }")
                    .on("tlqdocuments")

            ).user("tlq_1337", "password", "sg_dls_tlq_lookup").user("tlq_42", "password", "sg_dls_tlq_lookup")
            .user("tlq_1337_42", "password", "sg_dls_tlq_lookup").user("tlq_999", "password", "sg_dls_tlq_lookup")
            .user("tlq_empty_access_codes", "password", "sg_dls_tlq_lookup").user("tlq_no_codes", "password", "sg_dls_tlq_lookup")
            .user("tlq_no_entry_in_user_index", "password", "sg_dls_tlq_lookup").build();

    @BeforeClass
    public static void setupTestData() {
        try (Client client = cluster.getInternalNodeClient()) {

            // user access codes, basis for TLQ query
            client.index(new IndexRequest("user_access_codes").id("tlq_1337").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{ \"access_codes\": [1337] }", XContentType.JSON)).actionGet();
            client.index(new IndexRequest("user_access_codes").id("tlq_42").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{ \"access_codes\": [42] }", XContentType.JSON)).actionGet();
            client.index(new IndexRequest("user_access_codes").id("tlq_1337_42").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{ \"access_codes\": [1337, 42] }", XContentType.JSON)).actionGet();
            client.index(new IndexRequest("user_access_codes").id("tlq_999").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{ \"access_codes\": [999] }", XContentType.JSON)).actionGet();
            client.index(new IndexRequest("user_access_codes").id("tlq_empty_access_codes").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{ \"access_codes\": [] }", XContentType.JSON)).actionGet();
            client.index(new IndexRequest("user_access_codes").id("tlq_no_codes").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{ \"bla\": \"blub\" }", XContentType.JSON)).actionGet();

            // need to have keyword for bu field since we're testing aggregations
            client.admin().indices().create(new CreateIndexRequest("tlqdocuments")).actionGet();
            client.admin().indices().putMapping(new PutMappingRequest("tlqdocuments").type("_doc").source("bu", "type=keyword")).actionGet();

            // tlqdocuments, protected by TLQ
            client.index(new IndexRequest("tlqdocuments").id("1").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{ \"bu\": \"AAA\", \"access_codes\": [1337] }", XContentType.JSON)).actionGet();
            client.index(new IndexRequest("tlqdocuments").id("2").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{ \"bu\": \"AAA\", \"access_codes\": [42] }", XContentType.JSON)).actionGet();
            client.index(new IndexRequest("tlqdocuments").id("3").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{ \"bu\": \"AAA\", \"access_codes\": [1337, 42] }", XContentType.JSON)).actionGet();
            client.index(new IndexRequest("tlqdocuments").id("4").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{ \"bu\": \"BBB\", \"access_codes\": [1337] }", XContentType.JSON)).actionGet();
            client.index(new IndexRequest("tlqdocuments").id("5").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{ \"bu\": \"BBB\", \"access_codes\": [42] }", XContentType.JSON)).actionGet();
            client.index(new IndexRequest("tlqdocuments").id("6").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{ \"bu\": \"BBB\", \"access_codes\": [1337, 42] }", XContentType.JSON)).actionGet();
            client.index(new IndexRequest("tlqdocuments").id("7").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{ \"bu\": \"CCC\", \"access_codes\": [1337] }", XContentType.JSON)).actionGet();
            client.index(new IndexRequest("tlqdocuments").id("8").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{ \"bu\": \"CCC\", \"access_codes\": [42] }", XContentType.JSON)).actionGet();
            client.index(new IndexRequest("tlqdocuments").id("9").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{ \"bu\": \"CCC\", \"access_codes\": [1337, 42] }", XContentType.JSON)).actionGet();
            client.index(new IndexRequest("tlqdocuments").id("10").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{ \"bu\": \"DDD\", \"access_codes\": [1337] }", XContentType.JSON)).actionGet();
            client.index(new IndexRequest("tlqdocuments").id("11").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{ \"bu\": \"DDD\", \"access_codes\": [42] }", XContentType.JSON)).actionGet();
            client.index(new IndexRequest("tlqdocuments").id("12").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{ \"bu\": \"DDD\", \"access_codes\": [1337, 42] }", XContentType.JSON)).actionGet();
            client.index(new IndexRequest("tlqdocuments").id("13").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{ \"bu\": \"EEE\", \"access_codes\": [1337] }", XContentType.JSON)).actionGet();
            client.index(new IndexRequest("tlqdocuments").id("14").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{ \"bu\": \"EEE\", \"access_codes\": [42] }", XContentType.JSON)).actionGet();
            client.index(new IndexRequest("tlqdocuments").id("15").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{ \"bu\": \"EEE\", \"access_codes\": [1337, 42] }", XContentType.JSON)).actionGet();
            client.index(new IndexRequest("tlqdocuments").id("16").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{ \"bu\": \"FFF\" }",
                    XContentType.JSON)).actionGet();
            client.index(new IndexRequest("tlqdocuments").id("17").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{ \"bu\": \"FFF\", \"access_codes\": [12345] }", XContentType.JSON)).actionGet();
            client.index(new IndexRequest("tlqdocuments").id("18").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{ \"bu\": \"FFF\", \"access_codes\": [12345, 6789] }", XContentType.JSON)).actionGet();

            // we use a "bu" field here as well to test aggregations over multiple indices (TBD)
            client.admin().indices().create(new CreateIndexRequest("tlqdummy")).actionGet();
            client.admin().indices().putMapping(new PutMappingRequest("tlqdummy").type("_doc").source("bu", "type=keyword")).actionGet();

            // tlqdummy, not protected by TLQ
            client.index(new IndexRequest("tlqdummy").id("101").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{ \"mykey\": \"101\", \"bu\": \"GGG\" }", XContentType.JSON)).actionGet();
            client.index(new IndexRequest("tlqdummy").id("102").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{ \"mykey\": \"102\", \"bu\": \"GGG\" }", XContentType.JSON)).actionGet();
            client.index(new IndexRequest("tlqdummy").id("103").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{ \"mykey\": \"103\", \"bu\": \"GGG\" }", XContentType.JSON)).actionGet();
            client.index(new IndexRequest("tlqdummy").id("104").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{ \"mykey\": \"104\", \"bu\": \"GGG\" }", XContentType.JSON)).actionGet();
            client.index(new IndexRequest("tlqdummy").id("105").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{ \"mykey\": \"105\", \"bu\": \"GGG\" }", XContentType.JSON)).actionGet();

        }
    }

    // ------------------------
    // Test search and msearch
    // ------------------------

    @Test
    public void testSimpleSearch_AccessCode_1337() throws Exception {
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("tlq_1337", "password")) {
            SearchResponse searchResponse = client.search(new SearchRequest("tlqdocuments"), RequestOptions.DEFAULT);
            // 10 docs, all need to have access code 1337    
            Assert.assertEquals(searchResponse.toString(), 10, searchResponse.getHits().getTotalHits().value);
            // fields need to have 1337 access code
            assertAccessCodesMatch(searchResponse.getHits().getHits(), new Integer[] { 1337 });
        }
    }

    @Test
    public void testSimpleSearch_AccessCode_42() throws Exception {
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("tlq_42", "password")) {
            SearchResponse searchResponse = client.search(new SearchRequest("tlqdocuments"), RequestOptions.DEFAULT);
            // 10 docs, all need to have access code 42    
            Assert.assertEquals(searchResponse.toString(), 10, searchResponse.getHits().getTotalHits().value);
            // fields need to have 42 access code
            assertAccessCodesMatch(searchResponse.getHits().getHits(), new Integer[] { 42 });
        }
    }

    @Test
    public void testSimpleSearch_AccessCodes_1337_42() throws Exception {
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("tlq_1337_42", "password")) {
            SearchResponse searchResponse = client.search(new SearchRequest("tlqdocuments"), RequestOptions.DEFAULT);
            // 15 docs, all need to have either access code 1337 or 42    
            Assert.assertEquals(searchResponse.toString(), 15, searchResponse.getHits().getTotalHits().value);
            // fields need to have 42 or 1337 access code
            assertAccessCodesMatch(searchResponse.getHits().getHits(), new Integer[] { 42, 1337 });
        }
    }

    @Test
    public void testSimpleSearch_AccessCodes_999() throws Exception {
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("tlq_999", "password")) {
            SearchResponse searchResponse = client.search(new SearchRequest("tlqdocuments"), RequestOptions.DEFAULT);
            // no docs match, expect empty result    
            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.getHits().getTotalHits().value);
        }
    }

    @Test
    public void testSimpleSearch_AccessCodes_emptyAccessCodes() throws Exception {
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("tlq_empty_access_codes", "password")) {
            SearchResponse searchResponse = client.search(new SearchRequest("tlqdocuments"), RequestOptions.DEFAULT);
            // user has an empty array for access codes, expect no error and empty search result
            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.getHits().getTotalHits().value);
        }
    }

    @Test
    public void testSimpleSearch_AccessCodes_noAccessCodes() throws Exception {
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("tlq_no_codes", "password")) {
            SearchResponse searchResponse = client.search(new SearchRequest("tlqdocuments"), RequestOptions.DEFAULT);
            // user has no access code , expect no error and empty search result
            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.getHits().getTotalHits().value);
        }
    }

    @Test
    public void testSimpleSearch_AccessCodes_noEntryInUserIndex() throws Exception {
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("tlq_no_codes", "password")) {
            SearchResponse searchResponse = client.search(new SearchRequest("tlqdocuments"), RequestOptions.DEFAULT);
            // user has no entry in user index, expect no error and empty search result
            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.getHits().getTotalHits().value);
        }
    }

    @Test
    public void testSimpleSearch_AllIndices_All_AccessCodes_1337() throws Exception {
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("tlq_1337", "password")) {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().from(0).size(100);
            SearchRequest request = new SearchRequest("_all").source(searchSourceBuilder);
            SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);

            // assume hits from 2 indices:
            // - tlqdocuments, must contain only docs with access code 1337
            // - tlqdummy, contains all documents
            // no access to user_access_codes must be granted 

            // check all 5 tlqdummy entries present, index is not protected by DLS
            Set<SearchHit> tlqdummyHits = Arrays.asList(searchResponse.getHits().getHits()).stream().filter((h) -> h.getIndex().equals("tlqdummy"))
                    .collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 5, tlqdummyHits.size());

            // check 10 hits with code 1337 from tlqdocuments index. All other documents must be filtered
            Set<SearchHit> tlqdocumentHits = Arrays.asList(searchResponse.getHits().getHits()).stream()
                    .filter((h) -> h.getIndex().equals("tlqdocuments")).collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 10, tlqdocumentHits.size());
            assertAccessCodesMatch(tlqdocumentHits, new Integer[] { 1337 });

            // check no access to user_access_codes index
            Set<SearchHit> userAccessCodesHits = Arrays.asList(searchResponse.getHits().getHits()).stream()
                    .filter((h) -> h.getIndex().equals("user_access_codes")).collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 0, userAccessCodesHits.size());

        }
    }

    @Test
    public void testSimpleSearch_AllIndicesWildcard_AccessCodes_1337() throws Exception {
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("tlq_1337", "password")) {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().from(0).size(100);
            SearchRequest request = new SearchRequest("*").source(searchSourceBuilder);
            SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);

            // assume hits from 2 indices:
            // - tlqdocuments, must contain only docs with access code 1337
            // - tlqdummy, contains all documents
            // no access to user_access_codes must be granted 

            // check all 5 tlqdummy entries present, index is not protected by DLS
            Set<SearchHit> tlqdummyHits = Arrays.asList(searchResponse.getHits().getHits()).stream().filter((h) -> h.getIndex().equals("tlqdummy"))
                    .collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 5, tlqdummyHits.size());

            // check 10 hits with code 1337 from tlqdocuments index. All other documents must be filtered
            Set<SearchHit> tlqdocumentHits = Arrays.asList(searchResponse.getHits().getHits()).stream()
                    .filter((h) -> h.getIndex().equals("tlqdocuments")).collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 10, tlqdocumentHits.size());
            assertAccessCodesMatch(tlqdocumentHits, new Integer[] { 1337 });

            // check no access to user_access_codes index
            Set<SearchHit> userAccessCodesHits = Arrays.asList(searchResponse.getHits().getHits()).stream()
                    .filter((h) -> h.getIndex().equals("user_access_codes")).collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 0, userAccessCodesHits.size());

        }
    }

    @Test
    public void testSimpleSearch_ThreeIndicesWildcard_AccessCodes_1337() throws Exception {
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("tlq_1337", "password")) {
            SearchRequest request = new SearchRequest("tlq*, user*");
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().from(0).size(100);
            request.source(searchSourceBuilder);
            SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);

            // assume hits from 2 indices:
            // - tlqdocuments, must contain only docs with access code 1337
            // - tlqdummy, contains all documents
            // no access to user_access_codes must be granted 

            // check all 5 tlqdummy entries present, index is not protected by DLS
            Set<SearchHit> tlqdummyHits = Arrays.asList(searchResponse.getHits().getHits()).stream().filter((h) -> h.getIndex().equals("tlqdummy"))
                    .collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 5, tlqdummyHits.size());

            // check 10 hits with code 1337 from tlqdocuments index. All other documents must be filtered            
            Set<SearchHit> tlqdocumentHits = Arrays.asList(searchResponse.getHits().getHits()).stream()
                    .filter((h) -> h.getIndex().equals("tlqdocuments")).collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 10, tlqdocumentHits.size());
            assertAccessCodesMatch(tlqdocumentHits, new Integer[] { 1337 });

            // check no access to user_access_codes index
            Set<SearchHit> userAccessCodesHits = Arrays.asList(searchResponse.getHits().getHits()).stream()
                    .filter((h) -> h.getIndex().equals("user_access_codes")).collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 0, userAccessCodesHits.size());

        }
    }

    @Test
    public void testSimpleSearch_TwoIndicesConcreteNames_AccessCodes_1337() throws Exception {
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("tlq_1337", "password")) {
            SearchRequest request = new SearchRequest("tlqdocuments,tlqdummy");
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().from(0).size(100);
            request.source(searchSourceBuilder);
            SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);

            // assume hits from 2 indices:
            // - tlqdocuments, must contains only 10 docs with access code 1337
            // - tlqdummy, must contains all 5 documents

            // check all 5 tlqdummy entries present, index is not protected by DLS
            Set<SearchHit> tlqdummyHits = Arrays.asList(searchResponse.getHits().getHits()).stream().filter((h) -> h.getIndex().equals("tlqdummy"))
                    .collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 5, tlqdummyHits.size());

            // ccheck 10 hits with code 1337 from tlqdocuments index. All other documents must be filtered         
            Set<SearchHit> tlqdocumentHits = Arrays.asList(searchResponse.getHits().getHits()).stream()
                    .filter((h) -> h.getIndex().equals("tlqdocuments")).collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 10, tlqdocumentHits.size());
            assertAccessCodesMatch(tlqdocumentHits, new Integer[] { 1337 });
        }
    }

    @Test
    public void testMSearch_ThreeIndices_AccessCodes_1337() throws Exception {
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("tlq_1337", "password")) {

            MultiSearchRequest request = new MultiSearchRequest();
            request.add(new SearchRequest("tlqdummy"));
            request.add(new SearchRequest("tlqdocuments"));
            request.add(new SearchRequest("user_access_codes"));
            MultiSearchResponse searchResponse = client.msearch(request, RequestOptions.DEFAULT);

            Item[] responseItems = searchResponse.getResponses();

            // as per API order in response is the same as in the msearch request

            // check all 5 tlqdummy entries present
            List<SearchHit> tlqdummyHits = Arrays.asList(responseItems[0].getResponse().getHits().getHits());
            Assert.assertEquals(searchResponse.toString(), 5, tlqdummyHits.size());

            // check 10 hits with code 1337 from tlqdocuments index. All other documents must be filtered
            List<SearchHit> tlqdocumentHits = Arrays.asList(responseItems[1].getResponse().getHits().getHits());
            Assert.assertEquals(searchResponse.toString(), 10, tlqdocumentHits.size());
            assertAccessCodesMatch(tlqdocumentHits, new Integer[] { 1337 });

            // check no access to user_access_codes index, just two indices in the response
            Assert.assertTrue(responseItems[2].getResponse() == null);
            Assert.assertTrue(responseItems[2].getFailure() != null);

        }
    }

    // ------------------------
    // Test get and met
    // ------------------------

    @Test
    public void testGet_TlqDocumentsIndex_1337() throws Exception {
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("tlq_1337", "password")) {

            // user has 1337, document has 1337
            GetRequest request = new GetRequest().index("tlqdocuments").id("1");
            GetResponse searchResponse = client.get(request, RequestOptions.DEFAULT);
            Assert.assertTrue(searchResponse != null);
            Assert.assertTrue(searchResponse.isExists());
            assertAccessCodesMatch(searchResponse.getSourceAsMap(), "access_codes", new Integer[] { 1337 });

            // user has 1337, document has 42, not visible
            request = new GetRequest().index("tlqdocuments").id("2");
            searchResponse = client.get(request, RequestOptions.DEFAULT);
            Assert.assertFalse(searchResponse.isExists());

            // user has 1337, document has 42 and 1337
            request = new GetRequest().index("tlqdocuments").id("3");
            searchResponse = client.get(request, RequestOptions.DEFAULT);
            Assert.assertTrue(searchResponse != null);
            Assert.assertTrue(searchResponse.isExists());
            assertAccessCodesMatch(searchResponse.getSourceAsMap(), "access_codes", new Integer[] { 1337 });

            // user has 1337, document has no access codes, not visible
            request = new GetRequest().index("tlqdocuments").id("16");
            searchResponse = client.get(request, RequestOptions.DEFAULT);
            Assert.assertFalse(searchResponse.isExists());

            // user has 1337, document has 12345, not visible
            request = new GetRequest().index("tlqdocuments").id("17");
            searchResponse = client.get(request, RequestOptions.DEFAULT);
            Assert.assertFalse(searchResponse.isExists());

            // user has 1337, document has 12345 and 6789, not visible
            request = new GetRequest().index("tlqdocuments").id("18");
            searchResponse = client.get(request, RequestOptions.DEFAULT);
            Assert.assertFalse(searchResponse.isExists());

        }
    }

    @Test
    public void testGet_TlqDocumentsIndex_1337_42() throws Exception {
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("tlq_1337_42", "password")) {

            // user has 1337 and 42, document has 1337
            GetRequest request = new GetRequest().index("tlqdocuments").id("1");
            GetResponse searchResponse = client.get(request, RequestOptions.DEFAULT);
            Assert.assertTrue(searchResponse != null);
            Assert.assertTrue(searchResponse.isExists());
            assertAccessCodesMatch(searchResponse.getSourceAsMap(), "access_codes", new Integer[] { 1337, 42 });

            // user has 1337 and 42, document has 42
            request = new GetRequest().index("tlqdocuments").id("2");
            searchResponse = client.get(request, RequestOptions.DEFAULT);
            Assert.assertTrue(searchResponse != null);
            Assert.assertTrue(searchResponse.isExists());
            assertAccessCodesMatch(searchResponse.getSourceAsMap(), "access_codes", new Integer[] { 1337, 42 });

            // user has 1337 and 42, document has 42 and 1337
            request = new GetRequest().index("tlqdocuments").id("3");
            searchResponse = client.get(request, RequestOptions.DEFAULT);
            Assert.assertTrue(searchResponse != null);
            Assert.assertTrue(searchResponse.isExists());
            assertAccessCodesMatch(searchResponse.getSourceAsMap(), "access_codes", new Integer[] { 1337, 42 });

            // user has 1337 and 42, document has no access codes, not visible
            request = new GetRequest().index("tlqdocuments").id("16");
            searchResponse = client.get(request, RequestOptions.DEFAULT);
            Assert.assertFalse(searchResponse.isExists());

            // user has 1337 and 42, document has 12345, not visible
            request = new GetRequest().index("tlqdocuments").id("17");
            searchResponse = client.get(request, RequestOptions.DEFAULT);
            Assert.assertFalse(searchResponse.isExists());

            // user has 1337 and 42, document has 12345 and 6789, not visible
            request = new GetRequest().index("tlqdocuments").id("18");
            searchResponse = client.get(request, RequestOptions.DEFAULT);
            Assert.assertFalse(searchResponse.isExists());

        }
    }

    @Test
    public void testGet_TlqDummyIndex_1337() throws Exception {
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("tlq_1337", "password")) {

            // no restrictions on this index
            GetRequest request = new GetRequest().index("tlqdummy").id("101");
            GetResponse searchResponse = client.get(request, RequestOptions.DEFAULT);
            Assert.assertTrue(searchResponse != null);
            Assert.assertTrue(searchResponse.isExists());

            request = new GetRequest().index("tlqdummy").id("102");
            searchResponse = client.get(request, RequestOptions.DEFAULT);
            Assert.assertTrue(searchResponse != null);
            Assert.assertTrue(searchResponse.isExists());

        }
    }

    @Test
    public void testGet_UserAccessCodesIndex_1337() throws Exception {
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("tlq_1337", "password")) {
            // no access to user_codes index, must throw exception
            GetRequest request = new GetRequest().index("user_access_codes").id("tlq_1337");
            client.get(request, RequestOptions.DEFAULT);
            Assert.fail();
        } catch (ElasticsearchStatusException e) {
            Assert.assertEquals(e.toString(), RestStatus.FORBIDDEN, e.status());
        }
    }

    @Test
    public void testMGet_1337() throws Exception {
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("tlq_1337", "password")) {

            MultiGetRequest request = new MultiGetRequest();
            request.add("tlqdocuments", "1");
            request.add("tlqdocuments", "2");
            request.add("tlqdocuments", "3");
            request.add("tlqdocuments", "16");
            request.add("tlqdocuments", "17");
            request.add("tlqdocuments", "18");
            request.add("tlqdummy", "101");
            request.add("user_access_codes", "tlq_1337");

            MultiGetResponse searchResponse = client.mget(request, RequestOptions.DEFAULT);

            for (MultiGetItemResponse response : searchResponse.getResponses()) {
                // no response from index "user_access_codes"
                Assert.assertFalse(response.getIndex().equals("user_access_codes"));
                switch (response.getIndex()) {
                case "tlqdocuments":
                    Assert.assertTrue(response.getId(), response.getId().equals("1") | response.getId().equals("3"));
                    break;
                case "tlqdummy":
                    Assert.assertTrue(response.getId(), response.getId().equals("101"));
                    break;
                default:
                    Assert.fail("Index " + response.getIndex() + " present in mget response, but should not");
                }

            }

        }
    }

    // ------------------------
    // Test aggregations
    // ------------------------

    @Test
    public void testSimpleAggregation_tlqdocuments_AccessCode_1337() throws Exception {
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("tlq_1337", "password")) {

            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.aggregation(AggregationBuilders.terms("buaggregation").field("bu"));
            SearchRequest request = new SearchRequest("tlqdocuments").source(searchSourceBuilder);
            SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);
            Aggregations aggs = searchResponse.getAggregations();
            Assert.assertNotNull(searchResponse.toString(), aggs);
            Terms agg = aggs.get("buaggregation");
            Assert.assertTrue("Expected aggregation with name 'buaggregation'", agg != null);
            // expect AAA - EEE (FFF does not match) with 2 docs each
            for (String bucketName : new String[] { "AAA", "BBB", "CCC", "DDD", "EEE" }) {
                Bucket bucket = agg.getBucketByKey(bucketName);
                Assert.assertNotNull("Expected bucket " + bucketName + " to be present in agregations", bucket);
                Assert.assertTrue("Expected doc count in bucket " + bucketName + " to be 2", bucket.getDocCount() == 2);
            }
            // expect FFF to be absent
            Assert.assertNull("Expected bucket FFF to be absent", agg.getBucketByKey("FFF"));
        }
    }

    // ------------------------
    // Test scroll
    // ------------------------

    @Test
    public void testSimpleSearch_Scroll_AccessCode_1337() throws Exception {
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("tlq_1337", "password")) {

            final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
            SearchRequest searchRequest = new SearchRequest("tlqdocuments");
            searchRequest.scroll(scroll);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.size(1);
            searchRequest.source(searchSourceBuilder);

            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            String scrollId = searchResponse.getScrollId();
            SearchHit[] searchHits = searchResponse.getHits().getHits();
            int totalHits = 0;

            // we scroll one by one
            while (searchHits != null && searchHits.length > 0) {
                // for counting the total documents
                totalHits += searchHits.length;
                // only docs with access codes 1337 must be returned
                assertAccessCodesMatch(searchResponse.getHits().getHits(), new Integer[] { 1337 });
                // fetch next
                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(scroll);
                searchResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
                scrollId = searchResponse.getScrollId();
                searchHits = searchResponse.getHits().getHits();
            }

            // assume total of 10 documents
            Assert.assertTrue("" + totalHits, totalHits == 10);
        }
    }

}