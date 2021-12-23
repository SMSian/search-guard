package com.floragunn.searchguard;

import static com.floragunn.searchguard.test.RestMatchers.isForbidden;
import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static com.floragunn.searchguard.test.RestMatchers.json;
import static com.floragunn.searchguard.test.RestMatchers.nodeAt;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.ResizeRequest;
import org.elasticsearch.client.indices.ResizeResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.mustache.SearchTemplateRequest;
import org.elasticsearch.script.mustache.SearchTemplateResponse;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.test.helper.certificate.TestCertificates;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchguard.test.helper.cluster.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.rest.GenericRestClient;
import com.floragunn.searchguard.test.helper.rest.GenericRestClient.HttpResponse;
import com.google.common.collect.ImmutableMap;

public class PrivilegesEvaluatorTest {

    private static TestSgConfig.User RESIZE_USER_WITHOUT_CREATE_INDEX_PRIV = new TestSgConfig.User("resize_user_without_create_index_priv")
            .roles(new Role("resize_role").clusterPermissions("*").indexPermissions("indices:admin/resize", "indices:monitor/stats")
                    .on("resize_test_source"));

    private static TestSgConfig.User RESIZE_USER = new TestSgConfig.User("resize_user")
            .roles(new Role("resize_role").clusterPermissions("*").indexPermissions("indices:admin/resize", "indices:monitor/stats")
                    .on("resize_test_source").indexPermissions("SGS_CREATE_INDEX").on("resize_test_target"));

    private static TestSgConfig.User SEARCH_TEMPLATE_USER = new TestSgConfig.User("search_template_user").roles(new Role("search_template_role")
            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_SEARCH_TEMPLATES").indexPermissions("SGS_READ").on("resolve_test_*"));

    private static TestSgConfig.User SEARCH_NO_TEMPLATE_USER = new TestSgConfig.User("search_no_template_user").roles(
            new Role("search_no_template_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS").indexPermissions("SGS_READ").on("resolve_test_*"));

    private static TestSgConfig.User NEG_LOOKAHEAD_USER = new TestSgConfig.User("neg_lookahead_user").roles(
            new Role("neg_lookahead_user_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS").indexPermissions("SGS_READ").on("/^(?!t.*).*/"));

    private static TestSgConfig.User REGEX_USER = new TestSgConfig.User("regex_user")
            .roles(new Role("neg_lookahead_user_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS").indexPermissions("SGS_READ").on("/^[a-z].*/"));

    private static TestSgConfig.User SEARCH_TEMPLATE_LEGACY_USER = new TestSgConfig.User("search_template_legacy_user")
            .roles(new Role("search_template_legacy_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS").indexPermissions("SGS_READ")
                    .on("resolve_test_*").indexPermissions("indices:data/read/search/template").on("*"));
    
    private static TestSgConfig.User HIDDEN_TEST_USER = new TestSgConfig.User("hidden_test_user").roles(
            new Role("hidden_test_user_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS").indexPermissions("*").on("hidden_test_not_hidden"));

    private static TestCertificates certificatesContext = TestCertificates.builder()
            .ca("CN=root.ca.example.com,OU=SearchGuard,O=SearchGuard")
            .addNodes("CN=node-0.example.com,OU=SearchGuard,O=SearchGuard")
            .addClients("CN=client-0.example.com,OU=SearchGuard,O=SearchGuard")
            .addAdminClients("CN=admin-0.example.com,OU=SearchGuard,O=SearchGuard")
            .build();

    @ClassRule 
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();
    
    @ClassRule
    public static LocalCluster anotherCluster = new LocalCluster.Builder().singleNode().sslEnabled(certificatesContext)
            .setInSgConfig("sg_config.dynamic.do_not_fail_on_forbidden", "true")
            .user("resolve_test_user", "secret", new Role("resolve_test_user_role").indexPermissions("*").on("resolve_test_allow_*"))//
            .build();

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled(certificatesContext).remote("my_remote", anotherCluster)
            .setInSgConfig("sg_config.dynamic.do_not_fail_on_forbidden", "true")
            .user("resolve_test_user", "secret",
                    new Role("resolve_test_user_role").indexPermissions("*").on("resolve_test_allow_*").indexPermissions("*")
                            .on("/alias_resolve_test_index_allow_.*/")) //
            .user("exclusion_test_user_basic", "secret",
                    new Role("exclusion_test_user_role").clusterPermissions("*").indexPermissions("*").on("exclude_test_*")
                            .excludeIndexPermissions("*").on("exclude_test_disallow_*"))//
            .user("exclusion_test_user_basic_no_pattern", "secret",
                    new Role("exclusion_test_user_basic_no_pattern_role").clusterPermissions("*").indexPermissions("*").on("exclude_test_*")
                            .excludeIndexPermissions("*").on("exclude_test_disallow_2"))//            
            .user("exclusion_test_user_write", "secret",
                    new Role("exclusion_test_user_action_exclusion_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS")//
                            .indexPermissions("*").on("write_exclude_test_*")//
                            .excludeIndexPermissions("SGS_WRITE").on("write_exclude_test_disallow_*"))//  
            .user("exclusion_test_user_write_no_pattern", "secret",
                    new Role("exclusion_test_user_write_no_pattern_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS")//
                            .indexPermissions("*").on("write_exclude_test_*")//
                            .excludeIndexPermissions("SGS_WRITE").on("write_exclude_test_disallow_2"))//  
            .user("exclusion_test_user_cluster_permission", "secret",
                    new Role("exclusion_test_user_cluster_permission_role").clusterPermissions("*")
                            .excludeClusterPermissions("indices:data/read/msearch").indexPermissions("*").on("exclude_test_*")
                            .excludeIndexPermissions("*").on("exclude_test_disallow_*"))//
            .user("admin", "admin", new Role("admin_role").clusterPermissions("*"))//
            .user("permssion_rest_api_user", "secret", new Role("permssion_rest_api_user_role").clusterPermissions("indices:data/read/mtv"))//
            .users(SEARCH_TEMPLATE_USER, SEARCH_NO_TEMPLATE_USER, SEARCH_TEMPLATE_LEGACY_USER).build();

    @ClassRule
    public static LocalCluster clusterFof = new LocalCluster.Builder().singleNode().sslEnabled(certificatesContext).remote("my_remote", anotherCluster)
            .setInSgConfig("sg_config.dynamic.do_not_fail_on_forbidden", "false")
            .user("resolve_test_user", "secret",
                    new Role("resolve_test_user_role").indexPermissions("*").on("resolve_test_allow_*").indexPermissions("*")
                            .on("/alias_resolve_test_index_allow_.*/")) //            
            .user("exclusion_test_user_basic", "secret",
                    new Role("exclusion_test_user_role").clusterPermissions("*").indexPermissions("*").on("exclude_test_*")
                            .excludeIndexPermissions("*").on("exclude_test_disallow_*"))//
            .user("exclusion_test_user_basic_no_pattern", "secret",
                    new Role("exclusion_test_user_basic_no_pattern_role").clusterPermissions("*").indexPermissions("*").on("exclude_test_*")
                            .excludeIndexPermissions("*").on("exclude_test_disallow_2"))//                   
            .user("exclusion_test_user_write", "secret",
                    new Role("exclusion_test_user_action_exclusion_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS")//
                            .indexPermissions("*").on("write_exclude_test_*")//
                            .excludeIndexPermissions("SGS_WRITE").on("write_exclude_test_disallow_*"))//  
            .user("exclusion_test_user_write_no_pattern", "secret",
                    new Role("exclusion_test_user_write_no_pattern_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS")//
                            .indexPermissions("*").on("write_exclude_test_*")//
                            .excludeIndexPermissions("SGS_WRITE").on("write_exclude_test_disallow_2"))//             
            .user("exclusion_test_user_cluster_permission", "secret",
                    new Role("exclusion_test_user_cluster_permission_role").clusterPermissions("*")
                            .excludeClusterPermissions("indices:data/read/msearch").indexPermissions("*").on("exclude_test_*")
                            .excludeIndexPermissions("*").on("exclude_test_disallow_*"))//
            .users(RESIZE_USER, RESIZE_USER_WITHOUT_CREATE_INDEX_PRIV, NEG_LOOKAHEAD_USER, REGEX_USER, HIDDEN_TEST_USER)//
            .build();

    @BeforeClass
    public static void setupTestData() {

        try (Client client = cluster.getInternalNodeClient()) {
            client.index(new IndexRequest("resolve_test_allow_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "resolve_test_allow_1", "b", "y", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("resolve_test_allow_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "resolve_test_allow_2", "b", "yy", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("resolve_test_disallow_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "resolve_test_disallow_1", "b", "yy", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("resolve_test_disallow_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "resolve_test_disallow_2", "b", "yy", "date", "1985/01/01")).actionGet();

            client.index(new IndexRequest("alias_resolve_test_index_allow_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON,
                    "index", "alias_resolve_test_index_allow_1", "b", "y", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("alias_resolve_test_index_allow_aliased_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source(XContentType.JSON, "index", "alias_resolve_test_index_allow_aliased_1", "b", "y", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("alias_resolve_test_index_allow_aliased_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source(XContentType.JSON, "index", "alias_resolve_test_index_allow_aliased_2", "b", "y", "date", "1985/01/01")).actionGet();
            client.admin().indices().aliases(
                    new IndicesAliasesRequest().addAliasAction(AliasActions.add().alias("alias_resolve_test_alias_1").index("alias_resolve_test_*")))
                    .actionGet();

            client.index(new IndexRequest("exclude_test_allow_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "exclude_test_allow_1", "b", "y", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("exclude_test_allow_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "exclude_test_allow_2", "b", "yy", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("exclude_test_disallow_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "exclude_test_disallow_1", "b", "yy", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("exclude_test_disallow_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "exclude_test_disallow_2", "b", "yy", "date", "1985/01/01")).actionGet();
        }

        try (Client client = clusterFof.getInternalNodeClient()) {
            client.index(new IndexRequest("resolve_test_allow_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "resolve_test_allow_1", "b", "y", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("resolve_test_allow_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "resolve_test_allow_2", "b", "yy", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("resolve_test_disallow_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "resolve_test_disallow_1", "b", "yy", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("resolve_test_disallow_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "resolve_test_disallow_2", "b", "yy", "date", "1985/01/01")).actionGet();

            client.admin().indices()
                    .aliases(new IndicesAliasesRequest()
                            .addAliasAction(new AliasActions(AliasActions.Type.ADD).alias("resolve_test_allow_alias").indices("resolve_test_*")))
                    .actionGet();
            
            client.index(new IndexRequest("hidden_test_not_hidden").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "hidden_test_not_hidden", "b", "y", "date", "1985/01/01")).actionGet();
            
            client.admin().indices().create(new CreateIndexRequest(".hidden_test_actually_hidden").settings(ImmutableMap.of("index.hidden", true))).actionGet();
            client.index(new IndexRequest(".hidden_test_actually_hidden").id("test").source("a", "b").setRefreshPolicy(RefreshPolicy.IMMEDIATE)).actionGet();
            
            client.index(new IndexRequest("exclude_test_allow_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "exclude_test_allow_1", "b", "y", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("exclude_test_allow_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "exclude_test_allow_2", "b", "yy", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("exclude_test_disallow_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "exclude_test_disallow_1", "b", "yy", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("exclude_test_disallow_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "exclude_test_disallow_2", "b", "yy", "date", "1985/01/01")).actionGet();

            client.index(new IndexRequest("tttexclude_test_allow_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "tttexclude_test_allow_1", "b", "y", "date", "1985/01/01")).actionGet();
        }

        try (Client client = anotherCluster.getInternalNodeClient()) {
            client.index(new IndexRequest("resolve_test_allow_remote_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "a", "x",
                    "b", "y", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("resolve_test_allow_remote_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "a",
                    "xx", "b", "yy", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("resolve_test_disallow_remote_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "a",
                    "xx", "b", "yy", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("resolve_test_disallow_remote_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "a",
                    "xx", "b", "yy", "date", "1985/01/01")).actionGet();
        }
    }

    @Test
    public void resolveTestLocal() throws Exception {

        try (GenericRestClient restClient = cluster.getRestClient("resolve_test_user", "secret")) {
            HttpResponse httpResponse = restClient.get("/_resolve/index/resolve_test_*");

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(nodeAt("indices[*].name", contains("resolve_test_allow_1", "resolve_test_allow_2"))));
        }
    }

    @Test
    public void resolveTestRemote() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient("resolve_test_user", "secret")) {

            HttpResponse httpResponse = restClient.get("/_resolve/index/my_remote:resolve_test_*");

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse,
                    json(nodeAt("indices[*].name", contains("my_remote:resolve_test_allow_remote_1", "my_remote:resolve_test_allow_remote_2"))));
        }
    }

    @Test
    public void resolveTestLocalRemoteMixed() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient("resolve_test_user", "secret")) {

            HttpResponse httpResponse = restClient.get("/_resolve/index/resolve_test_*,my_remote:resolve_test_*_remote_*");

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(nodeAt("indices[*].name", contains("resolve_test_allow_1", "resolve_test_allow_2",
                    "my_remote:resolve_test_allow_remote_1", "my_remote:resolve_test_allow_remote_2"))));
        }
    }

    @Test
    public void resolveTestAliasAndIndexMixed() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient("resolve_test_user", "secret")) {

            HttpResponse httpResponse = restClient.get("/_resolve/index/alias_resolve_test_*");

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(nodeAt("indices[*].name", containsInAnyOrder("alias_resolve_test_index_allow_aliased_1",
                    "alias_resolve_test_index_allow_aliased_2", "alias_resolve_test_index_allow_1"))));
        }
    }

    @Test
    public void readAliasAndIndexMixed() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient("resolve_test_user", "secret")) {

            HttpResponse httpResponse = restClient.get("/alias_resolve_test_*/_search");

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(nodeAt("hits.hits[*]._source.index", containsInAnyOrder("alias_resolve_test_index_allow_aliased_1",
                    "alias_resolve_test_index_allow_aliased_2", "alias_resolve_test_index_allow_1"))));
        }
    }

    @Test
    public void excludeBasic() throws Exception {

        try (GenericRestClient restClient = cluster.getRestClient("exclusion_test_user_basic", "secret")) {

            HttpResponse httpResponse = restClient.get("/exclude_test_*/_search");

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse,
                    json(nodeAt("hits.hits[*]._source.index", containsInAnyOrder("exclude_test_allow_1", "exclude_test_allow_2"))));
        }
    }

    @Test
    public void excludeBasicNoPattern() throws Exception {

        try (GenericRestClient restClient = cluster.getRestClient("exclusion_test_user_basic_no_pattern", "secret")) {

            HttpResponse httpResponse = restClient.get("/exclude_test_*/_search");

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(nodeAt("hits.hits[*]._source.index",
                    containsInAnyOrder("exclude_test_allow_1", "exclude_test_allow_2", "exclude_test_disallow_1"))));
        }
    }

    @Test
    public void excludeWrite() throws Exception {
        try (Client client = cluster.getInternalNodeClient()) {
            client.index(new IndexRequest("write_exclude_test_allow_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "write_exclude_test_allow_1", "b", "y", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("write_exclude_test_allow_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "write_exclude_test_allow_2", "b", "yy", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("write_exclude_test_disallow_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON,
                    "index", "write_exclude_test_disallow_1", "b", "yy", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("write_exclude_test_disallow_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON,
                    "index", "write_exclude_test_disallow_2", "b", "yy", "date", "1985/01/01")).actionGet();
        }
        try (GenericRestClient restClient = cluster.getRestClient("exclusion_test_user_write", "secret");
                RestHighLevelClient client = cluster.getRestHighLevelClient("exclusion_test_user_write", "secret")) {

            HttpResponse httpResponse = restClient.get("/write_exclude_test_*/_search");

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(nodeAt("hits.hits[*]._source.index", containsInAnyOrder("write_exclude_test_allow_1",
                    "write_exclude_test_allow_2", "write_exclude_test_disallow_1", "write_exclude_test_disallow_2"))));

            IndexResponse indexResponse = client.index(new IndexRequest("write_exclude_test_allow_1").source("a", "b"), RequestOptions.DEFAULT);

            Assert.assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());

            try {
                client.index(new IndexRequest("write_exclude_test_disallow_1").source("a", "b"), RequestOptions.DEFAULT);

                Assert.fail();
            } catch (ElasticsearchStatusException e) {
                Assert.assertEquals(RestStatus.FORBIDDEN, e.status());
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("no permissions for [indices:data/write/index]"));
            }

        }
    }

    @Test
    public void excludeBasicFof() throws Exception {

        try (GenericRestClient restClient = clusterFof.getRestClient("exclusion_test_user_basic", "secret")) {

            HttpResponse httpResponse = restClient.get("/exclude_test_*/_search");
            Assert.assertThat(httpResponse, isForbidden());

            httpResponse = restClient.get("/exclude_test_allow_*/_search");
            Assert.assertThat(httpResponse, isOk());

            Assert.assertThat(httpResponse,
                    json(nodeAt("hits.hits[*]._source.index", containsInAnyOrder("exclude_test_allow_1", "exclude_test_allow_2"))));

            httpResponse = restClient.get("/exclude_test_disallow_1/_search");
            Assert.assertThat(httpResponse, isForbidden());
        }
    }

    @Test
    public void excludeBasicFofNoPattern() throws Exception {

        try (GenericRestClient restClient = clusterFof.getRestClient("exclusion_test_user_basic_no_pattern", "secret")) {

            HttpResponse httpResponse = restClient.get("/exclude_test_*/_search");
            Assert.assertThat(httpResponse, isForbidden());

            httpResponse = restClient.get("/exclude_test_allow_*/_search");
            Assert.assertThat(httpResponse, isOk());

            Assert.assertThat(httpResponse,
                    json(nodeAt("hits.hits[*]._source.index", containsInAnyOrder("exclude_test_allow_1", "exclude_test_allow_2"))));

            httpResponse = restClient.get("/exclude_test_disallow_1/_search");
            Assert.assertThat(httpResponse, isOk());

            httpResponse = restClient.get("/exclude_test_disallow_2/_search");
            Assert.assertThat(httpResponse, isForbidden());
        }
    }

    @Test
    public void excludeWriteFof() throws Exception {
        try (Client client = clusterFof.getInternalNodeClient()) {
            client.index(new IndexRequest("write_exclude_test_allow_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "write_exclude_test_allow_1", "b", "y", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("write_exclude_test_allow_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "write_exclude_test_allow_2", "b", "yy", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("write_exclude_test_disallow_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON,
                    "index", "write_exclude_test_disallow_1", "b", "yy", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("write_exclude_test_disallow_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON,
                    "index", "write_exclude_test_disallow_2", "b", "yy", "date", "1985/01/01")).actionGet();
        }

        try (GenericRestClient restClient = cluster.getRestClient("exclusion_test_user_write", "secret");
                RestHighLevelClient client = clusterFof.getRestHighLevelClient("exclusion_test_user_write", "secret")) {

            HttpResponse httpResponse = restClient.get("/write_exclude_test_*/_search");

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(nodeAt("hits.hits[*]._source.index", containsInAnyOrder("write_exclude_test_allow_1",
                    "write_exclude_test_allow_2", "write_exclude_test_disallow_1", "write_exclude_test_disallow_2"))));

            IndexResponse indexResponse = client.index(new IndexRequest("write_exclude_test_allow_1").source("a", "b"), RequestOptions.DEFAULT);

            Assert.assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());

            try {
                client.index(new IndexRequest("write_exclude_test_disallow_1").source("a", "b"), RequestOptions.DEFAULT);

                Assert.fail();
            } catch (ElasticsearchStatusException e) {
                Assert.assertEquals(RestStatus.FORBIDDEN, e.status());
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("no permissions for [indices:data/write/index]"));
            }
        }
    }

    @Test
    public void excludeClusterPermission() throws Exception {
        try (GenericRestClient basicCestClient = cluster.getRestClient("exclusion_test_user_basic", "secret");
                GenericRestClient clusterPermissionCestClient = cluster.getRestClient("exclusion_test_user_cluster_permission", "secret")) {

            HttpResponse httpResponse = basicCestClient.get("/exclude_test_*/_search");

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse,
                    json(nodeAt("hits.hits[*]._source.index", containsInAnyOrder("exclude_test_allow_1", "exclude_test_allow_2"))));

            httpResponse = clusterPermissionCestClient.get("/exclude_test_*/_search");

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse,
                    json(nodeAt("hits.hits[*]._source.index", containsInAnyOrder("exclude_test_allow_1", "exclude_test_allow_2"))));

            httpResponse = basicCestClient.postJson("/exclude_test_*/_msearch", "{}\n{\"query\": {\"match_all\": {}}}\n");
            Assert.assertThat(httpResponse, isOk());

            Assert.assertThat(httpResponse,
                    json(nodeAt("responses[0].hits.hits[*]._source.index", containsInAnyOrder("exclude_test_allow_1", "exclude_test_allow_2"))));

            httpResponse = clusterPermissionCestClient.postJson("/exclude_test_*/_msearch", "{}\n{\"query\": {\"match_all\": {}}}\n");
            Assert.assertThat(httpResponse, isForbidden());
        }
    }

    @Test
    public void evaluateClusterAndTenantPrivileges() throws Exception {
        try (GenericRestClient adminRestClient = cluster.getRestClient("admin", "admin");
                GenericRestClient permissionRestClient = cluster.getRestClient("permssion_rest_api_user", "secret")) {
            HttpResponse httpResponse = adminRestClient.get("/_searchguard/permission?permissions=indices:data/read/mtv,indices:data/read/viva");

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(nodeAt("permissions['indices:data/read/mtv']", equalTo(true))));
            Assert.assertThat(httpResponse, json(nodeAt("permissions['indices:data/read/viva']", equalTo(true))));

            httpResponse = permissionRestClient.get("/_searchguard/permission?permissions=indices:data/read/mtv,indices:data/read/viva");

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(nodeAt("permissions['indices:data/read/mtv']", equalTo(true))));
            Assert.assertThat(httpResponse, json(nodeAt("permissions['indices:data/read/viva']", equalTo(false))));
        }

    }

    @Test
    public void testResizeAction() throws Exception {
        String sourceIndex = "resize_test_source";
        String targetIndex = "resize_test_target";

        try (Client client = clusterFof.getInternalNodeClient()) {
            client.index(new IndexRequest(sourceIndex).setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index", "a", "b", "y",
                    "date", "1985/01/01")).actionGet();

            client.admin().indices()
                    .updateSettings(new UpdateSettingsRequest(sourceIndex).settings(Settings.builder().put("index.blocks.write", true).build()))
                    .actionGet();
        }

        Thread.sleep(300);

        try (RestHighLevelClient client = clusterFof.getRestHighLevelClient(RESIZE_USER_WITHOUT_CREATE_INDEX_PRIV)) {
            client.indices().shrink(new ResizeRequest(targetIndex, "whatever"), RequestOptions.DEFAULT);
            Assert.fail();
        } catch (ElasticsearchStatusException e) {
            // Expected
            Assert.assertTrue(e.toString(),
                    e.getMessage().contains("no permissions for [indices:admin/resize] and User resize_user_without_create_index_priv"));
        }

        try (RestHighLevelClient client = clusterFof.getRestHighLevelClient(RESIZE_USER_WITHOUT_CREATE_INDEX_PRIV)) {
            client.indices().shrink(new ResizeRequest(targetIndex, sourceIndex), RequestOptions.DEFAULT);
            Assert.fail();
        } catch (ElasticsearchStatusException e) {
            // Expected
            Assert.assertTrue(e.toString(),
                    e.getMessage().contains("no permissions for [indices:admin/create] and User resize_user_without_create_index_priv"));
        }

        try (RestHighLevelClient client = clusterFof.getRestHighLevelClient(RESIZE_USER)) {
            client.indices().shrink(new ResizeRequest(targetIndex, "whatever"), RequestOptions.DEFAULT);
            Assert.fail();
        } catch (ElasticsearchStatusException e) {
            // Expected
            Assert.assertTrue(e.toString(), e.getMessage().contains("no permissions for [indices:admin/resize] and User resize_user"));
        }

        try (RestHighLevelClient client = clusterFof.getRestHighLevelClient(RESIZE_USER)) {
            ResizeResponse resizeResponse = client.indices().shrink(new ResizeRequest(targetIndex, sourceIndex), RequestOptions.DEFAULT);
            Assert.assertTrue(resizeResponse.toString(), resizeResponse.isAcknowledged());
        }

        try (Client client = clusterFof.getInternalNodeClient()) {
            IndicesExistsResponse response = client.admin().indices().exists(new IndicesExistsRequest(targetIndex)).actionGet();
            Assert.assertTrue(response.toString(), response.isExists());
        }
    }

    @Test
    public void searchTemplate() throws Exception {

        SearchTemplateRequest searchTemplateRequest = new SearchTemplateRequest(new SearchRequest("resolve_test_allow_*"));
        searchTemplateRequest.setScriptType(ScriptType.INLINE);
        searchTemplateRequest.setScript("{\"query\": {\"term\": {\"b\": \"{{x}}\" } } }");
        searchTemplateRequest.setScriptParams(ImmutableMap.of("x", "yy"));

        try (RestHighLevelClient client = cluster.getRestHighLevelClient(SEARCH_TEMPLATE_USER)) {
            SearchTemplateResponse searchTemplateResponse = client.searchTemplate(searchTemplateRequest, RequestOptions.DEFAULT);
            SearchResponse searchResponse = searchTemplateResponse.getResponse();

            Assert.assertEquals(searchResponse.toString(), 1, searchResponse.getHits().getTotalHits().value);
        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient(SEARCH_NO_TEMPLATE_USER)) {
            SearchTemplateResponse searchTemplateResponse = client.searchTemplate(searchTemplateRequest, RequestOptions.DEFAULT);
            SearchResponse searchResponse = searchTemplateResponse.getResponse();

            Assert.fail(searchResponse.toString());
        } catch (ElasticsearchStatusException e) {
            Assert.assertEquals(e.toString(), RestStatus.FORBIDDEN, e.status());
        }
    }

    @Test
    public void searchTemplateLegacy() throws Exception {

        SearchTemplateRequest searchTemplateRequest = new SearchTemplateRequest(new SearchRequest("resolve_test_allow_*"));
        searchTemplateRequest.setScriptType(ScriptType.INLINE);
        searchTemplateRequest.setScript("{\"query\": {\"term\": {\"b\": \"{{x}}\" } } }");
        searchTemplateRequest.setScriptParams(ImmutableMap.of("x", "yy"));

        try (RestHighLevelClient client = cluster.getRestHighLevelClient(SEARCH_TEMPLATE_LEGACY_USER)) {
            SearchTemplateResponse searchTemplateResponse = client.searchTemplate(searchTemplateRequest, RequestOptions.DEFAULT);
            SearchResponse searchResponse = searchTemplateResponse.getResponse();

            Assert.assertEquals(searchResponse.toString(), 1, searchResponse.getHits().getTotalHits().value);
        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient(SEARCH_NO_TEMPLATE_USER)) {
            SearchTemplateResponse searchTemplateResponse = client.searchTemplate(searchTemplateRequest, RequestOptions.DEFAULT);
            SearchResponse searchResponse = searchTemplateResponse.getResponse();

            Assert.fail(searchResponse.toString());
        } catch (ElasticsearchStatusException e) {
            Assert.assertEquals(e.toString(), RestStatus.FORBIDDEN, e.status());
        }
    }

    @Test
    public void negativeLookaheadPattern() throws Exception {

        try (GenericRestClient restClient = clusterFof.getRestClient(NEG_LOOKAHEAD_USER)) {

            HttpResponse httpResponse = restClient.get("*/_search");

            Assert.assertEquals(httpResponse.getBody(), 403, httpResponse.getStatusCode());
            
            httpResponse = restClient.get("r*/_search");

            Assert.assertEquals(httpResponse.getBody(), 200, httpResponse.getStatusCode());
        }
    }

    @Test
    public void regexPattern() throws Exception {

        try (GenericRestClient restClient = clusterFof.getRestClient(REGEX_USER)) {

            HttpResponse httpResponse = restClient.get("*/_search");

            Assert.assertEquals(httpResponse.getBody(), 403, httpResponse.getStatusCode());
            
            httpResponse = restClient.get("r*/_search");

            Assert.assertEquals(httpResponse.getBody(), 200, httpResponse.getStatusCode());
        }
    }
    
    @Test
    public void resolveTestHidden() throws Exception {

        try (GenericRestClient restClient = clusterFof.getRestClient(HIDDEN_TEST_USER)) {
            HttpResponse httpResponse = restClient.get("/*hidden_test*/_search?expand_wildcards=all&pretty=true");
            Assert.assertEquals(httpResponse.getBody(), 403, httpResponse.getStatusCode());

            httpResponse = restClient.get("/*hidden_test*/_search?pretty=true");
            Assert.assertEquals(httpResponse.getBody(), 200, httpResponse.getStatusCode());
            Assert.assertFalse(httpResponse.getBody(), httpResponse.getBody().contains("hidden_test_actually_hidden"));
        }

    }
}
