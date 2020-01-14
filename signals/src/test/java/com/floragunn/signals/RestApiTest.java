package com.floragunn.signals;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.DayOfWeek;
import java.util.Base64;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import org.apache.http.Header;
import org.apache.http.HttpStatus;
import org.apache.http.message.BasicHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.quartz.TimeOfDay;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchguard.DefaultObjectMapper;
import com.floragunn.searchguard.test.helper.network.SocketUtils;
import com.floragunn.searchguard.test.helper.rest.RestHelper;
import com.floragunn.searchguard.test.helper.rest.RestHelper.HttpResponse;
import com.floragunn.signals.support.JsonBuilder;
import com.floragunn.signals.watch.Watch;
import com.floragunn.signals.watch.WatchBuilder;
import com.floragunn.signals.watch.action.handlers.email.EmailAccount;
import com.floragunn.signals.watch.action.handlers.email.EmailAction;
import com.floragunn.signals.watch.action.handlers.email.EmailAction.Attachment;
import com.floragunn.signals.watch.action.handlers.slack.SlackAccount;
import com.floragunn.signals.watch.action.handlers.slack.SlackActionConf;
import com.floragunn.signals.watch.init.WatchInitializationService;
import com.floragunn.signals.watch.result.ActionLog;
import com.floragunn.signals.watch.result.Status;
import com.floragunn.signals.watch.result.WatchLog;
import com.floragunn.signals.watch.severity.SeverityLevel;
import com.icegreen.greenmail.util.GreenMail;
import com.icegreen.greenmail.util.GreenMailUtil;
import com.icegreen.greenmail.util.ServerSetup;

import net.jcip.annotations.NotThreadSafe;

@NotThreadSafe
public class RestApiTest {
    private static final Logger log = LogManager.getLogger(RestApiTest.class);

    private static RestHelper rh = null;
    private static ScriptService scriptService;

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().resources("sg_config/signals")
            .nodeSettings("signals.enabled", true, "signals.index_names.log", "signals_main_log", "signals.enterprise.enabled", false).build();

    @BeforeClass
    public static void setupTestData() {

        try (Client client = cluster.getInternalClient()) {
            client.index(new IndexRequest("testsource").source(XContentType.JSON, "key1", "val1", "key2", "val2")).actionGet();

            client.index(new IndexRequest("testsource").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "a", "x", "b", "y"))
                    .actionGet();
            client.index(new IndexRequest("testsource").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "a", "xx", "b", "yy"))
                    .actionGet();
        }
    }

    @BeforeClass
    public static void setupDependencies() {
        scriptService = cluster.getInjectable(ScriptService.class);

        rh = cluster.restHelper();
    }

    @Test
    public void testGetWatchUnauthorized() throws Exception {

        Header auth = basicAuth("noshirt", "redshirt");
        String tenant = "_main";
        String watchId = "get_watch_unauth";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalClient()) {

            HttpResponse response = rh.executeGetRequest(watchPath, auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_FORBIDDEN, response.getStatusCode());

        }
    }

    @Test
    public void testPutWatch() throws Exception {
        Header auth = basicAuth("uhura", "uhura");
        String tenant = "_main";
        String watchId = "put_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalClient()) {
            client.admin().indices().create(new CreateIndexRequest("testsink_put_watch")).actionGet();

            Watch watch = new WatchBuilder(watchId).cronTrigger("* * * * * ?").search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("testsink_put_watch").name("testsink").build();
            HttpResponse response = rh.executePutRequest(watchPath, watch.toJson(), auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = rh.executeGetRequest(watchPath, auth);

            System.out.print(response.getBody());
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            watch = Watch.parseFromElasticDocument(new WatchInitializationService(null, scriptService), "test", "put_test", response.getBody(), -1);

            awaitMinCountOfDocuments(client, "testsink_put_watch", 1);

        } finally {
            rh.executeDeleteRequest(watchPath, auth);
        }
    }

    @Test
    public void testPutWatchWithSeverity() throws Exception {
        Header auth = basicAuth("uhura", "uhura");
        String tenant = "_main";
        String watchId = "put_test_severity";
        String testSink = "testsink_" + watchId;
        String testSinkResolve = "testsink_resolve_" + watchId;
        String testSource = "testsource_" + watchId;
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalClient()) {
            client.index(new IndexRequest(testSource).setRefreshPolicy(RefreshPolicy.IMMEDIATE).id("1").source(XContentType.JSON, "a", "x", "b", "y"))
                    .actionGet();

            client.admin().indices().create(new CreateIndexRequest(testSink)).actionGet();
            client.admin().indices().create(new CreateIndexRequest(testSinkResolve)).actionGet();

            Watch watch = new WatchBuilder(watchId).cronTrigger("* * * * * ?").search(testSource).query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").consider("data.testsearch.hits.total.value").greaterOrEqual(1)
                    .as(SeverityLevel.ERROR).when(SeverityLevel.ERROR).index(testSink).name("a1").and().whenResolved(SeverityLevel.ERROR)
                    .index(testSinkResolve).name("r1").build();

            System.out.print(watch.toJson());

            HttpResponse response = rh.executePutRequest(watchPath, watch.toJson(), auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = rh.executeGetRequest(watchPath, auth);

            System.out.print(response.getBody());
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            watch = Watch.parseFromElasticDocument(new WatchInitializationService(null, scriptService), "test", "put_test", response.getBody(), -1);

            awaitMinCountOfDocuments(client, testSink, 1);

            Assert.assertEquals(0, getCountOfDocuments(client, testSinkResolve));

            client.delete(new DeleteRequest(testSource).setRefreshPolicy(RefreshPolicy.IMMEDIATE).id("1")).actionGet();

            awaitMinCountOfDocuments(client, testSinkResolve, 1);

            Thread.sleep(2000);

            Assert.assertEquals(1, getCountOfDocuments(client, testSinkResolve));

        } finally {
            rh.executeDeleteRequest(watchPath, auth);
        }
    }

    @Test
    public void testPutWatchWithSeverityValidation() throws Exception {
        Header auth = basicAuth("uhura", "uhura");
        String tenant = "_main";
        String watchId = "put_test_severity_validation";
        String testSink = "testsink_" + watchId;
        String testSinkResolve = "testsink_resolve_" + watchId;
        String testSource = "testsource_" + watchId;
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalClient()) {
            client.index(new IndexRequest(testSource).setRefreshPolicy(RefreshPolicy.IMMEDIATE).id("1").source(XContentType.JSON, "a", "x", "b", "y"))
                    .actionGet();

            client.admin().indices().create(new CreateIndexRequest(testSink)).actionGet();
            client.admin().indices().create(new CreateIndexRequest(testSinkResolve)).actionGet();

            Watch watch = new WatchBuilder(watchId).cronTrigger("* * * * * ?").search(testSource).query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").consider("data.testsearch.hits.total.value").greaterOrEqual(1)
                    .as(SeverityLevel.ERROR).when(SeverityLevel.INFO).index(testSink).name("a1").and().whenResolved(SeverityLevel.ERROR)
                    .index(testSinkResolve).name("r1").build();

            HttpResponse response = rh.executePutRequest(watchPath, watch.toJson(), auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
            Assert.assertTrue(response.getBody(), response.getBody().contains("Uses a severity which is not defined by severity mapping: [info]"));

            System.out.println(response.getBody());

        } finally {
            rh.executeDeleteRequest(watchPath, auth);
        }
    }

    @Test
    public void testPutWatchWithSeverity2() throws Exception {
        Header auth = basicAuth("uhura", "uhura");
        String tenant = "_main";
        String watchId = "put_test_severity2";
        String testSink = "testsink_" + watchId;
        String testSinkResolve1 = "testsink_resolve1_" + watchId;
        String testSinkResolve2 = "testsink_resolve2_" + watchId;
        String testSource = "testsource_" + watchId;
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalClient()) {
            client.index(new IndexRequest(testSource).setRefreshPolicy(RefreshPolicy.IMMEDIATE).id("1").source(XContentType.JSON, "a", "x", "b", "y"))
                    .actionGet();

            client.admin().indices().create(new CreateIndexRequest(testSink)).actionGet();
            client.admin().indices().create(new CreateIndexRequest(testSinkResolve1)).actionGet();
            client.admin().indices().create(new CreateIndexRequest(testSinkResolve2)).actionGet();

            Watch watch = new WatchBuilder(watchId).atMsInterval(400).search(testSource).query("{\"match_all\" : {} }").as("testsearch")
                    .consider("data.testsearch.hits.total.value").greaterOrEqual(1).as(SeverityLevel.ERROR).greaterOrEqual(2)
                    .as(SeverityLevel.CRITICAL).when(SeverityLevel.ERROR, SeverityLevel.CRITICAL).index(testSink).name("a1").throttledFor("24h").and()
                    .whenResolved(SeverityLevel.ERROR).index(testSinkResolve1).name("r1").and().whenResolved(SeverityLevel.CRITICAL)
                    .index(testSinkResolve2).name("r2").build();

            System.out.print(watch.toJson());

            HttpResponse response = rh.executePutRequest(watchPath, watch.toJson(), auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = rh.executeGetRequest(watchPath, auth);

            System.out.print(response.getBody());
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            watch = Watch.parseFromElasticDocument(new WatchInitializationService(null, scriptService), "test", "put_test", response.getBody(), -1);

            awaitMinCountOfDocuments(client, testSink, 1);

            Thread.sleep(500);

            Assert.assertEquals(0, getCountOfDocuments(client, testSinkResolve1));
            Assert.assertEquals(0, getCountOfDocuments(client, testSinkResolve2));
            Assert.assertEquals(1, getCountOfDocuments(client, testSink));

            client.index(new IndexRequest(testSource).setRefreshPolicy(RefreshPolicy.IMMEDIATE).id("2").source(XContentType.JSON, "a", "x", "b", "y"))
                    .actionGet();

            awaitMinCountOfDocuments(client, testSink, 2);
            Thread.sleep(500);

            Assert.assertEquals(0, getCountOfDocuments(client, testSinkResolve1));
            Assert.assertEquals(0, getCountOfDocuments(client, testSinkResolve2));
            Assert.assertEquals(2, getCountOfDocuments(client, testSink));

            client.delete(new DeleteRequest(testSource).setRefreshPolicy(RefreshPolicy.IMMEDIATE).id("1")).actionGet();

            awaitMinCountOfDocuments(client, testSinkResolve2, 1);

            Thread.sleep(200);

            Assert.assertEquals(0, getCountOfDocuments(client, testSinkResolve1));
            Assert.assertEquals(1, getCountOfDocuments(client, testSinkResolve2));
            Assert.assertEquals(2, getCountOfDocuments(client, testSink));

            client.delete(new DeleteRequest(testSource).setRefreshPolicy(RefreshPolicy.IMMEDIATE).id("2")).actionGet();

            awaitMinCountOfDocuments(client, testSinkResolve1, 1);

            Thread.sleep(200);

            Assert.assertEquals(1, getCountOfDocuments(client, testSinkResolve1));
            Assert.assertEquals(1, getCountOfDocuments(client, testSinkResolve2));
            Assert.assertEquals(2, getCountOfDocuments(client, testSink));
        } finally {
            rh.executeDeleteRequest(watchPath, auth);
        }
    }

    @Test
    public void testPutWatchWithDash() throws Exception {
        Header auth = basicAuth("uhura", "uhura");
        String tenant = "dash-tenant";
        String watchId = "dash-watch";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalClient()) {
            client.admin().indices().create(new CreateIndexRequest("testsink_put_watch_with_dash")).actionGet();

            Watch watch = new WatchBuilder(watchId).cronTrigger("* * * * * ?").search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("testsink_put_watch_with_dash").name("testsink").build();
            HttpResponse response = rh.executePutRequest(watchPath, watch.toJson(), auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = rh.executeGetRequest(watchPath, auth);

            System.out.print(response.getBody());
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            watch = Watch.parseFromElasticDocument(new WatchInitializationService(null, scriptService), "test", "put_test", response.getBody(), -1);

            awaitMinCountOfDocuments(client, "testsink_put_watch_with_dash", 1);

            rh.executeDeleteRequest(watchPath, auth);

            Thread.sleep(500);

            response = rh.executeGetRequest(watchPath, auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_NOT_FOUND, response.getStatusCode());

        } finally {
            rh.executeDeleteRequest(watchPath, auth);
        }
    }

    @Test
    public void testAuthTokenFilter() throws Exception {
        Header auth = basicAuth("uhura", "uhura");
        String tenant = "_main";
        String watchId = "filter";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalClient()) {
            Watch watch = new WatchBuilder(watchId).cronTrigger("* * * * * ?").search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("testsink_put_watch").name("testsink").build();
            HttpResponse response = rh.executePutRequest(watchPath, watch.toJson(), auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = rh.executeGetRequest(watchPath, auth);
            System.out.print(response.getBody());

            Assert.assertFalse(response.getBody(), response.getBody().contains("auth_token"));

            watch = Watch.parseFromElasticDocument(new WatchInitializationService(null, scriptService), "test", watchId, response.getBody(), -1);

            Assert.assertNull(response.getBody(), watch.getAuthToken());

        } finally {
            rh.executeDeleteRequest(watchPath, auth);
        }
    }

    @Test
    public void testPutInvalidWatch() throws Exception {
        Header auth = basicAuth("uhura", "uhura");
        String tenant = "_main";
        String watchId = "put_invalid_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalClient()) {
            String watchJson = "{\"trigger\":{\"schedule\":{\"timezone\":\"Europe/Berlino\",\"cron\":[\"* * argh * * ?\"],\"x\": 2}}," //
                    + "\"checks\":["
                    + "{\"type\":\"searchx\",\"name\":\"testsearch\",\"target\":\"testsearch\",\"request\":{\"indices\":[\"testsource\"],\"body\":{\"query\":{\"match_all\":{}}}}},"
                    + "{\"type\":\"static\",\"name\":\"teststatic\",\"target\":\"teststatic\",\"value\":{\"bla\":{\"blub\":42}}},"
                    + "{\"type\":\"transform\",\"target\":\"testtransform\",\"source\":\"1 + x\"}," //
                    + "{\"type\":\"calc\",\"name\":\"testcalc\",\"source\":\"1 +\"}" //
                    + "]," //
                    + "\"actions\":[{\"type\":\"index\",\"index\":\"testsink_put_watch\"}],\"horst\": true}";

            HttpResponse response = rh.executePutRequest(watchPath, watchJson, auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
            System.out.println(response.getBody());

            JsonNode parsedResponse = DefaultObjectMapper.readTree(response.getBody());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_BAD_REQUEST, parsedResponse.get("status").asInt());
            Assert.assertEquals(response.getBody(), "Invalid value",
                    parsedResponse.get("detail").get("checks[testsearch].type").get(0).get("error").asText());
            Assert.assertEquals(response.getBody(), "searchx",
                    parsedResponse.get("detail").get("checks[testsearch].type").get(0).get("value").asText());
            Assert.assertEquals(response.getBody(), "Variable [x] is not defined.",
                    parsedResponse.get("detail").get("checks[].source").get(0).get("error").asText());
            Assert.assertTrue(response.getBody(),
                    parsedResponse.get("detail").get("trigger.schedule.cron").get(0).get("error").asText().contains("Invalid cron expression"));
            Assert.assertTrue(response.getBody(),
                    parsedResponse.get("detail").get("trigger.schedule.x").get(0).get("error").asText().contains("Unsupported attribute"));
            Assert.assertEquals(response.getBody(), "Required attribute is missing",
                    parsedResponse.get("detail").get("actions[].name").get(0).get("error").asText());
            Assert.assertEquals(response.getBody(), "unexpected end of script.",
                    parsedResponse.get("detail").get("checks[testcalc].source").get(0).get("error").asText());
            Assert.assertEquals(response.getBody(), "Unsupported attribute", parsedResponse.get("detail").get("horst").get(0).get("error").asText());

        }
    }

    @Test
    public void testPutInvalidWatchJsonSyntaxError() throws Exception {
        Header auth = basicAuth("uhura", "uhura");
        String tenant = "_main";
        String watchId = "put_invalid_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalClient()) {
            String watchJson = "{\"trigger\":{";

            HttpResponse response = rh.executePutRequest(watchPath, watchJson, auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
            System.out.println(response.getBody());

            JsonNode parsedResponse = DefaultObjectMapper.readTree(response.getBody());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_BAD_REQUEST, parsedResponse.get("status").asInt());
            Assert.assertTrue(response.getBody(),
                    parsedResponse.get("detail").get("_").get(0).get("error").asText().contains("Error while parsing JSON document"));
        }
    }

    @Test
    public void testPutWatchUnauthorized() throws Exception {

        Header auth = basicAuth("redshirt3", "redshirt");
        String tenant = "_main";
        String watchId = "put_watch_unauth";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalClient()) {
            Watch watch = new WatchBuilder("put_test").cronTrigger("* * * * * ?").search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("testsink_put_watch").name("testsink").build();
            HttpResponse response = rh.executePutRequest(watchPath, watch.toJson(), auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_FORBIDDEN, response.getStatusCode());

        } finally {
            rh.executeDeleteRequest(watchPath, auth);
        }
    }

    @Test
    public void testPutWatchWithUnauthorizedCheck() throws Exception {

        Header auth = basicAuth("redshirt2", "redshirt");
        String tenant = "_main";
        String watchId = "put_watch_with_unauth_check";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalClient()) {
            client.admin().indices().create(new CreateIndexRequest("testsink_put_watch_with_unauth_check")).actionGet();

            Watch watch = new WatchBuilder("put_test").cronTrigger("* * * * * ?").search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("testsink_put_watch_with_unauth_action").name("testsink").build();
            HttpResponse response = rh.executePutRequest(watchPath, watch.toJson(), auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            WatchLog watchLog = awaitWatchLog(client, "main", watchId);

            Assert.assertEquals(watchLog.toString(), Status.Code.EXECUTION_FAILED, watchLog.getStatus().getCode());
            Assert.assertTrue(watchLog.toString(), watchLog.getStatus().getDetail().contains("Error while executing SearchInput testsearch"));
            Assert.assertTrue(watchLog.toString(), watchLog.getStatus().getDetail().contains("no permissions for [indices:data/read/search]"));
        } finally {
            rh.executeDeleteRequest(watchPath, auth);
        }

    }

    @Test
    public void testHttpWhitelist() throws Exception {

        Header auth = basicAuth("uhura", "uhura");
        String tenant = "_main";
        String watchId = "http_whitelist";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalClient(); MockWebserviceProvider webhookProvider = new MockWebserviceProvider("/hook")) {
            client.admin().indices().create(new CreateIndexRequest("testsink_put_watch_with_credentials")).actionGet();

            Watch watch = new WatchBuilder("put_test").atMsInterval(100).search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().postWebhook(webhookProvider.getUri()).throttledFor("0").name("testhook")
                    .build();
            HttpResponse response = rh.executePutRequest(watchPath, watch.toJson(), auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            Thread.sleep(600);

            Assert.assertTrue(webhookProvider.getRequestCount() > 0);

            response = rh.executePutRequest("/_signals/settings/http.allowed_endpoints", "https://unkown*,https://whatever*", auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            response = rh.executeGetRequest("/_signals/settings/http.allowed_endpoints", auth);

            Thread.sleep(300);

            long requestCount = webhookProvider.getRequestCount();

            Thread.sleep(600);
            Assert.assertEquals(requestCount, webhookProvider.getRequestCount());

        } finally {
            rh.executePutRequest("/_signals/settings/http.allowed_endpoints", "[\"*\"]", auth);
            rh.executeDeleteRequest(watchPath, auth);
        }

    }

    @Ignore
    @Test
    public void testPutWatchWithCredentials() throws Exception {

        Header auth = basicAuth("uhura", "uhura");
        String tenant = "_main";
        String watchId = "put_watch_with_credentials";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalClient(); MockWebserviceProvider webhookProvider = new MockWebserviceProvider("/hook")) {
            client.admin().indices().create(new CreateIndexRequest("testsink_put_watch_with_credentials")).actionGet();

            Watch watch = new WatchBuilder("put_test").cronTrigger("* * * * * ?").search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().postWebhook(webhookProvider.getUri()).basicAuth("admin", "secret")
                    .name("testhook").build();
            HttpResponse response = rh.executePutRequest(watchPath, watch.toJson(), auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = rh.executeGetRequest(watchPath + "?pretty", auth);
            System.out.println(response.getBody());
            //this seems failing because in "get watch action" there is no real deserialization of a watch object
            //and so the tox params are not effective
            Assert.assertFalse(response.getBody(), response.getBody().contains("secret"));
            Assert.assertTrue(response.getBody(), response.getBody().contains("password__protected"));

            Thread.sleep(3000);
            Assert.assertEquals(1, webhookProvider.getRequestCount());

        } finally {
            rh.executeDeleteRequest(watchPath, auth);
        }

    }

    @Test
    public void testPutWatchWithUnauthorizedAction() throws Exception {

        Header auth = basicAuth("redshirt1", "redshirt");
        String tenant = "_main";
        String watchId = "put_watch_with_unauth_action";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalClient()) {
            client.admin().indices().create(new CreateIndexRequest("testsink_put_watch_with_unauth_action")).actionGet();

            Watch watch = new WatchBuilder("put_test").cronTrigger("* * * * * ?").search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("testsink_put_watch_with_unauth_action").name("testsink").build();
            HttpResponse response = rh.executePutRequest(watchPath, watch.toJson(), auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            WatchLog watchLog = awaitWatchLog(client, "main", watchId);

            Assert.assertEquals(watchLog.toString(), Status.Code.ACTION_FAILED, watchLog.getStatus().getCode());

            ActionLog actionLog = watchLog.getActions().get(0);

            Assert.assertEquals(actionLog.toString(), Status.Code.ACTION_FAILED, actionLog.getStatus().getCode());
            Assert.assertTrue(actionLog.toString(), actionLog.getStatus().getDetail().contains("no permissions for [indices:data/write/index]"));

        } finally {
            rh.executeDeleteRequest(watchPath, auth);
        }

    }

    @Test
    public void testPutWatchWithTenant() throws Exception {

        Header auth = basicAuth("uhura", "uhura");
        String tenant = "test1";
        String watchId = "put_watch_with_tenant";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;
        String watchPathWithWrongTenant = "/_signals/watch/_main/" + watchId;

        try (Client client = cluster.getInternalClient()) {
            client.admin().indices().create(new CreateIndexRequest("testsink_put_watch_with_tenant")).actionGet();

            Watch watch = new WatchBuilder("put_test").cronTrigger("* * * * * ?").search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("testsink_put_watch_with_tenant").name("testsink").build();
            HttpResponse response = rh.executePutRequest(watchPath, watch.toJson(), auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = rh.executeGetRequest(watchPath, auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            watch = Watch.parseFromElasticDocument(new WatchInitializationService(null, scriptService), "test", "put_test", response.getBody(), -1);

            response = rh.executeGetRequest(watchPathWithWrongTenant, auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_NOT_FOUND, response.getStatusCode());

        } finally {
            rh.executeDeleteRequest(watchPath, auth);
        }
    }

    @Test
    public void testPutWatchWithTenant2() throws Exception {

        Header auth = basicAuth("redshirt3", "redshirt");
        String tenant = "redshirt_club";
        String watchId = "put_watch_with_tenant2";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;
        String watchPathWithWrongTenant = "/_signals/watch/_main/" + watchId;

        try (Client client = cluster.getInternalClient()) {
            client.admin().indices().create(new CreateIndexRequest("testsink_put_watch_with_tenant2")).actionGet();

            Watch watch = new WatchBuilder("put_test").cronTrigger("* * * * * ?").search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("testsink_put_watch_with_tenant2").name("testsink").build();
            HttpResponse response = rh.executePutRequest(watchPath, watch.toJson(), auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = rh.executeGetRequest(watchPath, auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            watch = Watch.parseFromElasticDocument(new WatchInitializationService(null, scriptService), "test", "put_test", response.getBody(), -1);

            response = rh.executeGetRequest(watchPathWithWrongTenant, auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_NOT_FOUND, response.getStatusCode());

        } finally {
            rh.executeDeleteRequest(watchPath, auth);
        }
    }

    @Test
    public void testPutWatchWithUnauthorizedTenant() throws Exception {

        Header auth = basicAuth("redshirt1", "redshirt");
        String tenant = "test1";
        String watchId = "put_watch_with_unauthorized_tenant";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalClient()) {

            Watch watch = new WatchBuilder("put_test").cronTrigger("* * * * * ?").search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("testsink_put_watch_with_tenant").name("testsink").build();
            HttpResponse response = rh.executePutRequest(watchPath, watch.toJson(), auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_FORBIDDEN, response.getStatusCode());

        } finally {
            rh.executeDeleteRequest(watchPath, auth);
        }
    }

    @Test
    public void testDeleteWatch() throws Exception {

        Header auth = basicAuth("uhura", "uhura");
        String tenant = "_main";
        String watchId = "delete_watch";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalClient()) {
            client.admin().indices().create(new CreateIndexRequest("testsink_delete_watch")).actionGet();

            Watch watch = new WatchBuilder("put_test").atMsInterval(10).search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("testsink_delete_watch").name("testsink").build();
            HttpResponse response = rh.executePutRequest(watchPath, watch.toJson(), auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = rh.executeGetRequest(watchPath, auth);
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            awaitMinCountOfDocuments(client, "testsink_delete_watch", 1);

            rh.executeDeleteRequest(watchPath, auth);

            response = rh.executeGetRequest(watchPath, auth);
            Assert.assertEquals(response.getBody(), HttpStatus.SC_NOT_FOUND, response.getStatusCode());

            Thread.sleep(1500);

            long docCount = getCountOfDocuments(client, "testsink_delete_watch");

            Thread.sleep(1000);

            long newDocCount = getCountOfDocuments(client, "testsink_delete_watch");

            Assert.assertEquals(docCount, newDocCount);

        }
    }

    @Test
    public void testExecuteAnonymousWatch() throws Exception {

        try (Client client = cluster.getInternalClient()) {

            Watch watch = new WatchBuilder("execution_test_anon").cronTrigger("*/2 * * * * ?").search("testsource").query("{\"match_all\" : {} }")
                    .as("testsearch").put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("testsink").name("testsink").build();

            HttpResponse response = rh.executePostRequest("/_signals/watch/_main/_execute", "{\"watch\": " + watch.toJson() + "}",
                    basicAuth("uhura", "uhura"));

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

        }
    }

    @Test
    public void testExecuteWatchById() throws Exception {
        Header auth = basicAuth("uhura", "uhura");
        String tenant = "_main";
        String watchId = "execution_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalClient()) {

            Watch watch = new WatchBuilder(watchId).cronTrigger("0 0 */1 * * ?").search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("testsink").name("testsink").build();
            HttpResponse response = rh.executePutRequest(watchPath, watch.toJson(), auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = rh.executePostRequest(watchPath + "/_execute", "{}", auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());
        } finally {
            rh.executeDeleteRequest(watchPath, auth);
        }
    }

    @Test
    public void testExecuteAnonymousWatchWithGoto() throws Exception {

        String testSink = "testsink_anon_watch_with_goto";

        try (Client client = cluster.getInternalClient()) {

            Watch watch = new WatchBuilder("execution_test_anon").cronTrigger("*/2 * * * * ?").search("testsource").query("{\"match_all\" : {} }")
                    .as("testsearch").put("{\"bla\": {\"blub\": 42}, \"x\": \"1\"}").as("teststatic").then().index(testSink).docId("1")
                    .refreshPolicy(RefreshPolicy.IMMEDIATE).name("testsink").build();

            HttpResponse response = rh.executePostRequest("/_signals/watch/_main/_execute",
                    "{\"watch\": " + watch.toJson() + ", \"goto\": \"teststatic\"}", basicAuth("uhura", "uhura"));

            System.out.println(watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            GetResponse getResponse = client.get(new GetRequest(testSink, "1")).actionGet();

            Assert.assertTrue(getResponse.toString(), getResponse.getSource().get("testsource") == null);
            Assert.assertTrue(getResponse.toString(), getResponse.getSource().get("teststatic") != null);

        }
    }

    @Test
    public void testExecuteAnonymousWatchWithInput() throws Exception {

        String testSink = "testsink_anon_watch_with_input";

        try (Client client = cluster.getInternalClient()) {

            Watch watch = new WatchBuilder("execution_test_anon").cronTrigger("*/2 * * * * ?").search("testsource").query("{\"match_all\" : {} }")
                    .as("testsearch").put("{\"bla\": {\"blub\": 42}, \"x\": \"1\"}").as("teststatic").then().index(testSink).docId("1")
                    .refreshPolicy(RefreshPolicy.IMMEDIATE).name("testsink").build();

            HttpResponse response = rh.executePostRequest("/_signals/watch/_main/_execute",
                    "{\"watch\": " + watch.toJson() + ", \"goto\": \"_actions\", \"input\": { \"ext_input\": \"a\"}}", basicAuth("uhura", "uhura"));

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            GetResponse getResponse = client.get(new GetRequest(testSink, "1")).actionGet();

            Assert.assertTrue(getResponse.toString(), getResponse.getSource().get("testsource") == null);
            Assert.assertTrue(getResponse.toString(), getResponse.getSource().get("teststatic") == null);
            Assert.assertTrue(getResponse.toString(), getResponse.getSource().get("ext_input") != null);

        }
    }

    @Test
    public void testActivateWatchAuth() throws Exception {
        Header auth = basicAuth("uhura", "uhura");
        String tenant = "_main";
        String watchId = "activate_auth_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalClient()) {

            Watch watch = new WatchBuilder("deactivate_test").inactive().atMsInterval(100).search("testsource").query("{\"match_all\" : {} }")
                    .as("testsearch").put("{\"bla\": {\"blub\": 42}}").as("teststatic").build();
            HttpResponse response = rh.executePutRequest(watchPath, watch.toJson(), auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = rh.executePutRequest(watchPath + "/_active", "", auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            watch = getWatchByRest(tenant, watchId, auth);

            Assert.assertEquals(true, watch.isActive());

            response = rh.executeDeleteRequest(watchPath + "/_active", auth);
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            watch = getWatchByRest(tenant, watchId, auth);
            Assert.assertFalse(watch.isActive());

            response = rh.executeDeleteRequest(watchPath + "/_active", auth);
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            watch = getWatchByRest(tenant, watchId, auth);
            Assert.assertFalse(watch.isActive());

            response = rh.executePutRequest(watchPath + "/_active", "", auth);
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            watch = getWatchByRest(tenant, watchId, auth);
            Assert.assertTrue(watch.isActive());

        } finally {
            rh.executeDeleteRequest(watchPath, auth);
        }
    }

    @Test
    public void testDeactivateWatch() throws Exception {
        Header auth = basicAuth("uhura", "uhura");
        String tenant = "_main";
        String watchId = "deactivate_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalClient()) {

            client.admin().indices().create(new CreateIndexRequest("testsink_deactivate_watch")).actionGet();

            Watch watch = new WatchBuilder(watchId).atMsInterval(100).search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("testsink_deactivate_watch").throttledFor("0").name("testsink")
                    .build();
            HttpResponse response = rh.executePutRequest(watchPath, watch.toJson(), auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            awaitMinCountOfDocuments(client, "testsink_deactivate_watch", 1);

            response = rh.executeDeleteRequest(watchPath + "/_active", auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            Watch updatedWatch = getWatchByRest(tenant, watchId, auth);

            Assert.assertFalse(updatedWatch.isActive());

            Thread.sleep(1500);

            long executionCountWhenDeactivated = getCountOfDocuments(client, "testsink_deactivate_watch");

            Thread.sleep(1000);

            long lastExecutionCount = getCountOfDocuments(client, "testsink_deactivate_watch");

            Assert.assertEquals(executionCountWhenDeactivated, lastExecutionCount);

            response = rh.executePutRequest(watchPath + "/_active", "", auth);
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            awaitMinCountOfDocuments(client, "testsink_deactivate_watch", lastExecutionCount + 1);

        } finally {
            rh.executeDeleteRequest(watchPath, auth);
        }
    }

    @Test
    public void testDeactivateTenant() throws Exception {
        Header auth = basicAuth("uhura", "uhura");
        String tenant = "_main";
        String watchId = "deactivate_tenant_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;
        String testSink = "testsink_" + watchId;

        try (Client client = cluster.getInternalClient()) {

            client.admin().indices().create(new CreateIndexRequest(testSink)).actionGet();

            Watch watch = new WatchBuilder(watchId).atMsInterval(100).search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index(testSink).throttledFor("0").name("testsink").build();
            HttpResponse response = rh.executePutRequest(watchPath, watch.toJson(), auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            awaitMinCountOfDocuments(client, testSink, 1);

            response = rh.executeDeleteRequest("/_signals/tenant/" + tenant + "/_active", auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            Thread.sleep(210);

            long executionCountWhenDeactivated = getCountOfDocuments(client, testSink);

            Thread.sleep(310);

            long lastExecutionCount = getCountOfDocuments(client, testSink);

            Assert.assertEquals(executionCountWhenDeactivated, lastExecutionCount);

            response = rh.executePutRequest("/_signals/tenant/" + tenant + "/_active", "", auth);
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            awaitMinCountOfDocuments(client, testSink, lastExecutionCount + 1);

        } finally {
            rh.executeDeleteRequest(watchPath, auth);
        }
    }

    @Test
    public void testDeactivateGlobally() throws Exception {
        Header auth = basicAuth("uhura", "uhura");
        String tenant = "_main";
        String watchId = "deactivate_globally_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;
        String testSink = "testsink_" + watchId;

        try (Client client = cluster.getInternalClient()) {

            client.admin().indices().create(new CreateIndexRequest(testSink)).actionGet();

            Watch watch = new WatchBuilder(watchId).atMsInterval(100).search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index(testSink).throttledFor("0").name("testsink").build();
            HttpResponse response = rh.executePutRequest(watchPath, watch.toJson(), auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            awaitMinCountOfDocuments(client, testSink, 1);

            response = rh.executeDeleteRequest("/_signals/admin/_active", auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            Thread.sleep(210);

            long executionCountWhenDeactivated = getCountOfDocuments(client, testSink);

            Thread.sleep(310);

            long lastExecutionCount = getCountOfDocuments(client, testSink);

            Assert.assertEquals(executionCountWhenDeactivated, lastExecutionCount);

            response = rh.executePutRequest("/_signals/admin/_active", "", auth);
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            awaitMinCountOfDocuments(client, testSink, lastExecutionCount + 1);

        } finally {
            rh.executeDeleteRequest(watchPath, auth);
        }
    }

    @Test
    //FLAKY
    public void testAckWatch() throws Exception {
        Header auth = basicAuth("uhura", "uhura");
        String tenant = "_main";
        String watchId = "ack_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalClient()) {
            client.admin().indices().create(new CreateIndexRequest("testsource_ack_watch")).actionGet();
            client.admin().indices().create(new CreateIndexRequest("testsink_ack_watch")).actionGet();

            Watch watch = new WatchBuilder(watchId).atMsInterval(100).search("testsource_ack_watch").query("{\"match_all\" : {} }").as("testsearch")
                    .checkCondition("data.testsearch.hits.hits.length > 0").then().index("testsink_ack_watch").refreshPolicy(RefreshPolicy.IMMEDIATE)
                    .throttledFor("0").name("testaction").build();
            HttpResponse response = rh.executePutRequest(watchPath, watch.toJson(), auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            Thread.sleep(220);

            response = rh.executePutRequest(watchPath + "/_ack/testaction", "", auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_PRECONDITION_FAILED, response.getStatusCode());

            client.index(new IndexRequest("testsource_ack_watch").id("1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "key1",
                    "val1", "key2", "val2")).actionGet();

            awaitMinCountOfDocuments(client, "testsink_ack_watch", 1);

            response = rh.executePutRequest(watchPath + "/_ack/testaction", "", auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            Thread.sleep(100);

            response = rh.executeGetRequest(watchPath + "/_state", auth);
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            System.out.println(response.getBody());
            JsonNode statusDoc = DefaultObjectMapper.readTree(response.getBody());
            Assert.assertEquals(response.getBody(), "uhura", statusDoc.at("/actions/testaction/acked/by").textValue());

            Thread.sleep(200);

            long executionCountAfterAck = getCountOfDocuments(client, "testsink_ack_watch");

            Thread.sleep(310);

            long currentExecutionCount = getCountOfDocuments(client, "testsink_ack_watch");

            Assert.assertEquals(executionCountAfterAck, currentExecutionCount);

            // Make condition go away

            client.delete(new DeleteRequest("testsource_ack_watch", "1").setRefreshPolicy(RefreshPolicy.IMMEDIATE)).actionGet();

            Thread.sleep(310);

            currentExecutionCount = getCountOfDocuments(client, "testsink_ack_watch");

            Assert.assertEquals(executionCountAfterAck, currentExecutionCount);

            response = rh.executeGetRequest(watchPath + "/_state", auth);

            System.out.println(response.getBody());

            statusDoc = DefaultObjectMapper.readTree(response.getBody());
            Assert.assertFalse(response.getBody(), statusDoc.get("actions").get("testaction").hasNonNull("acked"));

            // Create condition again

            client.index(new IndexRequest("testsource_ack_watch").id("2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "key1",
                    "val1", "key2", "val2")).actionGet();

            //Test is here FLAKY
            awaitMinCountOfDocuments(client, "testsink_ack_watch", executionCountAfterAck + 1);

            currentExecutionCount = getCountOfDocuments(client, "testsink_ack_watch");

            Assert.assertNotEquals(executionCountAfterAck, currentExecutionCount);

            response = rh.executeDeleteRequest(watchPath + "/_active", auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());
        } finally {
            rh.executeDeleteRequest(watchPath, auth);
        }
    }

    @Test
    public void testSearchWatch() throws Exception {
        Header auth = basicAuth("uhura", "uhura");
        String tenant = "_main";
        String watchId = "search_watch";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalClient()) {

            Watch watch = new WatchBuilder("put_test").cronTrigger("0 0 1 * * ?").search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .then().index("testsink_search_watch").name("testsink").build();
            HttpResponse response = rh.executePutRequest(watchPath + "1", watch.toJson(), auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            watch = new WatchBuilder("put_test").cronTrigger("0 0 1 * * ?").search("testsource").query("{\"match_all\" : {} }").as("findme").then()
                    .index("testsink_search_watch").name("testsink").build();
            response = rh.executePutRequest(watchPath + "2", watch.toJson(), auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            watch = new WatchBuilder("put_test").cronTrigger("0 0 1 * * ?").search("testsource").query("{\"match_all\" : {} }").as("findme").then()
                    .index("testsink_search_watch").name("testsink").build();
            response = rh.executePutRequest(watchPath + "3", watch.toJson(), auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = rh.executePostRequest("/_signals/watch/" + tenant + "/_search", "{ \"query\": {\"match\": {\"checks.name\": \"findme\"}}}",
                    auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            Assert.assertTrue(response.getBody(), response.getBody().contains("\"hits\":{\"total\":{\"value\":2,\"relation\":\"eq\"}"));

        } finally {
            rh.executeDeleteRequest(watchPath + "1", auth);
            rh.executeDeleteRequest(watchPath + "2", auth);
            rh.executeDeleteRequest(watchPath + "3", auth);
        }
    }

    @Test
    public void testSearchWatchWithoutBody() throws Exception {
        Header auth = basicAuth("uhura", "uhura");
        String tenant = "unit_test_search_watch_without_body";
        String watchId = "search_watch_without_body";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalClient()) {

            Watch watch = new WatchBuilder("put_test").cronTrigger("0 0 1 * * ?").search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .then().index("testsink_search_watch").name("testsink").build();
            HttpResponse response = rh.executePutRequest(watchPath + "1", watch.toJson(), auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            watch = new WatchBuilder("put_test").cronTrigger("0 0 1 * * ?").search("testsource").query("{\"match_all\" : {} }").as("findme").then()
                    .index("testsink_search_watch").name("testsink").build();
            response = rh.executePutRequest(watchPath + "2", watch.toJson(), auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            watch = new WatchBuilder("put_test").cronTrigger("0 0 1 * * ?").search("testsource").query("{\"match_all\" : {} }").as("findme").then()
                    .index("testsink_search_watch").name("testsink").build();
            response = rh.executePutRequest(watchPath + "3", watch.toJson(), auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = rh.executeGetRequest("/_signals/watch/" + tenant + "/_search", auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            Assert.assertTrue(response.getBody(), response.getBody().contains("\"hits\":{\"total\":{\"value\":3,\"relation\":\"eq\"}"));

        } finally {
            rh.executeDeleteRequest(watchPath + "1", auth);
            rh.executeDeleteRequest(watchPath + "2", auth);
            rh.executeDeleteRequest(watchPath + "3", auth);
        }
    }

    @Test
    public void testSearchWatchScroll() throws Exception {
        Header auth = basicAuth("uhura", "uhura");
        String tenant = "_main";
        String watchId = "search_watch_scroll";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalClient()) {

            Watch watch = new WatchBuilder("put_test").cronTrigger("0 0 1 * * ?").search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .then().index("testsink_search_watch").name("testsink").build();
            HttpResponse response = rh.executePutRequest(watchPath + "1", watch.toJson(), auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            watch = new WatchBuilder("put_test").cronTrigger("0 0 1 * * ?").search("testsource").query("{\"match_all\" : {} }").as("findme").then()
                    .index("testsink_search_watch").name("testsink").build();
            response = rh.executePutRequest(watchPath + "2", watch.toJson(), auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            watch = new WatchBuilder("put_test").cronTrigger("0 0 1 * * ?").search("testsource").query("{\"match_all\" : {} }").as("findme").then()
                    .index("testsink_search_watch").name("testsink").build();
            response = rh.executePutRequest(watchPath + "3", watch.toJson(), auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = rh.executePostRequest("/_signals/watch/" + tenant + "/_search?scroll=60s&size=1",
                    "{ \"sort\": [{\"_meta.last_edit.date\": {\"order\": \"asc\"}}], \"query\": {\"match\": {\"checks.name\": \"findme\"}}}", auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            Assert.assertTrue(response.getBody(), response.getBody().contains("\"_id\":\"_main/search_watch_scroll2\""));

            JsonNode responseJsonNode = DefaultObjectMapper.readTree(response.getBody());

            String scrollId = responseJsonNode.get("_scroll_id").asText(null);

            Assert.assertNotNull(scrollId);

            response = rh.executePostRequest("/_search/scroll", "{ \"scroll\": \"60s\", \"scroll_id\": \"" + scrollId + "\"}", auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            Assert.assertTrue(response.getBody(), response.getBody().contains("\"_id\":\"_main/search_watch_scroll3\""));

        } finally {
            rh.executeDeleteRequest(watchPath + "1", auth);
            rh.executeDeleteRequest(watchPath + "2", auth);
            rh.executeDeleteRequest(watchPath + "3", auth);
        }
    }

    @Test
    public void testEmailDestination() throws Exception {
        Header auth = basicAuth("uhura", "uhura");
        String tenant = "_main";
        String watchId = "smtp_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        final int smtpPort = SocketUtils.findAvailableTcpPort();

        GreenMail greenMail = new GreenMail(new ServerSetup(smtpPort, "127.0.0.1", ServerSetup.PROTOCOL_SMTP));
        greenMail.start();

        try {
            EmailAccount destination = new EmailAccount();
            destination.setHost("localhost");
            destination.setPort(smtpPort);

            Assert.assertTrue(destination.toJson().contains("\"type\":\"email\""));
            Assert.assertFalse(destination.toJson().contains("session_timeout"));

            Attachment attachment = new EmailAction.Attachment();
            attachment.setType("context_data");

            //Add smtp destination
            HttpResponse response = rh.executePutRequest("/_signals/account/email/default", destination.toJson(), auth);
            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            //Update test
            response = rh.executePutRequest("/_signals/account/email/default", destination.toJson(), auth);
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            //Delete non existing destination
            response = rh.executeDeleteRequest("/_signals/account/email/aaa", auth);
            Assert.assertEquals(response.getBody(), HttpStatus.SC_NOT_FOUND, response.getStatusCode());

            //Get non existing destination
            response = rh.executeGetRequest("/_signals/account/email/aaabbb", auth);
            Assert.assertEquals(response.getBody(), HttpStatus.SC_NOT_FOUND, response.getStatusCode());

            //Get existing destination
            response = rh.executeGetRequest("/_signals/account/email/default", auth);
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            //Define a watch with an smtp action
            Watch watch = new WatchBuilder("smtp_test").cronTrigger("* * * * * ?").search("testsource").query("{\"match_all\" : {} }")
                    .as("testsearch").then().email("Test Mail Subject").to("mustache@cc.xx").from("mustache@df.xx").account("default")
                    .body("We searched {{data.testsearch._shards.total}} shards").attach("attachment.txt", attachment).name("testsmtpsink").build();

            response = rh.executePutRequest(watchPath, watch.toJson(), auth);
            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            //we expect one email to be sent (rest is throttled)
            if (!greenMail.waitForIncomingEmail(20000, 1)) {
                Assert.fail("Timeout waiting for mails");
            }

            String message = GreenMailUtil.getWholeMessage(greenMail.getReceivedMessages()[0]);

            //Check mail to contain resolved subject line
            Assert.assertTrue(message, message.contains("We searched 5 shards"));
            Assert.assertTrue(message, message.contains("Test Mail Subject"));

        } finally {
            rh.executeDeleteRequest(watchPath, auth);
            rh.executeDeleteRequest("/_signals/account/email/default", auth);
            greenMail.stop();
        }

    }

    @Test
    public void testSlackDestination() throws Exception {
        Header auth = basicAuth("uhura", "uhura");
        String tenant = "_main";
        String watchId = "slack_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try {
            SlackAccount destination = new SlackAccount();
            destination.setUrl(new URI("https://hooks.slack.com/services/SECRET"));

            Assert.assertTrue(destination.toJson().contains("\"type\":\"slack\""));

            SlackActionConf slackActionConf = new SlackActionConf();
            slackActionConf.setText("Test from slack action");
            //slackActionConf.setChannel("");
            slackActionConf.setFrom("xyz");
            slackActionConf.setIconEmoji(":got:");
            slackActionConf.setAccount("default");

            //Add destination
            HttpResponse response = rh.executePutRequest("/_signals/account/slack/default", destination.toJson(), auth);
            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            //Define a watch with an smtp action
            Watch watch = new WatchBuilder("slack_test").cronTrigger("* * * * * ?").search("testsource").query("{\"match_all\" : {} }")
                    .as("testsearch").then().slack(slackActionConf).name("testslacksink").build();

            response = rh.executePutRequest(watchPath, watch.toJson(), auth);
            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

        } finally {
            rh.executeDeleteRequest(watchPath, basicAuth("uhura", "uhura"));
            rh.executeDeleteRequest("/_signals/account/slack/default", basicAuth("uhura", "uhura"));
        }

    }

    @Test
    public void testDeleteAccountInUse() throws Exception {
        Header auth = basicAuth("uhura", "uhura");
        String tenant = "_main";
        String watchId = "slack_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try {
            SlackAccount destination = new SlackAccount();
            destination.setUrl(new URI("https://hooks.slack.com/services/SECRET"));

            SlackActionConf slackActionConf = new SlackActionConf();
            slackActionConf.setText("Test from slack action");
            slackActionConf.setAccount("test");

            HttpResponse response = rh.executePutRequest("/_signals/account/slack/test", destination.toJson(), auth);
            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            Watch watch = new WatchBuilder(watchId).cronTrigger("* * * * * ?").search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .then().slack(slackActionConf).name("testslacksink").build();

            response = rh.executePutRequest(watchPath, watch.toJson(), auth);
            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = rh.executeDeleteRequest("/_signals/account/slack/test", auth);
            Assert.assertEquals(response.getBody(), HttpStatus.SC_CONFLICT, response.getStatusCode());

            response = rh.executeDeleteRequest(watchPath, auth);
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            response = rh.executeDeleteRequest("/_signals/account/slack/test", auth);
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());
        } finally {
            rh.executeDeleteRequest(watchPath, auth);
            rh.executeDeleteRequest("/_signals/account/slack/test", auth);
        }

    }

    @Test
    public void testDeleteAccountInUseFromNonDefaultTenant() throws Exception {
        Header accountAuth = basicAuth("uhura", "uhura");

        Header auth = basicAuth("redshirt3", "redshirt");
        String tenant = "redshirt_club";
        String watchId = "slack_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try {
            SlackAccount destination = new SlackAccount();
            destination.setUrl(new URI("https://hooks.slack.com/services/SECRET"));

            SlackActionConf slackActionConf = new SlackActionConf();
            slackActionConf.setText("Test from slack action");
            slackActionConf.setAccount("test");

            HttpResponse response = rh.executePutRequest("/_signals/account/slack/test", destination.toJson(), accountAuth);
            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            Watch watch = new WatchBuilder(watchId).cronTrigger("* * * * * ?").search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .then().slack(slackActionConf).name("testslacksink").build();

            response = rh.executePutRequest(watchPath, watch.toJson(), auth);
            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = rh.executeDeleteRequest("/_signals/account/slack/test", accountAuth);
            Assert.assertEquals(response.getBody(), HttpStatus.SC_CONFLICT, response.getStatusCode());

            response = rh.executeDeleteRequest(watchPath, auth);
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            response = rh.executeDeleteRequest("/_signals/account/slack/test", accountAuth);
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());
        } finally {
            rh.executeDeleteRequest(watchPath, auth);
            rh.executeDeleteRequest("/_signals/account/slack/test", accountAuth);
        }

    }

    @Test
    public void testPutWeeklySchedule() throws Exception {
        Header auth = basicAuth("uhura", "uhura");
        String tenant = "_main";
        String watchId = "test_weekly_schedule";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalClient()) {

            Watch watch = new WatchBuilder("test").weekly(DayOfWeek.MONDAY, DayOfWeek.WEDNESDAY, new TimeOfDay(12, 0), new TimeOfDay(18, 0))
                    .search("testsource").query("{\"match_all\" : {} }").as("testsearch").put("{\"bla\": {\"blub\": 42}}").as("teststatic").then()
                    .index("testsink").name("testsink").build();

            System.out.println(watch.toJson());
            HttpResponse response = rh.executePutRequest(watchPath, watch.toJson(), auth);
            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = rh.executeGetRequest(watchPath, auth);
            System.out.println(response.getBody());
            // TODO

        } finally {
            rh.executeDeleteRequest(watchPath, auth);
        }
    }

    @Test
    public void testPutExponentialThrottling() throws Exception {
        Header auth = basicAuth("uhura", "uhura");
        String tenant = "_main";
        String watchId = "test_exponential_throttling";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalClient()) {

            Watch watch = new WatchBuilder("test").atMsInterval(1000).search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("testsink").throttledFor("1s**1.5|20s").name("testsink").build();

            System.out.println(watch.toJson());
            HttpResponse response = rh.executePutRequest(watchPath, watch.toJson(), auth);
            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = rh.executeGetRequest(watchPath, auth);
            System.out.println(response.getBody());
            // TODO

        } finally {
            rh.executeDeleteRequest(watchPath, auth);
        }
    }

    @Test
    public void testSearchDestinationScroll() throws Exception {
        Header auth = basicAuth("uhura", "uhura");
        String destinationId = "search_destination_scroll";
        String destinationPath = "/_signals/account/slack/" + destinationId;

        try (Client client = cluster.getInternalClient()) {

            SlackAccount slackDestination = new SlackAccount();
            slackDestination.setUrl(new URI("https://xyz.test.com"));

            HttpResponse response = rh.executePutRequest(destinationPath + "1", slackDestination.toJson(), auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            slackDestination.setUrl(new URI("https://abc.test.com"));
            response = rh.executePutRequest(destinationPath + "2", slackDestination.toJson(), auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            slackDestination = new SlackAccount();
            slackDestination.setUrl(new URI("https://abcdef.test.com"));

            response = rh.executePutRequest(destinationPath + "3", slackDestination.toJson(), auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = rh.executePostRequest("/_signals/destination/_search?scroll=60s&size=1",
                    "{ \"sort\": [{\"type.keyword\": {\"order\": \"asc\"}}], \"query\": {\"match\": {\"type\": \"SLACK\"}}}", auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            Assert.assertTrue(response.getBody(), response.getBody().contains("slack"));

            JsonNode responseJsonNode = DefaultObjectMapper.readTree(response.getBody());

            String scrollId = responseJsonNode.get("_scroll_id").asText(null);

            Assert.assertNotNull(scrollId);

            response = rh.executePostRequest("/_search/scroll", "{ \"scroll\": \"60s\", \"scroll_id\": \"" + scrollId + "\"}", auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            Assert.assertTrue(response.getBody(), response.getBody().contains("slack"));

        } finally {
            rh.executeDeleteRequest(destinationPath + "1", auth);
            rh.executeDeleteRequest(destinationPath + "2", auth);
            rh.executeDeleteRequest(destinationPath + "3", auth);
        }
    }

    @Test
    public void testConvEs() throws Exception {
        Header auth = basicAuth("uhura", "uhura");

        try (Client client = cluster.getInternalClient()) {

            String input = new JsonBuilder.Object()
                    .attr("trigger",
                            new JsonBuilder.Object().attr("schedule",
                                    new JsonBuilder.Object().attr("daily", new JsonBuilder.Object().attr("at", "noon"))))
                    .attr("input", new JsonBuilder.Object().attr("simple", new JsonBuilder.Object().attr("x", "y")))
                    .attr("actions",
                            new JsonBuilder.Object()
                                    .attr("my_action",
                                            new JsonBuilder.Object().attr("email",
                                                    new JsonBuilder.Object().attr("to", "horst@horst").attr("subject", "Hello World")
                                                            .attr("body", "Hallo {{ctx.payload.x}}").attr("attachments", "foo")))
                                    .attr("another_action",
                                            new JsonBuilder.Object().attr("index",
                                                    new JsonBuilder.Object().attr("index", "foo").attr("execution_time_field", "holla"))))
                    .toJsonString();

            HttpResponse response = rh.executePostRequest("/_signals/convert/es", input, auth);
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            System.out.println(response.getBody());

        }
    }

    private long getCountOfDocuments(Client client, String index) throws InterruptedException, ExecutionException {
        SearchRequest request = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        request.source(searchSourceBuilder);

        SearchResponse response = client.search(request).get();

        return response.getHits().getTotalHits().value;
    }

    private long awaitMinCountOfDocuments(Client client, String index, long minCount) throws Exception {
        long start = System.currentTimeMillis();

        for (int i = 0; i < 1000; i++) {
            Thread.sleep(10);
            long count = getCountOfDocuments(client, index);

            if (count >= minCount) {
                log.info("Found " + count + " documents in " + index + " after " + (System.currentTimeMillis() - start) + " ms");

                return count;
            }
        }

        Assert.fail("Did not find " + minCount + " documents in " + index + " after " + (System.currentTimeMillis() - start) + " ms");

        return 0;
    }

    private WatchLog getMostRecentWatchLog(Client client, String tenantName, String watchName) {
        try {
            SearchResponse searchResponse = client.search(new SearchRequest("signals_" + tenantName + "_log").source(
                    new SearchSourceBuilder().size(1).sort("execution_end", SortOrder.DESC).query(new MatchQueryBuilder("watch_id", watchName))))
                    .actionGet();

            if (searchResponse.getHits().getHits().length == 0) {
                return null;
            }

            SearchHit searchHit = searchResponse.getHits().getHits()[0];

            return WatchLog.parse(searchHit.getId(), searchHit.getSourceAsString());
        } catch (org.elasticsearch.index.IndexNotFoundException | SearchPhaseExecutionException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Error in getMostRecenWatchLog(" + tenantName + ", " + watchName + ")", e);
        }
    }

    private WatchLog awaitWatchLog(Client client, String tenantName, String watchName) throws Exception {
        try {
            long start = System.currentTimeMillis();
            Exception indexNotFoundException = null;

            for (int i = 0; i < 1000; i++) {
                Thread.sleep(10);

                try {
                    WatchLog watchLog = getMostRecentWatchLog(client, tenantName, watchName);

                    if (watchLog != null) {
                        log.info("Found " + watchLog + " for " + watchName + " after " + (System.currentTimeMillis() - start) + " ms");

                        return watchLog;
                    }

                    indexNotFoundException = null;

                } catch (org.elasticsearch.index.IndexNotFoundException | SearchPhaseExecutionException e) {
                    indexNotFoundException = e;
                    continue;
                }
            }

            if (indexNotFoundException != null) {
                Assert.fail("Did not find watch log index for " + watchName + " after " + (System.currentTimeMillis() - start) + " ms: "
                        + indexNotFoundException);
            } else {
                SearchResponse searchResponse = client
                        .search(new SearchRequest("signals_" + tenantName + "_log")
                                .source(new SearchSourceBuilder().sort("execution_end", SortOrder.DESC).query(new MatchAllQueryBuilder())))
                        .actionGet();

                log.info("Did not find watch log for " + watchName + " after " + (System.currentTimeMillis() - start) + " ms\n\n"
                        + searchResponse.getHits());

                Assert.fail("Did not find watch log for " + watchName + " after " + (System.currentTimeMillis() - start) + " ms");
            }
            return null;
        } catch (Exception e) {
            log.error("Exception in awaitWatchLog for " + watchName + ")", e);
            throw new RuntimeException("Exception in awaitWatchLog for " + watchName + ")", e);
        }
    }

    private Watch getWatchByRest(String tenant, String id, Header... header) throws Exception {

        HttpResponse response = rh.executeGetRequest("/_signals/watch/" + tenant + "/" + id, header);

        Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

        return Watch.parseFromElasticDocument(new WatchInitializationService(null, scriptService), "test", id, response.getBody(), -1);
    }

    private static Header basicAuth(String username, String password) {
        return new BasicHeader("Authorization",
                "Basic " + Base64.getEncoder().encodeToString((username + ":" + Objects.requireNonNull(password)).getBytes(StandardCharsets.UTF_8)));
    }
}
