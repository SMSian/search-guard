/*
 * Copyright 2015-2022 floragunn GmbH
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */

package com.floragunn.searchguard.legacy;

import org.apache.http.HttpStatus;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest.RefreshPolicy;
import org.opensearch.client.Client;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.junit.Assert;
import org.junit.Test;

import com.floragunn.searchguard.legacy.test.DynamicSgConfig;
import com.floragunn.searchguard.legacy.test.RestHelper;
import com.floragunn.searchguard.legacy.test.RestHelper.HttpResponse;
import com.floragunn.searchguard.legacy.test.SingleClusterTest;
import com.floragunn.searchguard.support.ConfigConstants;

public class NoDnfofTests extends SingleClusterTest {

    // Moved over from MultitenancyTests because this really has nothing to do with multi tenancy
    
    @Test
    public void testNoDnfof() throws Exception {

        final Settings settings = Settings.builder().put(ConfigConstants.SEARCHGUARD_ROLES_MAPPING_RESOLUTION, "BOTH").build();

        setup(Settings.EMPTY, new DynamicSgConfig().setSgConfig("sg_config_nodnfof.yml"), settings);
        final RestHelper rh = nonSslRestHelper();

        try (Client tc = getPrivilegedInternalNodeClient()) {
            tc.admin().indices().create(new CreateIndexRequest("copysf")).actionGet();

            tc.index(new IndexRequest("indexa").id("0").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":\"indexa\"}",
                    XContentType.JSON)).actionGet();
            tc.index(new IndexRequest("indexb").id("0").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":\"indexb\"}",
                    XContentType.JSON)).actionGet();

            tc.index(new IndexRequest("vulcangov").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}",
                    XContentType.JSON)).actionGet();
            tc.index(new IndexRequest("starfleet").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}",
                    XContentType.JSON)).actionGet();
            tc.index(new IndexRequest("starfleet_academy").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}",
                    XContentType.JSON)).actionGet();
            tc.index(new IndexRequest("starfleet_library").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}",
                    XContentType.JSON)).actionGet();
            tc.index(new IndexRequest("klingonempire").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}",
                    XContentType.JSON)).actionGet();
            tc.index(
                    new IndexRequest("public").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON))
                    .actionGet();

            tc.index(new IndexRequest("spock").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON))
                    .actionGet();
            tc.index(new IndexRequest("kirk").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON))
                    .actionGet();
            tc.index(new IndexRequest("role01_role02").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}",
                    XContentType.JSON)).actionGet();

            tc.admin().indices()
                    .aliases(new IndicesAliasesRequest()
                            .addAliasAction(AliasActions.add().indices("starfleet", "starfleet_academy", "starfleet_library").alias("sf")))
                    .actionGet();
            tc.admin().indices()
                    .aliases(new IndicesAliasesRequest().addAliasAction(AliasActions.add().indices("klingonempire", "vulcangov").alias("nonsf")))
                    .actionGet();
            tc.admin().indices().aliases(new IndicesAliasesRequest().addAliasAction(AliasActions.add().indices("public").alias("unrestricted")))
                    .actionGet();

        }

        HttpResponse resc;
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN,
                (resc = rh.executeGetRequest("indexa,indexb/_search?pretty", encodeBasicHeader("user_a", "user_a"))).getStatusCode());
        System.out.println(resc.getBody());

        Assert.assertEquals(HttpStatus.SC_FORBIDDEN,
                (resc = rh.executeGetRequest("indexa,indexb/_search?pretty", encodeBasicHeader("user_b", "user_b"))).getStatusCode());
        System.out.println(resc.getBody());

        String msearchBody = "{\"index\":\"indexa\", \"ignore_unavailable\": true}" + System.lineSeparator()
                + "{\"size\":10, \"query\":{\"bool\":{\"must\":{\"match_all\":{}}}}}" + System.lineSeparator()
                + "{\"index\":\"indexb\", \"ignore_unavailable\": true}" + System.lineSeparator()
                + "{\"size\":10, \"query\":{\"bool\":{\"must\":{\"match_all\":{}}}}}" + System.lineSeparator();
        System.out.println("#### msearch a");
        resc = rh.executePostRequest("_msearch?pretty", msearchBody, encodeBasicHeader("user_a", "user_a"));
        Assert.assertEquals(200, resc.getStatusCode());
        System.out.println(resc.getBody());
        Assert.assertTrue(resc.getBody(), resc.getBody().contains("indexa"));
       // Assert.assertFalse(resc.getBody(), resc.getBody().contains("indexb"));
        Assert.assertTrue(resc.getBody(), resc.getBody().contains("exception"));
        Assert.assertTrue(resc.getBody(), resc.getBody().contains("permission"));

        System.out.println("#### msearch b");
        resc = rh.executePostRequest("_msearch?pretty", msearchBody, encodeBasicHeader("user_b", "user_b"));
        Assert.assertEquals(200, resc.getStatusCode());
        System.out.println(resc.getBody());
        // Assert.assertFalse(resc.getBody(), resc.getBody().contains("indexa"));
        Assert.assertTrue(resc.getBody(), resc.getBody().contains("indexb"));
        Assert.assertTrue(resc.getBody(), resc.getBody().contains("exception"));
        Assert.assertTrue(resc.getBody(), resc.getBody().contains("permission"));

        msearchBody = "{\"index\":\"indexc\",  \"ignore_unavailable\": false}" + System.lineSeparator()
                + "{\"size\":10, \"query\":{\"bool\":{\"must\":{\"match_all\":{}}}}}" + System.lineSeparator()
                + "{\"index\":\"indexd\", \"ignore_unavailable\": false}" + System.lineSeparator()
                + "{\"size\":10, \"query\":{\"bool\":{\"must\":{\"match_all\":{}}}}}" + System.lineSeparator();

        System.out.println("#### msearch b2");
        resc = rh.executePostRequest("_msearch?pretty", msearchBody, encodeBasicHeader("user_b", "user_b"));
        System.out.println(resc.getBody());
        Assert.assertEquals(200, resc.getStatusCode());
        //Assert.assertFalse(resc.getBody(), resc.getBody().contains("indexc"));
        //Assert.assertFalse(resc.getBody(), resc.getBody().contains("indexd"));
        Assert.assertTrue(resc.getBody(), resc.getBody().contains("exception"));
        Assert.assertTrue(resc.getBody(), resc.getBody().contains("permission"));
        int count = resc.getBody().split("\"status\" : 403").length;
        Assert.assertEquals(3, count);

        String mgetBody = "{" + "\"docs\" : [" + "{" + "\"_index\" : \"indexa\"," + "\"_id\" : \"0\"" + " }," + " {"
                + "\"_index\" : \"indexb\"," + " \"_id\" : \"0\"" + "}" + "]" + "}";

        resc = rh.executePostRequest("_mget?pretty", mgetBody, encodeBasicHeader("user_b", "user_b"));
        Assert.assertEquals(200, resc.getStatusCode());
        Assert.assertFalse(resc.getBody(), resc.getBody().contains("\"content\" : \"indexa\""));
        Assert.assertTrue(resc.getBody(), resc.getBody().contains("indexb"));
        Assert.assertTrue(resc.getBody(), resc.getBody().contains("exception"));
        Assert.assertTrue(resc.getBody(), resc.getBody().contains("permission"));

        mgetBody = "{" + "\"docs\" : [" + "{" + "\"_index\" : \"indexx\"," + "\"_id\" : \"0\"" + " }," + " {"
                + "\"_index\" : \"indexy\"," + " \"_id\" : \"0\"" + "}" + "]" + "}";

        resc = rh.executePostRequest("_mget?pretty", mgetBody, encodeBasicHeader("user_b", "user_b"));
        Assert.assertEquals(200, resc.getStatusCode());
        Assert.assertTrue(resc.getBody(), resc.getBody().contains("exception"));
        count = resc.getBody().split("root_cause").length;
        Assert.assertEquals(3, count);

        Assert.assertEquals(HttpStatus.SC_FORBIDDEN,
                (resc = rh.executeGetRequest("_search?pretty", encodeBasicHeader("user_a", "user_a"))).getStatusCode());

        Assert.assertEquals(HttpStatus.SC_FORBIDDEN,
                (resc = rh.executeGetRequest("index*/_search?pretty", encodeBasicHeader("user_a", "user_a"))).getStatusCode());

        Assert.assertEquals(HttpStatus.SC_OK,
                (resc = rh.executeGetRequest("indexa/_search?pretty", encodeBasicHeader("user_a", "user_a"))).getStatusCode());
        System.out.println(resc.getBody());

        Assert.assertEquals(HttpStatus.SC_FORBIDDEN,
                (resc = rh.executeGetRequest("indexb/_search?pretty", encodeBasicHeader("user_a", "user_a"))).getStatusCode());
        System.out.println(resc.getBody());

        Assert.assertEquals(HttpStatus.SC_FORBIDDEN,
                (resc = rh.executeGetRequest("*/_search?pretty", encodeBasicHeader("user_a", "user_a"))).getStatusCode());
        System.out.println(resc.getBody());

        Assert.assertEquals(HttpStatus.SC_FORBIDDEN,
                (resc = rh.executeGetRequest("_all/_search?pretty", encodeBasicHeader("user_a", "user_a"))).getStatusCode());
        System.out.println(resc.getBody());

        Assert.assertEquals(HttpStatus.SC_FORBIDDEN,
                (resc = rh.executeGetRequest("notexists/_search?pretty", encodeBasicHeader("user_a", "user_a"))).getStatusCode());
        System.out.println(resc.getBody());

        Assert.assertEquals(HttpStatus.SC_NOT_FOUND,
                (resc = rh.executeGetRequest("indexanbh,indexabb*/_search?pretty", encodeBasicHeader("user_a", "user_a"))).getStatusCode());
        System.out.println(resc.getBody());

        Assert.assertEquals(HttpStatus.SC_FORBIDDEN,
                (resc = rh.executeGetRequest("starfleet/_search?pretty", encodeBasicHeader("user_a", "user_a"))).getStatusCode());
        System.out.println(resc.getBody());


    }
}
