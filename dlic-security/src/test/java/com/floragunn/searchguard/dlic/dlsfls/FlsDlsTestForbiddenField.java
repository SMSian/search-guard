/*
 * Copyright 2016-2017 by floragunn GmbH - All rights reserved
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

import org.apache.http.HttpStatus;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest.RefreshPolicy;
import org.opensearch.client.transport.TransportClient;
import org.opensearch.common.xcontent.XContentType;
import org.junit.Assert;
import org.junit.Test;

import com.floragunn.searchguard.test.helper.rest.RestHelper.HttpResponse;

public class FlsDlsTestForbiddenField extends AbstractDlsFlsTest{
    
    @Override
    protected void populateData(TransportClient tc) {


                
        tc.index(new IndexRequest("deals").type("deals").id("0").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{\"customer\": {\"name\":\"cust1\"}, \"zip\": \"12345\",\"secret\": \"tellnoone\",\"amount\": 10}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("deals").type("deals").id("1").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{\"customer\": {\"name\":\"cust2\", \"ctype\":\"industry\"}, \"amount\": 1500}", XContentType.JSON)).actionGet();

    }
    
    @Test
    public void testDlsAggregations() throws Exception {
        
        setup();
        
        
        String query = "{"+
            "\"query\" : {"+
                 "\"match_all\": {}"+
            "},"+
            "\"aggs\" : {"+
                "\"thesum\" : { \"sum\" : { \"field\" : \"amount\" } }"+
            "}"+
        "}";
        
        HttpResponse res;
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executePostRequest("/deals/_search?pretty", query, encodeBasicHeader("dept_manager_fls_dls", "password"))).getStatusCode());
        Assert.assertTrue(res.getBody().contains("\"value\" : 0,\n      \"relation"));
        Assert.assertTrue(res.getBody().contains("\"value\" : 0"));
        Assert.assertTrue(res.getBody().contains("\"failed\" : 0"));
        
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executePostRequest("/deals/_search?pretty", query, encodeBasicHeader("admin", "admin"))).getStatusCode());
        Assert.assertTrue(res.getBody().contains("\"value\" : 2,\n      \"relation"));
        Assert.assertTrue(res.getBody().contains("\"value\" : 1510.0"));
        Assert.assertTrue(res.getBody().contains("\"failed\" : 0"));
    }
    
    @Test
    public void testDls() throws Exception {
        
        setup();
        
        HttpResponse res;
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals/_search?pretty&size=0", encodeBasicHeader("dept_manager_fls_dls", "password"))).getStatusCode());
        Assert.assertTrue(res.getBody().contains("\"value\" : 0,\n      \"relation"));
        Assert.assertTrue(res.getBody().contains("\"failed\" : 0"));
        
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals/_search?pretty&size=0", encodeBasicHeader("admin", "admin"))).getStatusCode());
        Assert.assertTrue(res.getBody().contains("\"value\" : 2,\n      \"relation"));
        Assert.assertTrue(res.getBody().contains("\"failed\" : 0"));
        
        
        String query =
                
            "{"+
                "\"query\": {"+
                   "\"range\" : {"+
                      "\"amount\" : {"+
                           "\"gte\" : 8,"+
                            "\"lte\" : 20,"+
                            "\"boost\" : 3.0"+
                        "}"+
                    "}"+
                "}"+
            "}";
        
        
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executePostRequest("/deals/_search?pretty", query,encodeBasicHeader("dept_manager_fls_dls", "password"))).getStatusCode());
        Assert.assertTrue(res.getBody().contains("\"value\" : 0,\n      \"relation"));
        Assert.assertTrue(res.getBody().contains("\"failed\" : 0"));
        
        query =
                
                "{"+
                    "\"query\": {"+
                       "\"range\" : {"+
                          "\"amount\" : {"+
                               "\"gte\" : 100,"+
                                "\"lte\" : 2000,"+
                                "\"boost\" : 2.0"+
                            "}"+
                        "}"+
                    "}"+
                "}";
            
            
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executePostRequest("/deals/_search?pretty", query,encodeBasicHeader("dept_manager_fls_dls", "password"))).getStatusCode());
        Assert.assertTrue(res.getBody().contains("\"value\" : 0,\n      \"relation"));
        Assert.assertTrue(res.getBody().contains("\"failed\" : 0"));      
        
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executePostRequest("/deals/_search?pretty", query,encodeBasicHeader("admin", "admin"))).getStatusCode());
        Assert.assertTrue(res.getBody().contains("\"value\" : 1,\n      \"relation"));
        Assert.assertTrue(res.getBody().contains("\"failed\" : 0"));  
        
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals/_search?q=amount:10&pretty", encodeBasicHeader("dept_manager_fls_dls", "password"))).getStatusCode());
        Assert.assertTrue(res.getBody().contains("\"value\" : 0,\n      \"relation"));
        Assert.assertTrue(res.getBody().contains("\"failed\" : 0"));
        
        res = rh.executeGetRequest("/deals/deals/0?pretty", encodeBasicHeader("dept_manager_fls_dls", "password"));
        Assert.assertTrue(res.getBody().contains("\"found\" : false"));
        
        res = rh.executeGetRequest("/deals/deals/0?realtime=true&pretty", encodeBasicHeader("dept_manager_fls_dls", "password"));
        Assert.assertTrue(res.getBody().contains("\"found\" : false"));
        
        res = rh.executeGetRequest("/deals/deals/1?pretty", encodeBasicHeader("admin", "admin"));
        Assert.assertTrue(res.getBody().contains("\"found\" : true"));
        
        res = rh.executeGetRequest("/deals/deals/1?pretty", encodeBasicHeader("dept_manager_fls_dls", "password"));
        Assert.assertTrue(res.getBody().contains("\"found\" : false"));
     
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals/_count?pretty", encodeBasicHeader("admin", "admin"))).getStatusCode());
        Assert.assertTrue(res.getBody().contains("\"count\" : 2,"));
        Assert.assertTrue(res.getBody().contains("\"failed\" : 0"));  
        
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals/_count?pretty", encodeBasicHeader("dept_manager_fls_dls", "password"))).getStatusCode());
        Assert.assertTrue(res.getBody().contains("\"count\" : 0,"));
        Assert.assertTrue(res.getBody().contains("\"failed\" : 0"));  
    }
    
    @Test
    public void testCombined() throws Exception {
        
        setup();
        
        HttpResponse res;
        
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals/_search?pretty", encodeBasicHeader("user_combined", "password"))).getStatusCode());
        System.out.println(res.getBody());
        Assert.assertTrue(res.getBody().contains("\"value\" : 1,\n      \"relation"));
        Assert.assertTrue(res.getBody().contains("\"failed\" : 0"));
        Assert.assertTrue(res.getBody().contains("customer"));
        Assert.assertTrue(res.getBody().contains("industry"));
        Assert.assertFalse(res.getBody().contains("zip"));
        Assert.assertFalse(res.getBody().contains("cust1"));
    }
}