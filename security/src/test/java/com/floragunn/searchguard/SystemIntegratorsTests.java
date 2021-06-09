/*
 * Copyright 2015-2018 floragunn GmbH
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

package com.floragunn.searchguard;

import org.apache.http.HttpStatus;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.common.settings.Settings;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.test.SingleClusterTest;
import com.floragunn.searchguard.test.helper.cluster.ClusterConfiguration;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.rest.RestHelper;
import com.floragunn.searchguard.test.helper.rest.RestHelper.HttpResponse;
import com.google.common.collect.Lists;

public class SystemIntegratorsTests extends SingleClusterTest {
    
    @ClassRule 
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();
    
    @Test
    public void testInjectedUserMalformed() throws Exception {
    
        final Settings settings = Settings.builder()                
                .put(ConfigConstants.SEARCHGUARD_UNSUPPORTED_INJECT_USER_ENABLED, true)
                .put("http.type", "com.floragunn.searchguard.http.UserInjectingServerTransport")
                .build();
                      
        setup(settings, ClusterConfiguration.USERINJECTOR);
        
        final RestHelper rh = nonSslRestHelper();
        // username|role1,role2|remoteIP|attributes
        
        HttpResponse resc;
        
        resc = rh.executeGetRequest("_searchguard/authinfo", new BasicHeader(ConfigConstants.SG_INJECTED_USER, null));
        Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, resc.getStatusCode());

        resc = rh.executeGetRequest("_searchguard/authinfo", new BasicHeader(ConfigConstants.SG_INJECTED_USER, "|||"));
        Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, resc.getStatusCode());
        
        resc = rh.executeGetRequest("_searchguard/authinfo", new BasicHeader(ConfigConstants.SG_INJECTED_USER, "||127.0.0:80|"));
        Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, resc.getStatusCode());
        
        resc = rh.executeGetRequest("_searchguard/authinfo", new BasicHeader(ConfigConstants.SG_INJECTED_USER, "username||ip|"));
        Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, resc.getStatusCode());

        resc = rh.executeGetRequest("_searchguard/authinfo", new BasicHeader(ConfigConstants.SG_INJECTED_USER, "username||ip:port|"));
        Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, resc.getStatusCode());

        resc = rh.executeGetRequest("_searchguard/authinfo", new BasicHeader(ConfigConstants.SG_INJECTED_USER, "username||ip:80|"));
        Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, resc.getStatusCode());

        resc = rh.executeGetRequest("_searchguard/authinfo", new BasicHeader(ConfigConstants.SG_INJECTED_USER, "username||127.0.x:80|"));
        Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, resc.getStatusCode());

        resc = rh.executeGetRequest("_searchguard/authinfo", new BasicHeader(ConfigConstants.SG_INJECTED_USER, "username||127.0.0:80|key1,value1,key2"));
        Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, resc.getStatusCode());

        resc = rh.executeGetRequest("_searchguard/authinfo", new BasicHeader(ConfigConstants.SG_INJECTED_USER, "||127.0.0:80|key1,value1,key2,value2"));
        Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, resc.getStatusCode());
        
    }

    @Test
    public void testInjectedUser() throws Exception {
    
        final Settings settings = Settings.builder()                
                .put(ConfigConstants.SEARCHGUARD_UNSUPPORTED_INJECT_USER_ENABLED, true)
                .put("http.type", "com.floragunn.searchguard.http.UserInjectingServerTransport")
                .build();
                      
        setup(settings, ClusterConfiguration.USERINJECTOR);
        
        final RestHelper rh = nonSslRestHelper();
        // username|role1,role2|remoteIP|attributes
        
        HttpResponse resc;
               
        resc = rh.executeGetRequest("_searchguard/authinfo", new BasicHeader(ConfigConstants.SG_INJECTED_USER, "admin||127.0.0:80|"));
        Assert.assertEquals(HttpStatus.SC_OK, resc.getStatusCode());
        Assert.assertTrue(resc.getBody(), resc.getBody().contains("\"User admin <ext>\""));
        Assert.assertTrue(resc.getBody(), resc.getBody().contains("\"remote_address\":\"127.0.0.0:80\""));
        Assert.assertTrue(resc.getBody(), resc.getBody().contains("\"backend_roles\":[]"));
        Assert.assertTrue(resc.getBody(), resc.getBody().contains("\"custom_attribute_names\":[]"));

        resc = rh.executeGetRequest("_searchguard/authinfo", new BasicHeader(ConfigConstants.SG_INJECTED_USER, "admin|role1|127.0.0:80|key1,value1"));
        Assert.assertEquals(HttpStatus.SC_OK, resc.getStatusCode());
        Assert.assertTrue(resc.getBody(), resc.getBody().contains("\"User admin <ext> [backend_roles=[role1]]\""));
        Assert.assertTrue(resc.getBody().contains("\"remote_address\":\"127.0.0.0:80\""));
        Assert.assertTrue(resc.getBody().contains("\"backend_roles\":[\"role1\"]"));
        Assert.assertTrue(resc.getBody().contains("\"custom_attribute_names\":[\"key1\"]"));

        resc = rh.executeGetRequest("_searchguard/authinfo", new BasicHeader(ConfigConstants.SG_INJECTED_USER, "admin|role1,role2||key1,value1"));
        Assert.assertEquals(HttpStatus.SC_OK, resc.getStatusCode());
        Assert.assertTrue(resc.getBody(), resc.getBody().contains("\"User admin <ext> [backend_roles=[role1, role2]]\""));
        // remote IP is assigned by XFFResolver
        Assert.assertFalse(resc.getBody().contains("\"remote_address\":null"));
        Assert.assertTrue(resc.getBody().contains("\"backend_roles\":[\"role1\",\"role2\"]"));
        Assert.assertTrue(resc.getBody().contains("\"custom_attribute_names\":[\"key1\"]"));

        resc = rh.executeGetRequest("_searchguard/authinfo", new BasicHeader(ConfigConstants.SG_INJECTED_USER, "admin|role1,role2|8.8.8.8:8|key1,value1,key2,value2"));
        Assert.assertEquals(HttpStatus.SC_OK, resc.getStatusCode());
        Assert.assertTrue(resc.getBody(), resc.getBody().contains("\"User admin <ext> [backend_roles=[role1, role2]]\""));
        // remote IP is assigned by XFFResolver
        Assert.assertFalse(resc.getBody().contains("\"remote_address\":null"));
        Assert.assertTrue(resc.getBody().contains("\"backend_roles\":[\"role1\",\"role2\"]"));
        Assert.assertTrue(resc.getBody().contains("\"custom_attribute_names\":[\"key1\",\"key2\"]"));

        resc = rh.executeGetRequest("_searchguard/authinfo", new BasicHeader(ConfigConstants.SG_INJECTED_USER, "nagilum|role1,role2|8.8.8.8:8|key1,value1,key2,value2"));
        Assert.assertEquals(HttpStatus.SC_OK, resc.getStatusCode());
        Assert.assertTrue(resc.getBody(), resc.getBody().contains("\"User nagilum <ext> [backend_roles=[role1, role2]]\""));
        // remote IP is assigned by XFFResolver
        Assert.assertTrue(resc.getBody().contains("\"remote_address\":\"8.8.8.8:8\""));
        Assert.assertTrue(resc.getBody().contains("\"backend_roles\":[\"role1\",\"role2\"]"));
        // mapped by username
        Assert.assertTrue(resc.getBody().contains("\"sg_roles\":[\"sg_all_access\""));
        Assert.assertTrue(resc.getBody().contains("\"custom_attribute_names\":[\"key1\",\"key2\"]"));
        
        resc = rh.executeGetRequest("_searchguard/authinfo", new BasicHeader(ConfigConstants.SG_INJECTED_USER, "myuser|role1,vulcanadmin|8.8.8.8:8|key1,value1,key2,value2"));
        Assert.assertEquals(HttpStatus.SC_OK, resc.getStatusCode());
        Assert.assertTrue(resc.getBody(), resc.getBody().contains("\"User myuser <ext> [backend_roles=[role1, vulcanadmin]]\""));        
        // remote IP is assigned by XFFResolver
        Assert.assertTrue(resc.getBody().contains("\"remote_address\":\"8.8.8.8:8\""));
        Assert.assertTrue(resc.getBody().contains("\"backend_roles\":[\"role1\",\"vulcanadmin\"]"));
        // mapped by backend role "twitter"
        Assert.assertTrue(resc.getBody(), resc.getBody().contains("\"sg_roles\":[\"sg_public\",\"sg_role_vulcans_admin\"]"));
        Assert.assertTrue(resc.getBody().contains("\"custom_attribute_names\":[\"key1\",\"key2\"]"));
        
        // add requested tenant
        resc = rh.executeGetRequest("_searchguard/authinfo", new BasicHeader(ConfigConstants.SG_INJECTED_USER, "myuser|role1,vulcanadmin|8.8.8.8:8|key1,value1,key2,value2|"));
        Assert.assertEquals(HttpStatus.SC_OK, resc.getStatusCode());
        Assert.assertTrue(resc.getBody(), resc.getBody().contains("\"User myuser <ext> [backend_roles=[role1, vulcanadmin]]\""));                
        // remote IP is assigned by XFFResolver
        Assert.assertTrue(resc.getBody().contains("\"remote_address\":\"8.8.8.8:8\""));
        Assert.assertTrue(resc.getBody().contains("\"backend_roles\":[\"role1\",\"vulcanadmin\"]"));
        // mapped by backend role "twitter"
        Assert.assertTrue(resc.getBody().contains("\"sg_roles\":[\"sg_public\",\"sg_role_vulcans_admin\"]"));
        Assert.assertTrue(resc.getBody().contains("\"custom_attribute_names\":[\"key1\",\"key2\"]"));

        resc = rh.executeGetRequest("_searchguard/authinfo", new BasicHeader(ConfigConstants.SG_INJECTED_USER, "myuser|role1,vulcanadmin|8.8.8.8:8|key1,value1,key2,value2|mytenant"));
        Assert.assertEquals(HttpStatus.SC_OK, resc.getStatusCode());
        Assert.assertTrue(resc.getBody(), resc.getBody().contains("\"User myuser <ext> [backend_roles=[role1, vulcanadmin] requestedTenant=mytenant]\""));                
        // remote IP is assigned by XFFResolver
        Assert.assertTrue(resc.getBody().contains("\"remote_address\":\"8.8.8.8:8\""));
        Assert.assertTrue(resc.getBody().contains("\"backend_roles\":[\"role1\",\"vulcanadmin\"]"));
        // mapped by backend role "twitter"
        Assert.assertTrue(resc.getBody().contains("\"sg_roles\":[\"sg_public\",\"sg_role_vulcans_admin\"]"));
        Assert.assertTrue(resc.getBody().contains("\"custom_attribute_names\":[\"key1\",\"key2\"]"));

        resc = rh.executeGetRequest("_searchguard/authinfo", new BasicHeader(ConfigConstants.SG_INJECTED_USER, "myuser|role1,vulcanadmin|8.8.8.8:8||mytenant with whitespace"));
        Assert.assertEquals(HttpStatus.SC_OK, resc.getStatusCode());
        Assert.assertTrue(resc.getBody(), resc.getBody().contains("\"User myuser <ext> [backend_roles=[role1, vulcanadmin] requestedTenant=mytenant with whitespace]\""));                
        // remote IP is assigned by XFFResolver
        Assert.assertTrue(resc.getBody().contains("\"remote_address\":\"8.8.8.8:8\""));
        Assert.assertTrue(resc.getBody().contains("\"backend_roles\":[\"role1\",\"vulcanadmin\"]"));
        // mapped by backend role "twitter"
        Assert.assertTrue(resc.getBody().contains("\"sg_roles\":[\"sg_public\",\"sg_role_vulcans_admin\"]"));
        

    }    

    @Test
    public void testInjectedUserDisabled() throws Exception {
    
        final Settings settings = Settings.builder()                
                .put("http.type", "com.floragunn.searchguard.http.UserInjectingServerTransport")
                .build();
                      
        setup(settings, ClusterConfiguration.USERINJECTOR);
        
        final RestHelper rh = nonSslRestHelper();
        // username|role1,role2|remoteIP|attributes
        
        HttpResponse resc;
               
        resc = rh.executeGetRequest("_searchguard/authinfo", new BasicHeader(ConfigConstants.SG_INJECTED_USER, "admin|role1|127.0.0:80|key1,value1"));
        Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, resc.getStatusCode());
    }

  @Test
  public void testInjectedAdminUser() throws Exception {
  
      final Settings settings = Settings.builder()                
              .put(ConfigConstants.SEARCHGUARD_UNSUPPORTED_INJECT_USER_ENABLED, true)
              .put(ConfigConstants.SEARCHGUARD_UNSUPPORTED_INJECT_ADMIN_USER_ENABLED, true)
              .putList(ConfigConstants.SEARCHGUARD_AUTHCZ_ADMIN_DN, Lists.newArrayList("CN=kirk,OU=client,O=client,L=Test,C=DE","injectedadmin"))
              .put("http.type", "com.floragunn.searchguard.http.UserInjectingServerTransport")
              .build();
                    
      setup(settings, ClusterConfiguration.USERINJECTOR);
      
      final RestHelper rh = nonSslRestHelper();
      HttpResponse resc;
      
      // injected user is admin, access to SG index must be allowed
      resc = rh.executeGetRequest("searchguard/_search?pretty", new BasicHeader(ConfigConstants.SG_INJECTED_USER, "injectedadmin|role1|127.0.0:80|key1,value1"));
      Assert.assertEquals(resc.getBody(), HttpStatus.SC_OK, resc.getStatusCode());
      Assert.assertTrue(resc.getBody().contains("\"_id\" : \"config\""));
      Assert.assertTrue(resc.getBody().contains("\"_id\" : \"roles\""));
      Assert.assertTrue(resc.getBody().contains("\"_id\" : \"internalusers\""));
      Assert.assertTrue(resc.getBody().contains("\"_id\" : \"tattr\""));
      Assert.assertTrue(resc.getBody(), resc.getBody().contains("\"value\" : 8"));
      
      resc = rh.executeGetRequest("searchguard/_search?pretty", new BasicHeader(ConfigConstants.SG_INJECTED_USER, "wrongadmin|role1|127.0.0:80|key1,value1"));
      Assert.assertEquals(HttpStatus.SC_FORBIDDEN, resc.getStatusCode());
      
  }

    @Test
    public void testInjectedAdminUserAdminInjectionDisabled() throws Exception {
    
        final Settings settings = Settings.builder()                
                .put(ConfigConstants.SEARCHGUARD_UNSUPPORTED_INJECT_USER_ENABLED, true)
                .putList(ConfigConstants.SEARCHGUARD_AUTHCZ_ADMIN_DN, Lists.newArrayList("CN=kirk,OU=client,O=client,L=Test,C=DE","injectedadmin"))
                .put("http.type", "com.floragunn.searchguard.http.UserInjectingServerTransport")
                .build();
                      
        setup(settings, ClusterConfiguration.USERINJECTOR);
        
        final RestHelper rh = nonSslRestHelper();
        HttpResponse resc;
        
        // injected user is admin, access to SG index must be allowed
        resc = rh.executeGetRequest("searchguard/_search?pretty", new BasicHeader(ConfigConstants.SG_INJECTED_USER, "injectedadmin|role1|127.0.0:80|key1,value1"));
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, resc.getStatusCode());
        Assert.assertFalse(resc.getBody().contains("\"_id\" : \"config\""));
        Assert.assertFalse(resc.getBody().contains("\"_id\" : \"roles\""));
        Assert.assertFalse(resc.getBody().contains("\"_id\" : \"internalusers\""));
        Assert.assertFalse(resc.getBody().contains("\"_id\" : \"tattr\""));
        Assert.assertFalse(resc.getBody().contains("\"total\" : 6"));
                
    }    

}
