/*
 * Copyright 2022 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.enterprise.femt;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.Role;
import com.floragunn.searchguard.authz.RoleBasedActionAuthorization;
import com.floragunn.searchguard.authz.actions.Actions;
import com.floragunn.searchguard.sgconf.ActionGroups;
import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;
import com.floragunn.searchguard.user.User;

public class PrivilegesInterceptorImplTest {

    private static final ActionGroups emptyActionGroups = new ActionGroups(SgDynamicConfiguration.empty());
    private static final Actions actions = new Actions(null);

    @Test
    public void wildcardTenantMapping() throws Exception {
        SgDynamicConfiguration<Role> roles = SgDynamicConfiguration
                .fromMap(
                        DocNode.of("all_access",
                                DocNode.of("tenant_permissions",
                                        Arrays.asList(
                                                ImmutableMap.of("tenant_patterns", Arrays.asList("*"), "allowed_actions", Arrays.asList("*"))))),
                        CType.ROLES, -1, -1, -1, null);

        ImmutableSet<String> tenants = ImmutableSet.of("my_tenant", "test");

        RoleBasedActionAuthorization actionAuthorization = new RoleBasedActionAuthorization(roles, emptyActionGroups, actions, null, tenants);
        PrivilegesInterceptorImpl subject = new PrivilegesInterceptorImpl(FeMultiTenancyConfig.DEFAULT, tenants, actions);

        User user = User.forUser("test").searchGuardRoles("all_access").build();

        Assert.assertEquals(ImmutableMap.of("test", true, "my_tenant", true),
                subject.mapTenants(user, ImmutableSet.of("all_access"), actionAuthorization));
    }

}
