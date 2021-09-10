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

package com.floragunn.searchguard.sgconf;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.metadata.IndexAbstraction.Type;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.TransportAddress;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

import com.floragunn.searchguard.auth.blocking.ClientBlockRegistry;
import com.floragunn.searchguard.resolver.IndexResolverReplacer.Resolved;
import com.floragunn.searchguard.sgconf.SgRoles.TenantPermissions;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;
import com.floragunn.searchguard.sgconf.impl.v6.ActionGroupsV6;
import com.floragunn.searchguard.sgconf.impl.v6.RoleMappingsV6;
import com.floragunn.searchguard.sgconf.impl.v6.RoleV6;
import com.floragunn.searchguard.sgconf.impl.v6.RoleV6.Index;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.WildcardMatcher;
import com.floragunn.searchguard.user.User;
import com.google.common.base.Joiner;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Sets;

import inet.ipaddr.IPAddress;

public class ConfigModelV6 extends ConfigModel {

    protected final Logger log = LogManager.getLogger(this.getClass());
    private ConfigConstants.RolesMappingResolution rolesMappingResolution;
    private ActionGroupResolver agr;
    private SgRoles sgRoles;
    private RoleMappingHolder roleMappingHolder;
    private SgDynamicConfiguration<RoleV6> roles;

    public ConfigModelV6(SgDynamicConfiguration<RoleV6> roles, SgDynamicConfiguration<ActionGroupsV6> actiongroups,
            SgDynamicConfiguration<RoleMappingsV6> rolesmapping, DynamicConfigModel dcm, Settings esSettings) {

        this.roles = roles;

        try {
            rolesMappingResolution = ConfigConstants.RolesMappingResolution.valueOf(esSettings
                    .get(ConfigConstants.SEARCHGUARD_ROLES_MAPPING_RESOLUTION, ConfigConstants.RolesMappingResolution.MAPPING_ONLY.toString())
                    .toUpperCase());
        } catch (Exception e) {
            log.error("Cannot apply roles mapping resolution", e);
            rolesMappingResolution = ConfigConstants.RolesMappingResolution.MAPPING_ONLY;
        }

        agr = reloadActionGroups(actiongroups);
        sgRoles = reload(roles);
        roleMappingHolder = new RoleMappingHolder(rolesmapping, dcm.getHostsResolverMode());
    }

    public Set<String> getAllConfiguredTenantNames() {
        final Set<String> configuredTenants = new HashSet<>();
        for (Entry<String, RoleV6> sgRole : roles.getCEntries().entrySet()) {
            Map<String, String> tenants = sgRole.getValue().getTenants();

            if (tenants != null) {
                configuredTenants.addAll(tenants.keySet());
            }

        }

        return Collections.unmodifiableSet(configuredTenants);
    }

    public SgRoles getSgRoles() {
        return sgRoles;
    }
    
    public ActionGroupResolver getActionGroupResolver() {
        return agr;
    }

    @Override
    public List<ClientBlockRegistry<InetAddress>> getBlockIpAddresses() {
        return Collections.emptyList();
    }

    @Override
    public List<ClientBlockRegistry<String>> getBlockedUsers() {
        return Collections.emptyList();
    }

    @Override
    public List<ClientBlockRegistry<IPAddress>> getBlockedNetmasks() {
        return Collections.emptyList();
    }
    
    @Override
    public boolean isTenantValid(String requestedTenant) {
        // We don't have a list of all tenants in V6
        return true;
    }

    private ActionGroupResolver reloadActionGroups(SgDynamicConfiguration<ActionGroupsV6> actionGroups) {
        return new ActionGroupResolver() {

            private Set<String> getGroupMembers(final String groupname) {

                if (actionGroups == null) {
                    return Collections.emptySet();
                }

                return Collections.unmodifiableSet(resolve(actionGroups, groupname));
            }

            private Set<String> resolve(final SgDynamicConfiguration<?> actionGroups, final String entry) {

                // SG5 format, plain array
                //List<String> en = actionGroups.getAsList(DotPath.of(entry));
                //if (en.isEmpty()) {
                // try SG6 format including readonly and permissions key
                //  en = actionGroups.getAsList(DotPath.of(entry + "." + ConfigConstants.CONFIGKEY_ACTION_GROUPS_PERMISSIONS));
                //}

                if (!actionGroups.getCEntries().containsKey(entry)) {
                    return Collections.emptySet();
                }

                final Set<String> ret = new HashSet<String>();

                final Object actionGroupAsObject = actionGroups.getCEntries().get(entry);

                if (actionGroupAsObject != null && actionGroupAsObject instanceof List) {

                    for (final String perm : ((List<String>) actionGroupAsObject)) {
                        if (actionGroups.getCEntries().keySet().contains(perm)) {
                            ret.addAll(resolve(actionGroups, perm));
                        } else {
                            ret.add(perm);
                        }
                    }

                } else if (actionGroupAsObject != null && actionGroupAsObject instanceof ActionGroupsV6) {
                    for (final String perm : ((ActionGroupsV6) actionGroupAsObject).getPermissions()) {
                        if (actionGroups.getCEntries().keySet().contains(perm)) {
                            ret.addAll(resolve(actionGroups, perm));
                        } else {
                            ret.add(perm);
                        }
                    }
                } else {
                    throw new RuntimeException("Unable to handle " + actionGroupAsObject);
                }

                return Collections.unmodifiableSet(ret);
            }

            @Override
            public Set<String> resolvedActions(final List<String> actions) {
                if (actions == null || actions.size() == 0) {
                    return Collections.emptySet();
                }
                
                final Set<String> resolvedActions = new HashSet<String>();
                for (String string : actions) {
                    final Set<String> groups = getGroupMembers(string);
                    if (groups.isEmpty()) {
                        resolvedActions.add(string);
                    } else {
                        resolvedActions.addAll(groups);
                    }
                }

                return Collections.unmodifiableSet(resolvedActions);
            }
        };
    }

    private SgRoles reload(SgDynamicConfiguration<RoleV6> settings) {

        final Set<Future<SgRole>> futures = new HashSet<>(5000);
        final ExecutorService execs = Executors.newFixedThreadPool(10);

        for (Entry<String, RoleV6> sgRole : settings.getCEntries().entrySet()) {

            Future<SgRole> future = execs.submit(new Callable<SgRole>() {

                @Override
                public SgRole call() throws Exception {
                    SgRole _sgRole = new SgRole(sgRole.getKey());

                    if (sgRole.getValue() == null) {
                        return null;
                    }

                    final Set<String> permittedClusterActions = agr.resolvedActions(sgRole.getValue().getCluster());
                    _sgRole.addClusterPerms(permittedClusterActions);

                    //if(tenants != null) {
                    for (Entry<String, String> tenant : sgRole.getValue().getTenants().entrySet()) {

                        //if(tenant.equals(user.getName())) {
                        //    continue;
                        //}

                        if ("RW".equalsIgnoreCase(tenant.getValue())) {
                            _sgRole.addTenant(new Tenant(tenant.getKey(), true));
                        } else {
                            _sgRole.addTenant(new Tenant(tenant.getKey(), false));
                            //if(_sgRole.tenants.stream().filter(t->t.tenant.equals(tenant)).count() > 0) { //RW outperforms RO
                            //    _sgRole.addTenant(new Tenant(tenant, false));
                            //}
                        }
                    }
                    //}

                    //final Map<String, DynamicConfiguration> permittedAliasesIndices = sgRoleSettings.getGroups(DotPath.of("indices"));

                    for (final Entry<String, Index> permittedAliasesIndex : sgRole.getValue().getIndices().entrySet()) {

                        //final String resolvedRole = sgRole;
                        //final String indexPattern = permittedAliasesIndex;

                        final String dls = permittedAliasesIndex.getValue().get_dls_();
                        final List<String> fls = permittedAliasesIndex.getValue().get_fls_();
                        final List<String> maskedFields = permittedAliasesIndex.getValue().get_masked_fields_();

                        IndexPattern _indexPattern = new IndexPattern(permittedAliasesIndex.getKey());
                        _indexPattern.setDlsQuery(dls);
                        _indexPattern.addFlsFields(fls);
                        _indexPattern.addMaskedFields(maskedFields);

                        for (Entry<String, List<String>> type : permittedAliasesIndex.getValue().getTypes().entrySet()) {
                            TypePerm typePerm = new TypePerm(type.getKey());
                            final List<String> perms = type.getValue();
                            typePerm.addPerms(agr.resolvedActions(perms));
                            _indexPattern.addTypePerms(typePerm);
                        }

                        _sgRole.addIndexPattern(_indexPattern);
                        
                        if ("*".equals(permittedAliasesIndex.getKey())) {
                            // Map legacy config for actions which should have been marked as cluster permissions but were treated as index permissions
                            // These will be always configured for a * index pattern; other index patterns would not have worked for these actions in previous SG versions.

                            for (TypePerm typePerm : _indexPattern.getTypePerms()) {
                                if ("*".equals(typePerm.typePattern)) {
                                    if (typePerm.getPerms().contains("indices:data/read/search/template")) {
                                        _sgRole.clusterPerms.add("indices:data/read/search/template");
                                    }

                                    if (typePerm.getPerms().contains("indices:data/read/msearch/template")) {
                                        _sgRole.clusterPerms.add("indices:data/read/msearch/template");
                                    }
                                }
                            }
                        }


                    }

                    return _sgRole;
                }
            });

            futures.add(future);
        }

        execs.shutdown();
        try {
            execs.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Thread interrupted (1) while loading roles");
            return null;
        }

        try {
            SgRoles _sgRoles = new SgRoles(futures.size());
            for (Future<SgRole> future : futures) {
                _sgRoles.addSgRole(future.get());
            }

            return _sgRoles;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Thread interrupted (2) while loading roles");
            return null;
        } catch (ExecutionException e) {
            log.error("Error while updating roles: {}", e.getCause(), e.getCause());
            throw ExceptionsHelper.convertToOpenSearchException(e);
        }
    }

    //beans

    public static class SgRoles extends com.floragunn.searchguard.sgconf.SgRoles implements ToXContentObject {

        protected final Logger log = LogManager.getLogger(this.getClass());

        final Map<String, SgRole> roles;

        private SgRoles(int roleCount) {
            roles = new HashMap<>(roleCount);
        }

        private SgRoles addSgRole(SgRole sgRole) {
            if (sgRole != null) {
                this.roles.put(sgRole.getName(), sgRole);
            }
            return this;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((roles == null) ? 0 : roles.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            SgRoles other = (SgRoles) obj;
            if (roles == null) {
                if (other.roles != null)
                    return false;
            } else if (!roles.equals(other.roles))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return "roles=" + roles;
        }

        public Collection<SgRole> getRoles() {
            return Collections.unmodifiableCollection(roles.values());
        }

        public Set<String> getRoleNames() {
            return Collections.unmodifiableSet(roles.keySet());
        }

        public SgRoles filter(Set<String> keep) {
            final SgRoles retVal = new SgRoles(roles.size());
            for (SgRole sgr : roles.values()) {
                if (keep.contains(sgr.getName())) {
                    retVal.addSgRole(sgr);
                }
            }
            return retVal;
        }


        @Override
        public EvaluatedDlsFlsConfig getDlsFls(User user, IndexNameExpressionResolver resolver, ClusterService cs,
                NamedXContentRegistry namedXContentRegistry) {

            final Map<String, Set<String>> dlsQueries = new HashMap<String, Set<String>>();
            final Map<String, Set<String>> flsFields = new HashMap<String, Set<String>>();
            final Map<String, Set<String>> maskedFieldsMap = new HashMap<String, Set<String>>();

            for (SgRole sgr : roles.values()) {
                for (IndexPattern ip : sgr.getIpatterns()) {
                    final Set<String> fls = ip.getFls();
                    final String dls = ip.getDlsQuery(user);
                    final String indexPattern = ip.getUnresolvedIndexPattern(user);
                    final Set<String> maskedFields = ip.getMaskedFields();
                    String[] concreteIndices = new String[0];

                    if ((dls != null && dls.length() > 0) || (fls != null && fls.size() > 0) || (maskedFields != null && maskedFields.size() > 0)) {
                        concreteIndices = ip.getResolvedIndexPattern(user, resolver, cs);
                    }

                    if (dls != null && dls.length() > 0) {

                        if (dlsQueries.containsKey(indexPattern)) {
                            dlsQueries.get(indexPattern).add(dls);
                        } else {
                            dlsQueries.put(indexPattern, new HashSet<String>());
                            dlsQueries.get(indexPattern).add(dls);
                        }

                        for (int i = 0; i < concreteIndices.length; i++) {
                            final String ci = concreteIndices[i];
                            if (dlsQueries.containsKey(ci)) {
                                dlsQueries.get(ci).add(dls);
                            } else {
                                dlsQueries.put(ci, new HashSet<String>());
                                dlsQueries.get(ci).add(dls);
                            }
                        }

                    }

                    if (fls != null && fls.size() > 0) {

                        if (flsFields.containsKey(indexPattern)) {
                            flsFields.get(indexPattern).addAll(Sets.newHashSet(fls));
                        } else {
                            flsFields.put(indexPattern, new HashSet<String>());
                            flsFields.get(indexPattern).addAll(Sets.newHashSet(fls));
                        }

                        for (int i = 0; i < concreteIndices.length; i++) {
                            final String ci = concreteIndices[i];
                            if (flsFields.containsKey(ci)) {
                                flsFields.get(ci).addAll(Sets.newHashSet(fls));
                            } else {
                                flsFields.put(ci, new HashSet<String>());
                                flsFields.get(ci).addAll(Sets.newHashSet(fls));
                            }
                        }
                    }
                    
                    if (maskedFields != null && maskedFields.size() > 0) {

                        if (maskedFieldsMap.containsKey(indexPattern)) {
                            maskedFieldsMap.get(indexPattern).addAll(Sets.newHashSet(maskedFields));
                        } else {
                            maskedFieldsMap.put(indexPattern, new HashSet<String>());
                            maskedFieldsMap.get(indexPattern).addAll(Sets.newHashSet(maskedFields));
                        }

                        for (int i = 0; i < concreteIndices.length; i++) {
                            final String ci = concreteIndices[i];
                            if (maskedFieldsMap.containsKey(ci)) {
                                maskedFieldsMap.get(ci).addAll(Sets.newHashSet(maskedFields));
                            } else {
                                maskedFieldsMap.put(ci, new HashSet<String>());
                                maskedFieldsMap.get(ci).addAll(Sets.newHashSet(maskedFields));
                            }
                        }
                    }
                }
            }
            
            return new EvaluatedDlsFlsConfig(dlsQueries, flsFields, maskedFieldsMap);
        }

        //kibana special only, terms eval
        public Set<String> getAllPermittedIndicesForKibana(Resolved resolved, User user, Set<String> actions, IndexNameExpressionResolver resolver,
                ClusterService cs) {
            Set<String> retVal = new HashSet<>();
            for (SgRole sgr : roles.values()) {
                retVal.addAll(sgr.getAllResolvedPermittedIndices(Resolved._LOCAL_ALL, user, actions, resolver, cs));
                retVal.addAll(resolved.getRemoteIndices());
            }
            return Collections.unmodifiableSet(retVal);
        }

        //dnfof only
        public Set<String> reduce(Resolved resolved, User user, Set<String> actions, IndexNameExpressionResolver resolver, ClusterService cs) {
            Set<String> retVal = new HashSet<>();
            for (SgRole sgr : roles.values()) {
                retVal.addAll(sgr.getAllResolvedPermittedIndices(resolved, user, actions, resolver, cs));
            }
            if (log.isDebugEnabled()) {
                log.debug("Reduced requested resolved indices {} to permitted indices {}.", resolved, retVal.toString());
            }
            return Collections.unmodifiableSet(retVal);
        }

        //return true on success
        public boolean get(Resolved resolved, User user, Set<String> actions, IndexNameExpressionResolver resolver, ClusterService cs) {
            for (SgRole sgr : roles.values()) {
                if (ConfigModelV6.impliesTypePerm(sgr.getIpatterns(), resolved, user, actions, resolver, cs)) {
                    return true;
                }
            }
            return false;
        }

        public boolean impliesClusterPermissionPermission(String action) {
            return roles.values().stream().filter(r -> r.impliesClusterPermission(action)).count() > 0;
        }

        //rolespan
        public boolean impliesTypePermGlobal(Resolved resolved, User user, Set<String> actions, IndexNameExpressionResolver resolver,
                ClusterService cs) {
            Set<IndexPattern> ipatterns = new HashSet<ConfigModelV6.IndexPattern>();
            roles.values().stream().forEach(p -> ipatterns.addAll(p.getIpatterns()));
            return ConfigModelV6.impliesTypePerm(ipatterns, resolved, user, actions, resolver, cs);
        }


        @Override
        public Set<String> getClusterPermissions(User user) {
            Set<String> result = new HashSet<>();

            for (SgRole role : roles.values()) {
                result.addAll(role.getClusterPerms());
            }
            
            return result;
        }
        
        @Override
        public TenantPermissions getTenantPermissions(User user, String requestedTenant) {
            if (user == null) {
                return EMPTY_TENANT_PERMISSIONS;
            }
            
            if (USER_TENANT.equals(requestedTenant)) {
                return FULL_TENANT_PERMISSIONS;
            }
            
            boolean read = false;
            boolean write = false;
            
            for (SgRole role : roles.values()) {
                for (Tenant tenant : role.getTenants()) {
                    if (WildcardMatcher.match(tenant.getTenant(), requestedTenant)) {
                        read = true;
                        
                        if (tenant.isReadWrite()) {
                            write = true;
                        }
                    }
                }
            }
            
            // TODO SG8: Remove this

            if ("SGS_GLOBAL_TENANT".equals(requestedTenant) && !read && !write && (roles.containsKey("sg_kibana_user") || roles.containsKey("SGS_KIBANA_USER")
                    || roles.containsKey("sg_all_access") || roles.containsKey("SGS_ALL_ACCESS"))) {
                return FULL_TENANT_PERMISSIONS;
            }
            
            return new TenantPermissionsImpl(read, write);
        }
        
        @Override
        public boolean hasTenantPermission(User user, String requestedTenant, String action) {
            // Not supported for V6 config
            return false;
        }
        
        @Override
        public Map<String, Boolean> mapTenants(User user, Set<String> allTenantNames) {

            if (user == null) {
                return Collections.emptyMap();
            }

            Map<String, Boolean> result = new HashMap<>(roles.size());
            result.put(user.getName(), true);

            for (SgRole role : roles.values()) {
                for (Tenant tenant : role.getTenants()) {
                    String tenantPattern = tenant.getTenant();
                    boolean rw = tenant.isReadWrite();
                    
                    if (rw || !result.containsKey(tenantPattern)) { //RW outperforms RO
                        result.put(tenantPattern, rw);
                    }
                }
            }
            
            // TODO SG8: Remove this
            
            if (!result.containsKey("SGS_GLOBAL_TENANT") && (roles.containsKey("sg_kibana_user") || roles.containsKey("SGS_KIBANA_USER")
                    || roles.containsKey("sg_all_access") || roles.containsKey("SGS_ALL_ACCESS"))) {
                result.put("SGS_GLOBAL_TENANT", true);
            }

            return Collections.unmodifiableMap(result);
        }
        
        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();

            for (SgRole role : roles.values()) {
                builder.field(role.getName(), role);
            }

            builder.endObject();
            return builder;
        }
    }

    public static class SgRole implements ToXContentObject {

        private final String name;
        private final Set<Tenant> tenants = new HashSet<>();
        private final Set<IndexPattern> ipatterns = new HashSet<>();
        private final Set<String> clusterPerms = new HashSet<>();

        private SgRole(String name) {
            super();
            this.name = Objects.requireNonNull(name);
        }

        private boolean impliesClusterPermission(String action) {
            return WildcardMatcher.matchAny(clusterPerms, action);
        }

        //get indices which are permitted for the given types and actions
        //dnfof + kibana special only
        private Set<String> getAllResolvedPermittedIndices(Resolved resolved, User user, Set<String> actions, IndexNameExpressionResolver resolver,
                ClusterService cs) {

            final Set<String> retVal = new HashSet<>();
            for (IndexPattern p : ipatterns) {
                //what if we cannot resolve one (for create purposes)
                boolean patternMatch = false;
                final Set<TypePerm> tperms = p.getTypePerms();
                for (TypePerm tp : tperms) {
                    if (WildcardMatcher.matchAny(tp.typePattern, resolved.getTypes().toArray(new String[0]))) {
                        patternMatch = WildcardMatcher.matchAll(tp.perms.toArray(new String[0]), actions);
                    }
                }
                if (patternMatch) {
                    //resolved but can contain patterns for nonexistent indices
                    final String[] permitted = p.getResolvedIndexPattern(user, resolver, cs); //maybe they do not exist
                    final Set<String> res = new HashSet<>();
                    if (!resolved.isLocalAll() && !resolved.getAllIndicesOrPattern().contains("*") && !resolved.getAllIndicesOrPattern().contains("_all")) {
                        final Set<String> wanted = new HashSet<>(resolved.getAllIndices());
                        //resolved but can contain patterns for nonexistent indices
                        WildcardMatcher.wildcardRetainInSet(wanted, permitted);
                        res.addAll(wanted);
                    } else {
                        //we want all indices so just return what's permitted

                        //#557
                        //final String[] allIndices = resolver.concreteIndexNames(cs.state(), IndicesOptions.lenientExpandOpen(), "*");
                        final String[] allIndices = cs.state().getMetadata().getConcreteAllOpenIndices();
                        final Set<String> wanted = new HashSet<>(Arrays.asList(allIndices));
                        WildcardMatcher.wildcardRetainInSet(wanted, permitted);
                        res.addAll(wanted);
                    }
                    retVal.addAll(res);
                }
            }

            //all that we want and all thats permitted of them
            return Collections.unmodifiableSet(retVal);
        }

        private SgRole addTenant(Tenant tenant) {
            if (tenant != null) {
                this.tenants.add(tenant);
            }
            return this;
        }

        private SgRole addIndexPattern(IndexPattern indexPattern) {
            if (indexPattern != null) {
                this.ipatterns.add(indexPattern);
            }
            return this;
        }

        private SgRole addClusterPerms(Collection<String> clusterPerms) {
            if (clusterPerms != null) {
                this.clusterPerms.addAll(clusterPerms);
            }
            return this;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((clusterPerms == null) ? 0 : clusterPerms.hashCode());
            result = prime * result + ((ipatterns == null) ? 0 : ipatterns.hashCode());
            result = prime * result + ((name == null) ? 0 : name.hashCode());
            result = prime * result + ((tenants == null) ? 0 : tenants.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            SgRole other = (SgRole) obj;
            if (clusterPerms == null) {
                if (other.clusterPerms != null)
                    return false;
            } else if (!clusterPerms.equals(other.clusterPerms))
                return false;
            if (ipatterns == null) {
                if (other.ipatterns != null)
                    return false;
            } else if (!ipatterns.equals(other.ipatterns))
                return false;
            if (name == null) {
                if (other.name != null)
                    return false;
            } else if (!name.equals(other.name))
                return false;
            if (tenants == null) {
                if (other.tenants != null)
                    return false;
            } else if (!tenants.equals(other.tenants))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return System.lineSeparator() + "  " + name + System.lineSeparator() + "    tenants=" + tenants + System.lineSeparator()
                    + "    ipatterns=" + ipatterns + System.lineSeparator() + "    clusterPerms=" + clusterPerms;
        }

        public Set<Tenant> getTenants() {
            return Collections.unmodifiableSet(tenants);
        }

        public Set<IndexPattern> getIpatterns() {
            return Collections.unmodifiableSet(ipatterns);
        }

        public Set<String> getClusterPerms() {
            return Collections.unmodifiableSet(clusterPerms);
        }

        public String getName() {
            return name;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();

            if (this.clusterPerms != null && this.clusterPerms.size() > 0) {
                builder.field("cluster", this.clusterPerms);
            }

            if (ipatterns != null && ipatterns.size() > 0) {
                builder.field("indices");
                builder.startObject();

                for (IndexPattern indexPattern : ipatterns) {
                    builder.field(indexPattern.indexPattern, indexPattern);
                }

                builder.endObject();
            }

            if (tenants != null && tenants.size() > 0) {
                builder.field("tenants");
                builder.startObject();

                for (Tenant tenant : tenants) {
                    builder.field(tenant.tenant, tenant);
                }

                builder.endObject();
            }
            builder.endObject();

            return builder;
        }

    }

    //sg roles
    public static class IndexPattern implements ToXContentObject {
        private final String indexPattern;
        private String dlsQuery;
        private final Set<String> fls = new HashSet<>();
        private final Set<String> maskedFields = new HashSet<>();
        private final Set<TypePerm> typePerms = new HashSet<>();

        public IndexPattern(String indexPattern) {
            super();
            this.indexPattern = Objects.requireNonNull(indexPattern);
        }

        public IndexPattern addFlsFields(List<String> flsFields) {
            if (flsFields != null) {
                this.fls.addAll(flsFields);
            }
            return this;
        }

        public IndexPattern addMaskedFields(List<String> maskedFields) {
            if (maskedFields != null) {
                this.maskedFields.addAll(maskedFields);
            }
            return this;
        }

        public IndexPattern addTypePerms(TypePerm typePerm) {
            if (typePerm != null) {
                this.typePerms.add(typePerm);
            }
            return this;
        }

        public IndexPattern setDlsQuery(String dlsQuery) {
            if (dlsQuery != null) {
                this.dlsQuery = dlsQuery;
            }
            return this;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((dlsQuery == null) ? 0 : dlsQuery.hashCode());
            result = prime * result + ((fls == null) ? 0 : fls.hashCode());
            result = prime * result + ((maskedFields == null) ? 0 : maskedFields.hashCode());
            result = prime * result + ((indexPattern == null) ? 0 : indexPattern.hashCode());
            result = prime * result + ((typePerms == null) ? 0 : typePerms.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            IndexPattern other = (IndexPattern) obj;
            if (dlsQuery == null) {
                if (other.dlsQuery != null)
                    return false;
            } else if (!dlsQuery.equals(other.dlsQuery))
                return false;
            if (fls == null) {
                if (other.fls != null)
                    return false;
            } else if (!fls.equals(other.fls))
                return false;
            if (maskedFields == null) {
                if (other.maskedFields != null)
                    return false;
            } else if (!maskedFields.equals(other.maskedFields))
                return false;
            if (indexPattern == null) {
                if (other.indexPattern != null)
                    return false;
            } else if (!indexPattern.equals(other.indexPattern))
                return false;
            if (typePerms == null) {
                if (other.typePerms != null)
                    return false;
            } else if (!typePerms.equals(other.typePerms))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return System.lineSeparator() + "        indexPattern=" + indexPattern + System.lineSeparator() + "          dlsQuery=" + dlsQuery
                    + System.lineSeparator() + "          fls=" + fls + System.lineSeparator() + "          typePerms=" + typePerms;
        }

        public String getUnresolvedIndexPattern(User user) {
            return replaceProperties(indexPattern, user);
        }

        private String[] getResolvedIndexPattern(User user, IndexNameExpressionResolver resolver, ClusterService cs) {
            String unresolved = getUnresolvedIndexPattern(user);
            String[] resolved = null;
            if (WildcardMatcher.containsWildcard(unresolved)) {
            	final String[] aliasesForPermittedPattern = cs.state().getMetadata().getIndicesLookup().entrySet().stream()
                        .filter(e -> e.getValue().getType().equals(Type.ALIAS)).filter(e -> WildcardMatcher.match(unresolved, e.getKey())).map(e -> e.getKey())
                        .toArray(String[]::new);

                if (aliasesForPermittedPattern != null && aliasesForPermittedPattern.length > 0) {
                    resolved = resolver.concreteIndexNames(cs.state(), IndicesOptions.lenientExpandOpen(), aliasesForPermittedPattern);
                }
            }

            if (resolved == null && !unresolved.isEmpty()) {
                resolved = resolver.concreteIndexNames(cs.state(), IndicesOptions.lenientExpandOpen(), unresolved);
            }
            if (resolved == null || resolved.length == 0) {
                return new String[] { unresolved };
            } else {
                //append unresolved value for pattern matching
                String[] retval = Arrays.copyOf(resolved, resolved.length + 1);
                retval[retval.length - 1] = unresolved;
                return retval;
            }
        }

        public String getDlsQuery(User user) {
            return replaceProperties(dlsQuery, user);
        }

        public Set<String> getFls() {
            return Collections.unmodifiableSet(fls);
        }

        public Set<String> getMaskedFields() {
            return Collections.unmodifiableSet(maskedFields);
        }

        public Set<TypePerm> getTypePerms() {
            return Collections.unmodifiableSet(typePerms);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();

            if (typePerms != null) {
                for (TypePerm typePerm : typePerms) {
                    builder.array(typePerm.typePattern, typePerm);
                }
            }

            if (dlsQuery != null) {
                builder.field("_dls_", dlsQuery);
            }

            if (fls != null && fls.size() > 0) {
                builder.array("_fls_", fls);
            }

            builder.endObject();
            return builder;
        }

    }

    public static class TypePerm implements ToXContentObject {
        private final String typePattern;
        private final Set<String> perms = new HashSet<>();

        private TypePerm(String typePattern) {
            super();
            this.typePattern = Objects.requireNonNull("*");//typePattern
            /*if(IGNORED_TYPES.contains(typePattern)) {
                throw new RuntimeException("typepattern '"+typePattern+"' not allowed");
            }*/
        }

        private TypePerm addPerms(Collection<String> perms) {
            if (perms != null) {
                this.perms.addAll(perms);
            }
            return this;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((perms == null) ? 0 : perms.hashCode());
            result = prime * result + ((typePattern == null) ? 0 : typePattern.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            TypePerm other = (TypePerm) obj;
            if (perms == null) {
                if (other.perms != null)
                    return false;
            } else if (!perms.equals(other.perms))
                return false;
            if (typePattern == null) {
                if (other.typePattern != null)
                    return false;
            } else if (!typePattern.equals(other.typePattern))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return System.lineSeparator() + "             typePattern=" + typePattern + System.lineSeparator() + "             perms=" + perms;
        }

        public String getTypePattern() {
            return typePattern;
        }

        public Set<String> getPerms() {
            return Collections.unmodifiableSet(perms);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startArray();

            for (String perm : perms) {
                builder.value(perm);
            }

            builder.endArray();
            return builder;
        }

    }

    public static class Tenant implements ToXContentObject {
        private final String tenant;
        private final boolean readWrite;

        private Tenant(String tenant, boolean readWrite) {
            super();
            this.tenant = tenant;
            this.readWrite = readWrite;
        }

        public String getTenant() {
            return tenant;
        }

        public boolean isReadWrite() {
            return readWrite;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + (readWrite ? 1231 : 1237);
            result = prime * result + ((tenant == null) ? 0 : tenant.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            Tenant other = (Tenant) obj;
            if (readWrite != other.readWrite)
                return false;
            if (tenant == null) {
                if (other.tenant != null)
                    return false;
            } else if (!tenant.equals(other.tenant))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return System.lineSeparator() + "                tenant=" + tenant + System.lineSeparator() + "                readWrite=" + readWrite;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            // NOT SUPPORTED
            builder.endObject();
            
            return builder;
        }
    }

    private static String replaceProperties(String orig, User user) {

        if (user == null || orig == null) {
            return orig;
        }

        orig = orig.replace("${user.name}", user.getName()).replace("${user_name}", user.getName());
        orig = replaceRoles(orig, user);
        for (Entry<String, String> entry : user.getCustomAttributesMap().entrySet()) {
            if (entry == null || entry.getKey() == null || entry.getValue() == null) {
                continue;
            }
            orig = orig.replace("${" + entry.getKey() + "}", entry.getValue());
            orig = orig.replace("${" + entry.getKey().replace('.', '_') + "}", entry.getValue());
        }
        return orig;
    }

    private static String replaceRoles(final String orig, final User user) {
        String retVal = orig;
        if (orig.contains("${user.roles}") || orig.contains("${user_roles}")) {
            final String commaSeparatedRoles = toQuotedCommaSeparatedString(user.getRoles());
            retVal = orig.replace("${user.roles}", commaSeparatedRoles).replace("${user_roles}", commaSeparatedRoles);
        }
        return retVal;
    }

    private static String toQuotedCommaSeparatedString(final Set<String> roles) {
        return Joiner.on(',').join(Iterables.transform(roles, s -> {
            return new StringBuilder(s.length() + 2).append('"').append(s).append('"').toString();
        }));
    }

    private static boolean impliesTypePerm(Set<IndexPattern> ipatterns, Resolved resolved, User user, Set<String> actions,
            IndexNameExpressionResolver resolver, ClusterService cs) {
        if (resolved.isLocalAll()) {
            // Only let localAll pass if there is an explicit privilege for a * index pattern
            
            for (IndexPattern indexPattern : ipatterns) {                
                if ("*".equals(indexPattern.getUnresolvedIndexPattern(user))) {
                    Set<String> matchingActions = new HashSet<>(actions);
                    
                    for (TypePerm typePerm : indexPattern.typePerms) {
                        for (String action : actions) {
                            if (WildcardMatcher.matchAny(typePerm.perms, action)) {
                                matchingActions.remove(action);
                            }
                        }
                    }
                    
                    if (matchingActions.isEmpty()) {
                        return true;
                    }
                }                
            }
            
            return false;
        } else {
            Set<String> matchingIndex = new HashSet<>(resolved.getAllIndices());

            for (String in : resolved.getAllIndices()) {
                //find index patterns who are matching
                Set<String> matchingActions = new HashSet<>(actions);
                Set<String> matchingTypes = new HashSet<>(resolved.getTypes());
                for (IndexPattern p : ipatterns) {
                    if (WildcardMatcher.matchAny(p.getResolvedIndexPattern(user, resolver, cs), in)) {
                        //per resolved index per pattern
                        for (String t : resolved.getTypes()) {
                            for (TypePerm tp : p.typePerms) {
                                if (WildcardMatcher.match(tp.typePattern, t)) {
                                    matchingTypes.remove(t);
                                    for (String a : actions) {
                                        if (WildcardMatcher.matchAny(tp.perms, a)) {
                                            matchingActions.remove(a);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                if (matchingActions.isEmpty() && matchingTypes.isEmpty()) {
                    matchingIndex.remove(in);
                }
            }

            return matchingIndex.isEmpty();
        }
    }

 
    private class RoleMappingHolder {

        private ListMultimap<String, String> users;
        private ListMultimap<Set<String>, String> abars;
        private ListMultimap<String, String> bars;
        private ListMultimap<String, String> hosts;
        private final String hostResolverMode;

        private RoleMappingHolder(final SgDynamicConfiguration<RoleMappingsV6> rolesMapping, final String hostResolverMode) {

            this.hostResolverMode = hostResolverMode;

            if (rolesMapping != null) {

                final ListMultimap<String, String> users_ = ArrayListMultimap.create();
                final ListMultimap<Set<String>, String> abars_ = ArrayListMultimap.create();
                final ListMultimap<String, String> bars_ = ArrayListMultimap.create();
                final ListMultimap<String, String> hosts_ = ArrayListMultimap.create();

                for (final Entry<String, RoleMappingsV6> roleMap : rolesMapping.getCEntries().entrySet()) {

                    for (String u : roleMap.getValue().getUsers()) {
                        users_.put(u, roleMap.getKey());
                    }

                    final Set<String> abar = new HashSet<String>(roleMap.getValue().getAndBackendroles());

                    if (!abar.isEmpty()) {
                        abars_.put(abar, roleMap.getKey());
                    }

                    for (String bar : roleMap.getValue().getBackendroles()) {
                        bars_.put(bar, roleMap.getKey());
                    }

                    for (String host : roleMap.getValue().getHosts()) {
                        hosts_.put(host, roleMap.getKey());
                    }
                }

                users = users_;
                abars = abars_;
                bars = bars_;
                hosts = hosts_;
            }
        }

        private Set<String> map(final User user, final TransportAddress caller) {

            if (user == null || users == null || abars == null || bars == null || hosts == null) {
                return Collections.emptySet();
            }

            final Set<String> sgRoles = new TreeSet<String>();

            if (rolesMappingResolution == ConfigConstants.RolesMappingResolution.BOTH
                    || rolesMappingResolution == ConfigConstants.RolesMappingResolution.BACKENDROLES_ONLY) {
                if (log.isDebugEnabled()) {
                    log.debug("Pass backendroles from {}", user);
                }
                sgRoles.addAll(user.getRoles());
            }

            if (((rolesMappingResolution == ConfigConstants.RolesMappingResolution.BOTH
                    || rolesMappingResolution == ConfigConstants.RolesMappingResolution.MAPPING_ONLY))) {

                for (String p : WildcardMatcher.getAllMatchingPatterns(users.keySet(), user.getName())) {
                    sgRoles.addAll(users.get(p));
                }

                for (String p : WildcardMatcher.getAllMatchingPatterns(bars.keySet(), user.getRoles())) {
                    sgRoles.addAll(bars.get(p));
                }

                for (Set<String> p : abars.keySet()) {
                    if (WildcardMatcher.allPatternsMatched(p, user.getRoles())) {
                        sgRoles.addAll(abars.get(p));
                    }
                }

                if (caller != null) {
                    //IPV4 or IPv6 (compressed and without scope identifiers)
                    final String ipAddress = caller.getAddress();

                    for (String p : WildcardMatcher.getAllMatchingPatterns(hosts.keySet(), ipAddress)) {
                        sgRoles.addAll(hosts.get(p));
                    }

                    if (caller.address() != null
                            && (hostResolverMode.equalsIgnoreCase("ip-hostname") || hostResolverMode.equalsIgnoreCase("ip-hostname-lookup"))) {
                        final String hostName = caller.address().getHostString();

                        for (String p : WildcardMatcher.getAllMatchingPatterns(hosts.keySet(), hostName)) {
                            sgRoles.addAll(hosts.get(p));
                        }
                    }

                    if (caller.address() != null && hostResolverMode.equalsIgnoreCase("ip-hostname-lookup")) {

                        final String resolvedHostName = caller.address().getHostName();

                        for (String p : WildcardMatcher.getAllMatchingPatterns(hosts.keySet(), resolvedHostName)) {
                            sgRoles.addAll(hosts.get(p));
                        }
                    }
                }
            }

            return Collections.unmodifiableSet(sgRoles);

        }
    }


    public Set<String> mapSgRoles(User user, TransportAddress caller) {
        return roleMappingHolder.map(user, caller);
    }

    public static class TenantPermissionsImpl implements TenantPermissions {
        
                
        private final boolean read;
        private final boolean write;
        
        public TenantPermissionsImpl(boolean read, boolean write) {
            this.read = read;
            this.write = write;
        }
        
        public boolean isReadPermitted() {
            return read;
        }
        
        public boolean isWritePermitted() {
            return write;
        }

        public Set<String> getPermissions() {
            return Collections.emptySet();
        }
    }
    
    private final static Set<String> SET_OF_EVERYTHING = ImmutableSet.of("*");
    
    private static final TenantPermissions FULL_TENANT_PERMISSIONS = new TenantPermissions() {
        
        
        @Override
        public boolean isWritePermitted() {
            return true;
        }
        
        @Override
        public boolean isReadPermitted() {
            return true;
        }
        
        @Override
        public Set<String> getPermissions() {
            return SET_OF_EVERYTHING;
        }
    };
    
    private static final TenantPermissions EMPTY_TENANT_PERMISSIONS = new TenantPermissions() {
        @Override
        public boolean isWritePermitted() {
            return false;
        }
        
        @Override
        public boolean isReadPermitted() {
            return false;
        }
        
        @Override
        public Set<String> getPermissions() {
            return Collections.emptySet();
        }
    };
}
