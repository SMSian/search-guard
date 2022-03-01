/*
 * Copyright 2017-2022 by floragunn GmbH - All rights reserved
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

import static com.floragunn.searchguard.privileges.PrivilegesInterceptor.InterceptionResult.ALLOW;
import static com.floragunn.searchguard.privileges.PrivilegesInterceptor.InterceptionResult.DENY;
import static com.floragunn.searchguard.privileges.PrivilegesInterceptor.InterceptionResult.NORMAL;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.IndicesRequest.Replaceable;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.mapping.get.GetFieldMappingsIndexRequest;
import org.opensearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.action.admin.indices.refresh.RefreshRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.get.MultiGetRequest;
import org.opensearch.action.get.MultiGetRequest.Item;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.replication.ReplicationRequest;
import org.opensearch.action.support.single.shard.SingleShardRequest;
import org.opensearch.action.termvectors.MultiTermVectorsRequest;
import org.opensearch.action.termvectors.TermVectorsRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.rest.RestStatus;

import com.floragunn.searchguard.privileges.ActionRequestIntrospector.ResolvedIndices;
import com.floragunn.searchguard.privileges.PrivilegesInterceptor;
import com.floragunn.searchguard.sgconf.SgRoles;
import com.floragunn.searchguard.sgconf.SgRoles.TenantPermissions;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.util.ImmutableSet;

public class PrivilegesInterceptorImpl implements PrivilegesInterceptor {

    private static final String USER_TENANT = "__user__";

    protected final Logger log = LogManager.getLogger(this.getClass());
    private final String kibanaServerUsername;
    private final String kibanaIndexName;
    private final String kibanaIndexNamePrefix;
    private final Pattern versionedKibanaIndexPattern;
    private final Pattern kibanaIndexPatternWithTenant;
    private final Predicate<String> isTenantValid;
    private final boolean enabled;

    public PrivilegesInterceptorImpl(FeMultiTenancyConfig config, Predicate<String> isTenantValid) {
        this.enabled = config.isEnabled();
        this.kibanaServerUsername = config.getServerUsername();
        this.kibanaIndexName = config.getIndex();
        this.kibanaIndexNamePrefix = this.kibanaIndexName + "_";
        this.versionedKibanaIndexPattern = Pattern
                .compile(Pattern.quote(this.kibanaIndexName) + "(_-?[0-9]+_[a-z0-9]+)?(_[0-9]+\\.[0-9]+\\.[0-9]+(_[0-9]{3})?)");
        this.kibanaIndexPatternWithTenant = Pattern.compile(Pattern.quote(this.kibanaIndexName) + "(_-?[0-9]+_[a-z0-9]+(_[0-9]{3})?)");

        this.isTenantValid = isTenantValid;
    }

    private boolean isTenantAllowed(final ActionRequest request, final String action, final User user, SgRoles sgRoles,
            final String requestedTenant) {

        if (!isTenantValid.test(requestedTenant)) {
            log.warn("Invalid tenant: " + requestedTenant + "; user: " + user);

            return false;
        }

        TenantPermissions tenantPermissions = sgRoles.getTenantPermissions(user, requestedTenant);

        if (!tenantPermissions.isReadPermitted()) {
            log.warn("Tenant {} is not allowed for user {}", requestedTenant, user.getName());
            return false;
        } else {

            if (log.isDebugEnabled()) {
                log.debug("request " + request.getClass());
            }

            if (!tenantPermissions.isWritePermitted() && action.startsWith("indices:data/write")) {
                log.warn("Tenant {} is not allowed to write (user: {})", requestedTenant, user.getName());
                return false;
            }
        }

        return true;
    }

    @Override
    public InterceptionResult replaceKibanaIndex(final ActionRequest request, final String action, final User user,
            final ResolvedIndices requestedResolved, SgRoles sgRoles) {

        if (!enabled) {
            return NORMAL;
        }

        if (user.getName().equals(kibanaServerUsername)) {
            return NORMAL;
        }

        if (log.isDebugEnabled()) {
            log.debug("replaceKibanaIndex(" + action + ", " + user + ")\nrequestedResolved: " + requestedResolved);
        }

        IndexInfo kibanaIndexInfo = checkForExclusivelyUsedKibanaIndexOrAlias(request, requestedResolved);

        if (kibanaIndexInfo == null) {
            // This is not about the .kibana index: Nothing to do here, get out early!
            return NORMAL;
        }

        if (log.isDebugEnabled()) {
            log.debug("IndexInfo: " + kibanaIndexInfo);
        }

        String requestedTenant = user.getRequestedTenant();

        if (requestedTenant == null || requestedTenant.length() == 0) {
            if (kibanaIndexInfo.tenantInfoPart != null) {
                // XXX This indicates that the user tried to directly address an internal Kibana index including tenant name  (like .kibana_92668751_admin)
                // The original implementation allows these requests to pass with normal privileges if the sgtenant header is null. Tenant privileges are ignored then.
                // Integration tests (such as test_multitenancy_mget) are relying on this behaviour.
                return NORMAL;
            } else if (isTenantAllowed(request, action, user, sgRoles, "SGS_GLOBAL_TENANT")) {
                return NORMAL;
            } else {
                return DENY;
            }
        } else {
            if (isTenantAllowed(request, action, user, sgRoles, requestedTenant)) {
                if (kibanaIndexInfo.isReplacementNeeded()) {
                    replaceIndex(request, kibanaIndexInfo.originalName, kibanaIndexInfo.toInternalIndexName(user), action, user);
                }
                return ALLOW;
            } else {
                return DENY;
            }
        }

        // TODO handle user tenant in that way that this tenant cannot be specified as
        // regular tenant
        // to avoid security issue

    }

    private void replaceIndex(ActionRequest request, String oldIndexName, String newIndexName, String action, User user) {
        boolean kibOk = false;

        if (log.isDebugEnabled()) {
            log.debug("{} index will be replaced with {} in this {} request", oldIndexName, newIndexName, request.getClass().getName());
        }

        if (request instanceof GetFieldMappingsIndexRequest || request instanceof GetFieldMappingsRequest) {
            return;
        }

        //handle msearch and mget
        //in case of GET change the .kibana index to the userskibanaindex
        //in case of Search add the userskibanaindex
        //if (request instanceof CompositeIndicesRequest) {
        String[] newIndexNames = new String[] { newIndexName };

        if (request instanceof CreateIndexRequest) {
            ((CreateIndexRequest) request).index(newIndexName);
            kibOk = true;
        } else if (request instanceof BulkRequest) {

            for (DocWriteRequest<?> ar : ((BulkRequest) request).requests()) {

                if (ar instanceof DeleteRequest) {
                    ((DeleteRequest) ar).index(newIndexName);
                }

                if (ar instanceof IndexRequest) {
                    ((IndexRequest) ar).index(newIndexName);
                }

                if (ar instanceof UpdateRequest) {
                    ((UpdateRequest) ar).index(newIndexName);
                }
            }

            kibOk = true;

        } else if (request instanceof MultiGetRequest) {

            for (Item item : ((MultiGetRequest) request).getItems()) {
                item.index(newIndexName);
            }

            kibOk = true;

        } else if (request instanceof MultiSearchRequest) {

            for (SearchRequest ar : ((MultiSearchRequest) request).requests()) {
                ar.indices(newIndexNames);
            }

            kibOk = true;

        } else if (request instanceof MultiTermVectorsRequest) {

            for (TermVectorsRequest ar : (Iterable<TermVectorsRequest>) () -> ((MultiTermVectorsRequest) request).iterator()) {
                ar.index(newIndexName);
            }

            kibOk = true;
        } else if (request instanceof UpdateRequest) {
            ((UpdateRequest) request).index(newIndexName);
            kibOk = true;
        } else if (request instanceof IndexRequest) {
            ((IndexRequest) request).index(newIndexName);
            kibOk = true;
        } else if (request instanceof DeleteRequest) {
            ((DeleteRequest) request).index(newIndexName);
            kibOk = true;
        } else if (request instanceof SingleShardRequest) {
            ((SingleShardRequest<?>) request).index(newIndexName);
            kibOk = true;
        } else if (request instanceof RefreshRequest) {
            ((RefreshRequest) request).indices(newIndexNames); //???
            kibOk = true;
        } else if (request instanceof ReplicationRequest) {
            ((ReplicationRequest<?>) request).index(newIndexName);
            kibOk = true;
        } else if (request instanceof Replaceable) {
            Replaceable replaceableRequest = (Replaceable) request;
            replaceableRequest.indices(newIndexNames);
            kibOk = true;
        } else if (request instanceof IndicesAliasesRequest) {
            IndicesAliasesRequest indicesAliasesRequest = (IndicesAliasesRequest) request;

            for (AliasActions aliasActions : indicesAliasesRequest.getAliasActions()) {
                if (aliasActions.indices() == null || aliasActions.indices().length != 1) {
                    // This is guarded by replaceKibanaIndex()
                    // Only actions operating on a single Kibana index should arrive here.
                    log.warn("Unexpected AliasActions: " + aliasActions);
                    continue;
                }

                if (!aliasActions.indices()[0].equals(oldIndexName)) {
                    // This is guarded by replaceKibanaIndex()
                    // Only actions operating on a single Kibana index should arrive here.
                    log.warn("Unexpected AliasActions: " + aliasActions + "; expected index: " + oldIndexName);
                    continue;
                }

                aliasActions.index(newIndexName);

                if (aliasActions.actionType() != AliasActions.Type.REMOVE_INDEX) {
                    if (aliasActions.aliases() == null) {
                        log.warn("Unexpected AliasActions: " + aliasActions);
                        continue;
                    }

                    String[] aliases = aliasActions.aliases();
                    String[] newAliases = new String[aliases.length];
                    for (int i = 0; i < aliases.length; i++) {
                        IndexInfo indexInfo = checkForExclusivelyUsedKibanaIndexOrAlias(aliases[i]);

                        if (indexInfo != null && indexInfo.isReplacementNeeded()) {
                            newAliases[i] = indexInfo.toInternalIndexName(user);
                        } else {
                            newAliases[i] = aliases[i];
                        }
                    }

                    aliasActions.aliases(newAliases);
                }

                if (log.isDebugEnabled()) {
                    log.debug("Rewritten IndicesAliasesRequest: " + indicesAliasesRequest.getAliasActions());
                }
            }

            kibOk = true;
        } else {
            log.warn("Dont know what to do (1) with {}", request.getClass());
        }

        if (!kibOk) {
            log.warn("Dont know what to do (2) with {}", request.getClass());
        }
    }

    private IndexInfo checkForExclusivelyUsedKibanaIndexOrAlias(ActionRequest request, ResolvedIndices requestedResolved) {
        if (requestedResolved.isLocalAll()) {
            return null;
        }

        Set<String> indices = getIndices(request);

        if (indices.size() == 1) {
            return checkForExclusivelyUsedKibanaIndexOrAlias(indices.iterator().next());
        } else {
            return null;
        }
    }

    private IndexInfo checkForExclusivelyUsedKibanaIndexOrAlias(String aliasOrIndex) {

        if (aliasOrIndex.equals(kibanaIndexName)) {
            // Pre 7.12: Just .kibana
            return new IndexInfo(aliasOrIndex, kibanaIndexName, null);
        }

        if (aliasOrIndex.startsWith(kibanaIndexNamePrefix)) {
            Matcher matcher = versionedKibanaIndexPattern.matcher(aliasOrIndex);

            if (matcher.matches()) {
                // Post 7.12: .kibana_7.12.0
                // Prefix will be: .kibana_
                // Suffix will be: _7.12.0
                if (matcher.group(1) == null) {
                    // Basic case
                    return new IndexInfo(aliasOrIndex, kibanaIndexName, matcher.group(2));
                } else {
                    // We have here the case that the index replacement has been already applied.
                    // This can happen when internal ES operations trigger further operations, e.g. an index triggers an auto_create.
                    return new IndexInfo(aliasOrIndex, kibanaIndexName, matcher.group(2), matcher.group(1));
                }
            }

            matcher = kibanaIndexPatternWithTenant.matcher(aliasOrIndex);

            if (matcher.matches()) {
                // Pre 7.12: .kibana_12345678_tenantname

                return new IndexInfo(aliasOrIndex, kibanaIndexName, null, matcher.group(1));
            }
        }

        return null;
    }

    private Set<String> getIndices(ActionRequest request) {
        if (request instanceof PutMappingRequest) {
            PutMappingRequest putMappingRequest = (PutMappingRequest) request;

            return ImmutableSet.of(putMappingRequest.getConcreteIndex() != null ? putMappingRequest.getConcreteIndex().getName() : null,
                    putMappingRequest.indices());
        } else if (request instanceof IndicesRequest) {
            if (((IndicesRequest) request).indices() != null) {
                return ImmutableSet.of(Arrays.asList(((IndicesRequest) request).indices()));
            } else {
                return Collections.emptySet();
            }
        } else if (request instanceof BulkRequest) {
            return ((BulkRequest) request).getIndices();
        } else if (request instanceof MultiGetRequest) {
            return ((MultiGetRequest) request).getItems().stream().map(item -> item.index()).collect(Collectors.toSet());
        } else if (request instanceof MultiSearchRequest) {
            return ((MultiSearchRequest) request).requests().stream().flatMap(r -> Arrays.stream(r.indices())).collect(Collectors.toSet());
        } else if (request instanceof MultiTermVectorsRequest) {
            return ((MultiTermVectorsRequest) request).getRequests().stream().flatMap(r -> Arrays.stream(r.indices())).collect(Collectors.toSet());
        } else {
            if (log.isTraceEnabled()) {
                log.trace("Not supported for multi tenancy: " + request);
            }

            return Collections.emptySet();
        }
    }

    private class IndexInfo {
        final String originalName;
        final String prefix;
        final String suffix;
        final String tenantInfoPart;

        IndexInfo(String originalName, String prefix, String suffix) {
            this.originalName = originalName;
            this.prefix = prefix;
            this.suffix = suffix;
            this.tenantInfoPart = null;
        }

        IndexInfo(String originalName, String prefix, String suffix, String tenantInfoPart) {
            this.originalName = originalName;
            this.prefix = prefix;
            this.suffix = suffix;
            this.tenantInfoPart = tenantInfoPart;
        }

        String toInternalIndexName(User user) {
            if (USER_TENANT.equals(user.getRequestedTenant())) {
                return toInternalIndexName(user.getName());
            } else {
                return toInternalIndexName(user.getRequestedTenant());
            }
        }

        boolean isReplacementNeeded() {
            return this.tenantInfoPart == null;
        }

        private String toInternalIndexName(String tenant) {
            if (tenant == null) {
                throw new OpenSearchException("tenant must not be null here");
            }

            String tenantInfoPart = "_" + tenant.hashCode() + "_" + tenant.toLowerCase().replaceAll("[^a-z0-9]+", "");

            if (this.tenantInfoPart != null && !this.tenantInfoPart.equals(tenantInfoPart)) {
                throw new OpenSearchSecurityException(
                        "This combination of sgtenant header and index is not allowed.\nTenant: " + tenant + "\nIndex: " + originalName,
                        RestStatus.BAD_REQUEST);
            }

            StringBuilder result = new StringBuilder(prefix).append(tenantInfoPart);

            if (this.suffix != null) {
                result.append(suffix);
            }

            return result.toString();
        }

        @Override
        public String toString() {
            return "IndexInfo [originalName=" + originalName + ", prefix=" + prefix + ", suffix=" + suffix + ", tenantInfoPart=" + tenantInfoPart
                    + "]";
        }

    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public String getKibanaIndex() {
        return kibanaIndexName;
    }

    @Override
    public String getKibanaServerUser() {
        return kibanaServerUsername;
    }
}
