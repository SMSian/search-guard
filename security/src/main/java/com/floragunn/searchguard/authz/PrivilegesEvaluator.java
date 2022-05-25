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

package com.floragunn.searchguard.authz;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.GuiceDependencies;
import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.authc.legacy.LegacySgConfig;
import com.floragunn.searchguard.authc.session.backend.SessionApi;
import com.floragunn.searchguard.authz.PrivilegesEvaluationResult.Status;
import com.floragunn.searchguard.authz.actions.Action;
import com.floragunn.searchguard.authz.actions.ActionRequestIntrospector;
import com.floragunn.searchguard.authz.actions.ActionRequestIntrospector.ActionRequestInfo;
import com.floragunn.searchguard.authz.actions.Actions;
import com.floragunn.searchguard.authz.config.ActionGroup;
import com.floragunn.searchguard.authz.config.AuthorizationConfig;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.authz.config.RoleMapping;
import com.floragunn.searchguard.authz.config.Tenant;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ClusterInfoHolder;
import com.floragunn.searchguard.configuration.ConfigMap;
import com.floragunn.searchguard.configuration.ConfigurationChangeListener;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.privileges.PrivilegesInterceptor;
import com.floragunn.searchguard.privileges.SpecialPrivilegesEvaluationContext;
import com.floragunn.searchguard.privileges.SpecialPrivilegesEvaluationContextProviderRegistry;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.Pattern;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentStateProvider;
import com.google.common.base.Strings;

public class PrivilegesEvaluator implements ComponentStateProvider {
    private static final String USER_TENANT = "__user__";
    private static final Logger log = LogManager.getLogger(PrivilegesEvaluator.class);
    protected final Logger actionTrace = LogManager.getLogger("sg_action_trace");
    private final ClusterService clusterService;

    private final IndexNameExpressionResolver resolver;

    private final AuditLog auditLog;
    private ThreadContext threadContext;

    private PrivilegesInterceptor privilegesInterceptor;

    private final boolean checkSnapshotRestoreWritePrivileges;

    private final ClusterInfoHolder clusterInfoHolder;
    private final ActionRequestIntrospector actionRequestIntrospector;
    private final SnapshotRestoreEvaluator snapshotRestoreEvaluator;
    private final SpecialPrivilegesEvaluationContextProviderRegistry specialPrivilegesEvaluationContextProviderRegistry;
    private final Pattern adminOnlyActions;
    private final Pattern adminOnlyIndices;
    private final Actions actions;
    private final ConfigConstants.RolesMappingResolution rolesMappingResolution;
    private final ComponentState componentState = new ComponentState(10, null, "privileges_evaluator");

    private volatile AuthorizationConfig authzConfig = AuthorizationConfig.DEFAULT;
    private volatile RoleBasedActionAuthorization actionAuthorization = null;
    private volatile LegacyRoleBasedDocumentAuthorization documentAuthorization = null;
    private volatile RoleMapping.InvertedIndex roleMapping;

    public PrivilegesEvaluator(Client localClient, ClusterService clusterService, ThreadPool threadPool,
            ConfigurationRepository configurationRepository, IndexNameExpressionResolver resolver, AuditLog auditLog, Settings settings,
            ClusterInfoHolder clusterInfoHolder, Actions actions, ActionRequestIntrospector actionRequestIntrospector,
            SpecialPrivilegesEvaluationContextProviderRegistry specialPrivilegesEvaluationContextProviderRegistry,
            GuiceDependencies guiceDependencies, NamedXContentRegistry namedXContentRegistry, boolean enterpriseModulesEnabled) {
        super();
        this.clusterService = clusterService;
        this.resolver = resolver;
        this.auditLog = auditLog;

        this.threadContext = threadPool.getThreadContext();

        this.checkSnapshotRestoreWritePrivileges = settings.getAsBoolean(ConfigConstants.SEARCHGUARD_CHECK_SNAPSHOT_RESTORE_WRITE_PRIVILEGES,
                ConfigConstants.SG_DEFAULT_CHECK_SNAPSHOT_RESTORE_WRITE_PRIVILEGES);

        this.clusterInfoHolder = clusterInfoHolder;
        this.specialPrivilegesEvaluationContextProviderRegistry = specialPrivilegesEvaluationContextProviderRegistry;

        this.actions = actions;
        this.actionRequestIntrospector = actionRequestIntrospector;
        snapshotRestoreEvaluator = new SnapshotRestoreEvaluator(settings, auditLog, guiceDependencies);
        this.adminOnlyActions = getPatternFromSettings(settings, ConfigConstants.SEARCHGUARD_ADMIN_ONLY_ACTIONS,
                ImmutableList.of("cluster:admin:searchguard:config/*"));
        this.adminOnlyIndices = getPatternFromSettings(settings, ConfigConstants.SEARCHGUARD_ADMIN_ONLY_INDICES,
                ImmutableList.of("searchguard", ".searchguard_*", ".signals_watches*", ".signals_accounts", ".signals_settings"));

        this.rolesMappingResolution = getRolesMappingResolution(settings);

        configurationRepository.subscribeOnChange(new ConfigurationChangeListener() {

            @Override
            public void onChange(ConfigMap configMap) {
                SgDynamicConfiguration<AuthorizationConfig> config = configMap.get(CType.AUTHZ);
                SgDynamicConfiguration<LegacySgConfig> legacyConfig = configMap.get(CType.CONFIG);
                AuthorizationConfig authzConfig = AuthorizationConfig.DEFAULT;

                if (config != null && config.getCEntry("default") != null) {
                    PrivilegesEvaluator.this.authzConfig = authzConfig = config.getCEntry("default");

                    log.info("Updated authz config:\n" + config);
                    if (log.isDebugEnabled()) {
                        log.debug(authzConfig);
                    }
                } else if (legacyConfig != null && legacyConfig.getCEntry("sg_config") != null) {
                    try {
                        LegacySgConfig sgConfig = legacyConfig.getCEntry("sg_config");
                        PrivilegesEvaluator.this.authzConfig = authzConfig = AuthorizationConfig.parseLegacySgConfig(sgConfig.getSource(), null);

                        log.info("Updated authz config (legacy):\n" + legacyConfig);
                        if (log.isDebugEnabled()) {
                            log.debug(authzConfig);
                        }
                    } catch (ConfigValidationException e) {
                        log.error("Error while parsing sg_config:\n" + e);
                    }
                }

                SgDynamicConfiguration<Role> roles = configMap.get(CType.ROLES);
                SgDynamicConfiguration<Tenant> tenants = configMap.get(CType.TENANTS);

                ActionGroup.FlattenedIndex actionGroups = configMap.get(CType.ACTIONGROUPS) != null
                        ? new ActionGroup.FlattenedIndex(configMap.get(CType.ACTIONGROUPS))
                        : ActionGroup.FlattenedIndex.EMPTY;

                actionAuthorization = new RoleBasedActionAuthorization(roles, actionGroups, actions,
                        clusterService.state().metadata().getIndicesLookup().keySet(), tenants.getCEntries().keySet(), adminOnlyIndices,
                        authzConfig.getMetricsLevel());

                documentAuthorization = new LegacyRoleBasedDocumentAuthorization(roles, resolver, clusterService);

                roleMapping = new RoleMapping.InvertedIndex(configMap.get(CType.ROLESMAPPING));

                componentState.setConfigVersion(configMap.getVersionsAsString());
                componentState.replacePart(actionAuthorization.getComponentState());
                componentState.replacePart(documentAuthorization.getComponentState());
                componentState.updateStateFromParts();
            }
        });

        clusterService.addListener(new ClusterStateListener() {

            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                RoleBasedActionAuthorization actionAuthorization = PrivilegesEvaluator.this.actionAuthorization;

                if (actionAuthorization != null) {
                    actionAuthorization.updateIndices(event.state().metadata().getIndicesLookup().keySet());
                }

                DocumentAuthorization documentAuthorization = PrivilegesEvaluator.this.documentAuthorization;

                if (documentAuthorization != null) {
                    documentAuthorization.updateIndices(event.state().metadata().getIndicesLookup().keySet());
                }
            }
        });
    }

    public boolean isInitialized() {
        return actionAuthorization != null;
    }

    public ImmutableSet<String> getMappedRoles(User user, SpecialPrivilegesEvaluationContext specialPrivilegesEvaluationContext) {
        if (roleMapping == null) {
            return ImmutableSet.empty();
        }

        if (specialPrivilegesEvaluationContext == null) {
            TransportAddress caller = Objects.requireNonNull((TransportAddress) this.threadContext.getTransient(ConfigConstants.SG_REMOTE_ADDRESS));
            return mapSgRoles(user, caller);
        } else {
            return specialPrivilegesEvaluationContext.getMappedRoles();
        }
    }

    public PrivilegesEvaluationResult evaluate(User user, ImmutableSet<String> mappedRoles, String action0, ActionRequest request, Task task,
            PrivilegesEvaluationContext context, SpecialPrivilegesEvaluationContext specialPrivilegesEvaluationContext) {

        if (!isInitialized()) {
            throw new ElasticsearchSecurityException("Search Guard is not initialized.");
        }

        if (action0.startsWith("internal:indices/admin/upgrade")) {
            action0 = "indices:admin/upgrade";
        }

        Action action = actions.get(action0);

        if (adminOnlyActions.matches(action0)) {
            log.info("Action " + action0 + " is reserved for users authenticating with an admin certificate");
            return PrivilegesEvaluationResult.INSUFFICIENT.reason("Action is reserved for users authenticating with an admin certificate")
                    .missingPrivileges(action);
        }

        if (SessionApi.DeleteAction.NAME.equals(action0)) {
            // Special case for deleting own session: This is always allowed
            return PrivilegesEvaluationResult.OK;
        }

        if (action.isOpen()) {
            return PrivilegesEvaluationResult.OK;
        }

        AuthorizationConfig authzConfig = this.authzConfig;
        ActionAuthorization actionAuthorization;

        if (specialPrivilegesEvaluationContext == null) {
            actionAuthorization = this.actionAuthorization;
        } else {
            actionAuthorization = specialPrivilegesEvaluationContext.getActionAuthorization();
        }

        try {
            if (request instanceof BulkRequest && (com.google.common.base.Strings.isNullOrEmpty(user.getRequestedTenant()))) {
                // Shortcut for bulk actions. The details are checked on the lower level of the BulkShardRequests (Action indices:data/write/bulk[s]).
                // This shortcut is only possible if the default tenant is selected, as we might need to rewrite the request for non-default tenants.
                // No further access check for the default tenant is necessary, as access will be also checked on the TransportShardBulkAction level.

                PrivilegesEvaluationResult result = actionAuthorization.hasClusterPermission(context, action);

                if (result.getStatus() != PrivilegesEvaluationResult.Status.OK) {
                    log.info("No cluster-level permission for {} [Action [{}]] [RolesChecked {}]\n{}", user, action0, mappedRoles, result);
                    return result;
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("Taking shortcut for BulkRequest; user: " + user);
                    }

                    return result;
                }
            }

            ActionRequestInfo requestInfo = actionRequestIntrospector.getActionRequestInfo(action0, request);

            if (log.isDebugEnabled()) {
                if (requestInfo.isUnknown()) {
                    log.debug("### evaluate UNKNOWN " + action0 + " (" + request.getClass().getName() + ")\nUser: " + user
                            + "\nspecialPrivilegesEvaluationContext: " + specialPrivilegesEvaluationContext);
                } else if (!requestInfo.isIndexRequest()) {
                    log.debug("### evaluate " + action0 + " (" + request.getClass().getName() + ")\nUser: " + user
                            + "\nspecialPrivilegesEvaluationContext: " + specialPrivilegesEvaluationContext);
                } else {
                    log.debug("### evaluate " + action0 + " (" + request.getClass().getName() + ")\nUser: " + user
                            + "\nspecialPrivilegesEvaluationContext: " + specialPrivilegesEvaluationContext + "\nResolved: "
                            + requestInfo.getResolvedIndices() + "\nUresolved: " + requestInfo.getUnresolved() + "\nIgnoreUnauthorizedIndices: "
                            + authzConfig.isIgnoreUnauthorizedIndices());
                }
            }

            // check snapshot/restore requests 
            PrivilegesEvaluationResult result = snapshotRestoreEvaluator.evaluate(request, task, action, clusterInfoHolder);
            if (!result.isPending()) {
                return result;
            }

            if (action.isClusterPrivilege()) {
                result = actionAuthorization.hasClusterPermission(context, action);

                if (result.getStatus() != PrivilegesEvaluationResult.Status.OK) {
                    log.info("No {}-level perm match for {} {} [Action [{}]] [RolesChecked {}]:\n{}", "cluster", user, requestInfo, action0,
                            mappedRoles, result);
                    return result;
                }

                if (request instanceof RestoreSnapshotRequest && checkSnapshotRestoreWritePrivileges) {
                    // Evaluate additional index privileges                
                    return evaluateIndexPrivileges(user, action0, action.expandPrivileges(request), request, task, requestInfo, mappedRoles,
                            authzConfig, actionAuthorization, specialPrivilegesEvaluationContext, context);
                }

                if (privilegesInterceptor != null) {
                    PrivilegesInterceptor.InterceptionResult replaceResult = privilegesInterceptor.replaceKibanaIndex(context, request, action,
                            actionAuthorization);

                    if (log.isDebugEnabled()) {
                        log.debug("Result from privileges interceptor for cluster perm: {}", replaceResult);
                    }

                    if (replaceResult == PrivilegesInterceptor.InterceptionResult.DENY) {
                        auditLog.logMissingPrivileges(action0, request, task);
                        return PrivilegesEvaluationResult.INSUFFICIENT.reason("Denied due to multi-tenancy settings");
                    } else if (replaceResult == PrivilegesInterceptor.InterceptionResult.ALLOW) {
                        return PrivilegesEvaluationResult.OK;
                    }
                }

                ImmutableSet<Action> additionalPrivileges = action.getAdditionalPrivileges(request);

                if (additionalPrivileges.isEmpty()) {
                    if (log.isTraceEnabled()) {
                        log.trace("Allowing as cluster privilege: " + action0);
                    }
                    return PrivilegesEvaluationResult.OK;
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("Additional privileges required: " + additionalPrivileges);
                    }

                    return evaluateAdditionalPrivileges(user, action0, additionalPrivileges, request, task, requestInfo, mappedRoles, authzConfig,
                            actionAuthorization, specialPrivilegesEvaluationContext, context);
                }
            }

            if (action.isTenantPrivilege()) {
                result = hasTenantPermission(user, mappedRoles, action, actionAuthorization, context);

                if (!result.isOk()) {
                    log.info("No {}-level perm match for {} {} [Action [{}]] [RolesChecked {}]:\n{}", "tenant", user, requestInfo, action0,
                            mappedRoles, result);
                    return result;
                } else {
                    if (log.isTraceEnabled()) {
                        log.trace("Allowing as tenant privilege: " + action0);
                    }
                    return result;
                }
            }

            if (checkDocWhitelistHeader(user, action0, request)) {
                if (log.isTraceEnabled()) {
                    log.trace("Allowing due to doc whitelist: " + action0);
                }
                return PrivilegesEvaluationResult.OK;
            }

            ImmutableSet<Action> allIndexPermsRequired = action.expandPrivileges(request);

            if (log.isDebugEnabled()) {
                if (allIndexPermsRequired.size() > 1 || !allIndexPermsRequired.contains(action)) {
                    log.debug("Expanded index privileges: " + allIndexPermsRequired);
                }
            }

            return evaluateIndexPrivileges(user, action0, allIndexPermsRequired, request, task, requestInfo, mappedRoles, authzConfig,
                    actionAuthorization, specialPrivilegesEvaluationContext, context);
        } catch (Exception e) {
            log.error("Error while evaluating " + action0 + " (" + request.getClass().getName() + ")", e);
            return PrivilegesEvaluationResult.INSUFFICIENT.with(ImmutableList.of(new PrivilegesEvaluationResult.Error(e.getMessage(), e)));
        }
    }

    private PrivilegesEvaluationResult evaluateIndexPrivileges(User user, String action0, ImmutableSet<Action> requiredPermissions,
            ActionRequest request, Task task, ActionRequestInfo actionRequestInfo, ImmutableSet<String> mappedRoles, AuthorizationConfig authzConfig,
            ActionAuthorization actionAuthorization, SpecialPrivilegesEvaluationContext specialPrivilegesEvaluationContext,
            PrivilegesEvaluationContext context) throws PrivilegesEvaluationException {

        ActionFilter additionalActionFilter = null;

        if (actionRequestInfo.getResolvedIndices().containsOnlyRemoteIndices()) {
            log.debug(
                    "Request contains only remote indices. We can skip all further checks and let requests be handled by remote cluster: " + action0);
            return PrivilegesEvaluationResult.OK;
        }

        if (log.isDebugEnabled()) {
            log.debug("requested resolved indextypes: {}", actionRequestInfo);
        }

        if (privilegesInterceptor != null) {

            PrivilegesInterceptor.InterceptionResult replaceResult = privilegesInterceptor.replaceKibanaIndex(context, request, actions.get(action0),
                    actionAuthorization);

            if (log.isDebugEnabled()) {
                log.debug("Result from privileges interceptor: {}", replaceResult);
            }

            if (replaceResult == PrivilegesInterceptor.InterceptionResult.DENY) {
                auditLog.logMissingPrivileges(action0, request, task);
                return PrivilegesEvaluationResult.INSUFFICIENT.reason("Denied due to multi-tenancy settings");
            } else if (replaceResult == PrivilegesInterceptor.InterceptionResult.ALLOW) {
                return PrivilegesEvaluationResult.OK.with(additionalActionFilter);
            }
        }

        boolean dnfofPossible = authzConfig.isIgnoreUnauthorizedIndices() && authzConfig.getIgnoreUnauthorizedIndicesActions().matches(action0)
                && (actionRequestInfo.ignoreUnavailable() || actionRequestInfo.containsWildcards());

        if (!dnfofPossible) {
            context.setResolveLocalAll(false);
        }

        ImmutableSet<Action> allIndexPermsRequired = requiredPermissions.matching(Action::isIndexPrivilege);
        ImmutableSet<Action> clusterPermissions = requiredPermissions.matching(Action::isClusterPrivilege);

        if (!clusterPermissions.isEmpty()) {
            for (Action clusterPermission : clusterPermissions) {
                PrivilegesEvaluationResult result = actionAuthorization.hasClusterPermission(context, clusterPermission);

                if (result.getStatus() != PrivilegesEvaluationResult.Status.OK) {
                    if (log.isEnabled(Level.INFO)) {
                        log.info("### No cluster privileges for " + clusterPermission + " (" + request.getClass().getName() + ")\nUser: " + user
                                + "\nResolved Indices: " + actionRequestInfo.getResolvedIndices() + "\nUnresolved: "
                                + actionRequestInfo.getUnresolved() + "\nRoles: " + mappedRoles + "\nRequired Privileges: " + allIndexPermsRequired);
                    }

                    return result;
                }
            }
        }

        PrivilegesEvaluationResult privilegesEvaluationResult = actionAuthorization.hasIndexPermission(context, allIndexPermsRequired,
                actionRequestInfo.getResolvedIndices());

        if (log.isTraceEnabled()) {
            log.trace("Result from privileges evaluation: " + privilegesEvaluationResult.getStatus() + "\n" + privilegesEvaluationResult);
        }

        if (privilegesEvaluationResult.getStatus() == Status.PARTIALLY_OK) {
            if (dnfofPossible) {
                if (log.isDebugEnabled()) {
                    log.debug("DNF: Reducing indices to " + privilegesEvaluationResult.getAvailableIndices() + "\n" + privilegesEvaluationResult);
                }

                privilegesEvaluationResult = actionRequestIntrospector.reduceIndices(action0, request,
                        privilegesEvaluationResult.getAvailableIndices(), actionRequestInfo);

            } else {
                privilegesEvaluationResult = privilegesEvaluationResult.status(Status.INSUFFICIENT);
            }
        } else if (privilegesEvaluationResult.getStatus() == Status.INSUFFICIENT) {
            if (dnfofPossible) {
                if (!actionRequestInfo.getResolvedIndices().getRemoteIndices().isEmpty()) {
                    privilegesEvaluationResult = actionRequestIntrospector.reduceIndices(action0, request, ImmutableSet.empty(), actionRequestInfo);
                } else if (authzConfig.getIgnoreUnauthorizedIndicesActionsAllowingEmptyResult().matches(action0)) {
                    if (log.isTraceEnabled()) {
                        log.trace("Changing result from INSUFFICIENT to EMPTY");
                    }

                    privilegesEvaluationResult = privilegesEvaluationResult.status(Status.EMPTY);
                }
            }
        }

        if (privilegesEvaluationResult.getStatus() == Status.EMPTY) {
            if (actionRequestIntrospector.forceEmptyResult(request)) {
                if (log.isDebugEnabled()) {
                    log.debug("DNF: Reducing indices to yield an empty result\n" + privilegesEvaluationResult);
                }

                privilegesEvaluationResult = privilegesEvaluationResult.status(Status.OK);
            } else {
                log.warn("DNFOF for empty results is not available for " + action0 + " (" + request.getClass().getName() + ")");
            }
        }

        if (privilegesEvaluationResult.getStatus() != Status.OK) {
            Level logLevel = privilegesEvaluationResult.hasErrors() ? Level.WARN : Level.INFO;

            if (log.isEnabled(logLevel)) {
                log.log(logLevel,
                        "### No index privileges for " + action0 + " (" + request.getClass().getName() + ")\nUser: " + user + "\nResolved Indices: "
                                + actionRequestInfo.getResolvedIndices() + "\nUnresolved: " + actionRequestInfo.getUnresolved() + "\nRoles: "
                                + mappedRoles + "\nRequired Privileges: " + allIndexPermsRequired + "\n" + privilegesEvaluationResult);
            }

            return privilegesEvaluationResult;
        }

        if (request instanceof ResizeRequest) {
            if (log.isDebugEnabled()) {
                log.debug("Checking additional create index action for resize operation: " + request);
            }
            ResizeRequest resizeRequest = (ResizeRequest) request;
            CreateIndexRequest createIndexRequest = resizeRequest.getTargetIndexRequest();
            PrivilegesEvaluationResult subResponse = evaluate(user, mappedRoles, CreateIndexAction.NAME, createIndexRequest, task, context,
                    specialPrivilegesEvaluationContext);

            if (!subResponse.isOk()) {
                return subResponse;
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("Allowed because we have all indices permissions for " + allIndexPermsRequired);
        }

        return PrivilegesEvaluationResult.OK;
    }

    private PrivilegesEvaluationResult evaluateAdditionalPrivileges(User user, String action0, ImmutableSet<Action> additionalPrivileges,
            ActionRequest request, Task task, ActionRequestInfo actionRequestInfo, ImmutableSet<String> mappedRoles, AuthorizationConfig authzConfig,
            ActionAuthorization actionAuthorization, SpecialPrivilegesEvaluationContext specialPrivilegesEvaluationContext,
            PrivilegesEvaluationContext context) throws PrivilegesEvaluationException {

        if (additionalPrivileges.forAllApplies((a) -> a.isIndexPrivilege())) {
            return evaluateIndexPrivileges(user, action0, additionalPrivileges, request, task, actionRequestInfo, mappedRoles, authzConfig,
                    actionAuthorization, specialPrivilegesEvaluationContext, context);
        }

        ImmutableSet<Action> indexPrivileges = ImmutableSet.empty();

        for (Action action : additionalPrivileges) {
            if (action.isClusterPrivilege()) {
                PrivilegesEvaluationResult result = actionAuthorization.hasClusterPermission(context, action);

                if (result.getStatus() != PrivilegesEvaluationResult.Status.OK) {
                    log.info("Additional privilege missing: " + result);
                    return result;
                }
            } else if (action.isTenantPrivilege()) {
                PrivilegesEvaluationResult result = hasTenantPermission(user, mappedRoles, action, actionAuthorization, context);

                if (result.getStatus() != PrivilegesEvaluationResult.Status.OK) {
                    log.info("Additional privilege missing: " + result);
                    return result;
                }
            } else if (action.isIndexPrivilege()) {
                indexPrivileges = indexPrivileges.with(action);
            }
        }

        if (!indexPrivileges.isEmpty()) {
            return evaluateIndexPrivileges(user, action0, indexPrivileges, request, task, actionRequestInfo, mappedRoles, authzConfig,
                    actionAuthorization, specialPrivilegesEvaluationContext, context);
        } else {
            if (log.isTraceEnabled()) {
                log.trace("Allowing: " + action0);
            }
            return PrivilegesEvaluationResult.OK;
        }

    }

    public ImmutableSet<String> mapSgRoles(final User user, final TransportAddress caller) {
        if (roleMapping == null) {
            throw new ElasticsearchSecurityException("SearchGuard is not yet initialized");
        }

        return roleMapping.evaluate(user, caller, rolesMappingResolution);
    }

    public Set<String> getAllConfiguredTenantNames() {
        return actionAuthorization.getTenants();
    }

    public boolean multitenancyEnabled() {
        return privilegesInterceptor != null && privilegesInterceptor.isEnabled();
    }

    /**
     * @deprecated Even though this does not really belong to privileges evaluation, this is necessary to support KibanaInfoAction. This should be somehow moved to the MT module.
     */
    public String getKibanaServerUser() {
        return privilegesInterceptor != null ? privilegesInterceptor.getKibanaServerUser() : "kibanaserver";
    }

    /**
     * @deprecated Even though this does not really belong to privileges evaluation, this is necessary to support KibanaInfoAction. This should be somehow moved to the MT module.
     */
    public String getKibanaIndex() {
        return privilegesInterceptor != null ? privilegesInterceptor.getKibanaIndex() : null;
    }

    public boolean notFailOnForbiddenEnabled() {
        return authzConfig.isIgnoreUnauthorizedIndices();
    }

    public static boolean isTenantPerm(String action0) {
        return action0.startsWith("cluster:admin:searchguard:tenant:");
    }

    /**
     * Only used for authinfo REST API
     */
    public Map<String, Boolean> mapTenants(User user, Set<String> roles) {
        return privilegesInterceptor != null ? privilegesInterceptor.mapTenants(user, ImmutableSet.of(roles), actionAuthorization)
                : ImmutableMap.empty();
    }

    public Map<String, Boolean> evaluateClusterAndTenantPrivileges(User user, TransportAddress caller, Collection<String> privilegesAskedFor) {
        if (privilegesAskedFor == null || privilegesAskedFor.isEmpty() || user == null) {
            log.debug("Privileges or user empty");
            return Collections.emptyMap();
        }

        // Note: This does not take authtokens into account yet. However, as this is only an API for Kibana and Kibana does not use authtokens, 
        // this does not really matter        
        ImmutableSet<String> mappedRoles = mapSgRoles(user, caller);
        String requestedTenant = getRequestedTenant(user);
        PrivilegesEvaluationContext context = new PrivilegesEvaluationContext(user, mappedRoles, null, null, authzConfig.isDebugEnabled(),
                actionRequestIntrospector, resolver);

        Map<String, Boolean> result = new HashMap<>();

        boolean tenantValid = isTenantValid(requestedTenant);

        if (!tenantValid) {
            log.info("Invalid tenant: " + requestedTenant + "; user: " + user);
        }

        for (String privilegeAskedFor : privilegesAskedFor) {
            Action action = actions.get(privilegeAskedFor);

            try {
                if (action.isTenantPrivilege()) {
                    if (tenantValid) {
                        PrivilegesEvaluationResult privilegesEvaluationResult = actionAuthorization.hasTenantPermission(context, action,
                                requestedTenant);

                        result.put(privilegeAskedFor, privilegesEvaluationResult.getStatus() == PrivilegesEvaluationResult.Status.OK);
                    } else {
                        result.put(privilegeAskedFor, false);
                    }
                } else {
                    PrivilegesEvaluationResult privilegesEvaluationResult = actionAuthorization.hasClusterPermission(context, action);

                    result.put(privilegeAskedFor, privilegesEvaluationResult.getStatus() == PrivilegesEvaluationResult.Status.OK);
                }
            } catch (PrivilegesEvaluationException e) {
                log.error("Error while evaluating " + privilegeAskedFor + " for " + user, e);
                result.put(privilegeAskedFor, false);
            }
        }

        return result;
    }

    private boolean isTenantValid(String requestedTenant) {

        if ("SGS_GLOBAL_TENANT".equals(requestedTenant) || USER_TENANT.equals(requestedTenant)) {
            return true;
        }

        return getAllConfiguredTenantNames().contains(requestedTenant);
    }

    private PrivilegesEvaluationResult hasTenantPermission(User user, ImmutableSet<String> mappedRoles, Action action,
            ActionAuthorization actionAuthorization, PrivilegesEvaluationContext context) throws PrivilegesEvaluationException {
        String requestedTenant = !Strings.isNullOrEmpty(user.getRequestedTenant()) ? user.getRequestedTenant() : "SGS_GLOBAL_TENANT";

        if (!multitenancyEnabled() && !"SGS_GLOBAL_TENANT".equals(requestedTenant)) {
            log.warn("Denying request to non-default tenant because MT is disabled: " + requestedTenant);
            return PrivilegesEvaluationResult.INSUFFICIENT.reason("Multi-tenancy is disabled");
        }

        return actionAuthorization.hasTenantPermission(context, action, requestedTenant);
    }

    private String getRequestedTenant(User user) {

        String requestedTenant = user.getRequestedTenant();

        if (Strings.isNullOrEmpty(requestedTenant) || !multitenancyEnabled()) {
            return "SGS_GLOBAL_TENANT";
        } else {
            return requestedTenant;
        }
    }

    public boolean hasClusterPermission(User user, String actionName, TransportAddress callerTransportAddress) throws PrivilegesEvaluationException {
        SpecialPrivilegesEvaluationContext specialPrivilegesEvaluationContext = specialPrivilegesEvaluationContextProviderRegistry.provide(user,
                threadContext);

        if (specialPrivilegesEvaluationContext != null) {
            user = specialPrivilegesEvaluationContext.getUser();
        }

        ImmutableSet<String> mappedRoles;
        ActionAuthorization actionAuthorization;

        if (specialPrivilegesEvaluationContext == null) {
            mappedRoles = mapSgRoles(user, callerTransportAddress);
            actionAuthorization = this.actionAuthorization;
        } else {
            mappedRoles = specialPrivilegesEvaluationContext.getMappedRoles();
            actionAuthorization = specialPrivilegesEvaluationContext.getActionAuthorization();
        }

        Action action = this.actions.get(actionName);

        PrivilegesEvaluationContext context = new PrivilegesEvaluationContext(user, mappedRoles, action, null, authzConfig.isDebugEnabled(),
                actionRequestIntrospector, resolver);

        PrivilegesEvaluationResult privilegesEvaluationResult = actionAuthorization.hasClusterPermission(context, action);

        return privilegesEvaluationResult.getStatus() == PrivilegesEvaluationResult.Status.OK;
    }

    public boolean hasClusterPermissions(User user, List<String> permissions, TransportAddress callerTransportAddress)
            throws PrivilegesEvaluationException {
        if (permissions.isEmpty()) {
            return true;
        }

        SpecialPrivilegesEvaluationContext specialPrivilegesEvaluationContext = specialPrivilegesEvaluationContextProviderRegistry.provide(user,
                threadContext);

        if (specialPrivilegesEvaluationContext != null) {
            user = specialPrivilegesEvaluationContext.getUser();
        }

        ImmutableSet<String> mappedRoles;
        ActionAuthorization actionAuthorization;

        if (specialPrivilegesEvaluationContext == null) {
            mappedRoles = mapSgRoles(user, callerTransportAddress);
            actionAuthorization = this.actionAuthorization;
        } else {
            mappedRoles = specialPrivilegesEvaluationContext.getMappedRoles();
            actionAuthorization = specialPrivilegesEvaluationContext.getActionAuthorization();
        }

        PrivilegesEvaluationContext context = new PrivilegesEvaluationContext(user, mappedRoles, null, null, authzConfig.isDebugEnabled(),
                actionRequestIntrospector, resolver);

        for (String permission : permissions) {
            PrivilegesEvaluationResult privilegesEvaluationResult = actionAuthorization.hasClusterPermission(context, actions.get(permission));

            if (privilegesEvaluationResult.getStatus() != PrivilegesEvaluationResult.Status.OK) {
                return false;
            }
        }

        return true;
    }

    private boolean checkDocWhitelistHeader(User user, String action, ActionRequest request) {
        String docWhitelistHeader = threadContext.getHeader(ConfigConstants.SG_DOC_WHITELST_HEADER);

        if (docWhitelistHeader == null) {
            return false;
        }

        if (!(request instanceof GetRequest)) {
            return false;
        }

        try {
            DocumentWhitelist documentWhitelist = DocumentWhitelist.parse(docWhitelistHeader);
            GetRequest getRequest = (GetRequest) request;

            if (documentWhitelist.isWhitelisted(getRequest.index(), getRequest.id())) {
                if (log.isDebugEnabled()) {
                    log.debug("Request " + request + " is whitelisted by " + documentWhitelist);
                }

                return true;
            } else {
                return false;
            }

        } catch (Exception e) {
            log.error("Error while handling document whitelist: " + docWhitelistHeader, e);
            return false;
        }
    }

    public PrivilegesInterceptor getPrivilegesInterceptor() {
        return privilegesInterceptor;
    }

    public void setPrivilegesInterceptor(PrivilegesInterceptor privilegesInterceptor) {
        this.privilegesInterceptor = privilegesInterceptor;
    }

    private static ConfigConstants.RolesMappingResolution getRolesMappingResolution(Settings settings) {
        try {
            return ConfigConstants.RolesMappingResolution.valueOf(
                    settings.get(ConfigConstants.SEARCHGUARD_ROLES_MAPPING_RESOLUTION, ConfigConstants.RolesMappingResolution.MAPPING_ONLY.toString())
                            .toUpperCase());
        } catch (Exception e) {
            log.error("Cannot apply roles mapping resolution", e);
            return ConfigConstants.RolesMappingResolution.MAPPING_ONLY;
        }
    }

    public RoleBasedActionAuthorization getActionAuthorization() {
        return actionAuthorization;
    }

    public DocumentAuthorization getDocumentAuthorization() {
        return documentAuthorization;
    }

    public RoleMapping.InvertedIndex getRoleMapping() {
        return roleMapping;
    }

    public ConfigConstants.RolesMappingResolution getRolesMappingResolution() {
        return rolesMappingResolution;
    }

    public ActionGroup.FlattenedIndex getActionGroups() {
        return actionAuthorization.getActionGroups();
    }

    public ClusterService getClusterService() {
        return clusterService;
    }

    public IndexNameExpressionResolver getResolver() {
        return resolver;
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }

    public boolean isDebugEnabled() {
        return authzConfig.isDebugEnabled();
    }

    private static Pattern getPatternFromSettings(Settings settings, String key, List<String> defaultValue) {
        try {
            return Pattern.create(settings.getAsList(key, defaultValue));
        } catch (ConfigValidationException | SettingsException e) {
            log.error("Invalid settings option " + key, e);
            return Pattern.createUnchecked(defaultValue);
        }
    }
}
