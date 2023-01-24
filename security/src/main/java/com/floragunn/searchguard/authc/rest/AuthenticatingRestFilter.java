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
package com.floragunn.searchguard.authc.rest;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.SearchGuardModulesRegistry;
import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.auditlog.AuditLog.Origin;
import com.floragunn.searchguard.authc.base.AuthcResult;
import com.floragunn.searchguard.authc.blocking.BlockedIpRegistry;
import com.floragunn.searchguard.authc.blocking.BlockedUserRegistry;
import com.floragunn.searchguard.authc.legacy.LegacyRestAuthenticationProcessor;
import com.floragunn.searchguard.authc.legacy.LegacySgConfig;
import com.floragunn.searchguard.authz.PrivilegesEvaluator;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigMap;
import com.floragunn.searchguard.configuration.ConfigurationChangeListener;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.ssl.transport.PrincipalExtractor;
import com.floragunn.searchguard.ssl.util.ExceptionUtils;
import com.floragunn.searchguard.ssl.util.SSLRequestHelper;
import com.floragunn.searchguard.ssl.util.SSLRequestHelper.SSLInfo;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.AuthDomainInfo;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.action.RestApi;
import com.floragunn.searchsupport.action.StandardResponse;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentState.State;
import com.floragunn.searchsupport.cstate.ComponentStateProvider;
import com.floragunn.searchsupport.diag.DiagnosticContext;
import java.io.IOException;
import java.nio.file.Path;
import javax.net.ssl.SSLPeerUnverifiedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;

public class AuthenticatingRestFilter implements ComponentStateProvider {

    private static final Logger log = LogManager.getLogger(AuthenticatingRestFilter.class);

    private final AuditLog auditLog;
    private final ThreadContext threadContext;
    private final PrincipalExtractor principalExtractor;
    private final Settings settings;
    private final Path configPath;
    private final DiagnosticContext diagnosticContext;
    private final AdminDNs adminDns;
    private final ComponentState componentState = new ComponentState(1, "authc", "rest_filter");

    private volatile RestAuthenticationProcessor authenticationProcessor;

    public AuthenticatingRestFilter(ConfigurationRepository configurationRepository, SearchGuardModulesRegistry modulesRegistry, AdminDNs adminDns,
            BlockedIpRegistry blockedIpRegistry, BlockedUserRegistry blockedUserRegistry, AuditLog auditLog, ThreadPool threadPool,
            PrincipalExtractor principalExtractor, PrivilegesEvaluator privilegesEvaluator, Settings settings, Path configPath,
            DiagnosticContext diagnosticContext) {
        this.adminDns = adminDns;
        this.auditLog = auditLog;
        this.threadContext = threadPool.getThreadContext();
        this.principalExtractor = principalExtractor;
        this.settings = settings;
        this.configPath = configPath;
        this.diagnosticContext = diagnosticContext;

        configurationRepository.subscribeOnChange(new ConfigurationChangeListener() {

            @Override
            public void onChange(ConfigMap configMap) {
                SgDynamicConfiguration<RestAuthcConfig> config = configMap.get(CType.AUTHC);
                SgDynamicConfiguration<LegacySgConfig> legacyConfig = configMap.get(CType.CONFIG);

                if (config != null && config.getCEntry("default") != null) {
                    RestAuthenticationProcessor authenticationProcessor = new RestAuthenticationProcessor.Default(config.getCEntry("default"),
                            modulesRegistry, adminDns, blockedIpRegistry, blockedUserRegistry, auditLog, threadPool, privilegesEvaluator);

                    AuthenticatingRestFilter.this.authenticationProcessor = authenticationProcessor;

                    componentState.replacePartsWithType("config", config.getComponentState());
                    componentState.replacePartsWithType("rest_authentication_processor", authenticationProcessor.getComponentState());
                    componentState.updateStateFromParts();

                    if (log.isDebugEnabled()) {
                        log.debug("New configuration:\n" + config.getCEntry("default").toYamlString());
                    }
                } else if (legacyConfig != null && legacyConfig.getCEntry("sg_config") != null) {
                    RestAuthenticationProcessor authenticationProcessor = new LegacyRestAuthenticationProcessor(legacyConfig.getCEntry("sg_config"),
                            modulesRegistry, adminDns, blockedIpRegistry, blockedUserRegistry, auditLog, threadPool, privilegesEvaluator);

                    AuthenticatingRestFilter.this.authenticationProcessor = authenticationProcessor;

                    componentState.replacePartsWithType("config", legacyConfig.getComponentState());
                    componentState.replacePartsWithType("rest_authentication_processor", authenticationProcessor.getComponentState());
                    componentState.updateStateFromParts();

                    if (log.isDebugEnabled()) {
                        log.debug("New legacy configuration:\n" + legacyConfig.getCEntry("sg_config").toYamlString());
                    }
                } else {
                    componentState.setState(State.SUSPENDED, "no_configuration");
                }
            }
        });

    }

    public RestHandler wrap(RestHandler original) {
        return new AuthenticatingRestHandler(original);
    }

    class AuthenticatingRestHandler implements RestHandler {
        private final RestHandler original;

        AuthenticatingRestHandler(RestHandler original) {
            this.original = original;
        }

        @Override
        public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
            org.apache.logging.log4j.ThreadContext.clearAll();
            diagnosticContext.traceActionStack(request.getHttpRequest().method() + " " + request.getHttpRequest().uri());

            if (!checkRequest(original, request, channel, client)) {
                return;
            }

            if (isAuthcRequired(request)) {
                String sslPrincipal = threadContext.getTransient(ConfigConstants.SG_SSL_PRINCIPAL);

                // Admin Cert authentication works also without a valid configuration; that's why it is not done inside of AuthenticationProcessor
                if (adminDns.isAdminDN(sslPrincipal)) {
                    // PKI authenticated REST call

                    User user = new User(sslPrincipal, AuthDomainInfo.TLS_CERT);
                    threadContext.putTransient(ConfigConstants.SG_USER, user);
                    auditLog.logSucceededLogin(user, true, null, request);
                    original.handleRequest(request, channel, client);
                    return;
                }

                if (authenticationProcessor == null) {
                    log.error("Not yet initialized (you may need to run sgctl)");
                    channel.sendResponse(new BytesRestResponse(RestStatus.SERVICE_UNAVAILABLE,
                            "Search Guard not initialized (SG11). See https://docs.search-guard.com/latest/sgctl"));
                    return;
                }

                authenticationProcessor.authenticate(original, request, channel, (result) -> {
                    if (authenticationProcessor.isDebugEnabled() && DebugApi.PATH.equals(request.path())) {
                        sendDebugInfo(channel, result);
                        return;
                    }

                    if (result.getStatus() == AuthcResult.Status.PASS) {
                        // make it possible to filter logs by username
                        threadContext.putTransient(ConfigConstants.SG_USER, result.getUser());
                        org.apache.logging.log4j.ThreadContext.clearAll();
                        org.apache.logging.log4j.ThreadContext.put("user", result.getUser() != null ? result.getUser().getName() : null);

                        try {
                            original.handleRequest(request, channel, client);
                        } catch (Exception e) {
                            log.error("Error in " + original, e);
                            try {
                                channel.sendResponse(new BytesRestResponse(channel, e));
                            } catch (IOException e1) {
                                log.error(e1);
                            }
                        }
                    } else {
                        org.apache.logging.log4j.ThreadContext.remove("user");

                        if (result.getRestStatus() != null && result.getRestStatusMessage() != null) {
                            BytesRestResponse response;

                            if (isJsonResponseRequested(request)) {
                                response = new BytesRestResponse(result.getRestStatus(), "application/json", DocNode.of(//
                                        "status", result.getRestStatus().getStatus(), //
                                        "error.reason", result.getRestStatusMessage(), //
                                        "error.type", "authentication_exception" //
                                ).toJsonString());
                            } else {
                                response = new BytesRestResponse(result.getRestStatus(), result.getRestStatusMessage());
                            }

                            if (!result.getHeaders().isEmpty()) {
                                result.getHeaders().forEach((k, v) -> v.forEach((e) -> response.addHeader(k, e)));
                            }

                            channel.sendResponse(response);
                        }
                    }
                }, (e) -> {
                    try {
                        channel.sendResponse(new BytesRestResponse(channel, e));
                    } catch (IOException e1) {
                        log.error(e1);
                    }
                });
            } else {
                original.handleRequest(request, channel, client);

            }
        }

        private boolean isJsonResponseRequested(RestRequest request) {
            String accept = request.header("accept");

            return accept != null && (accept.startsWith("application/json") || accept.startsWith("application/vnd.elasticsearch+json"));
        }

        private boolean isAuthcRequired(RestRequest request) {
            return request.method() != Method.OPTIONS && !"/_searchguard/license".equals(request.path())
                    && !"/_searchguard/license".equals(request.path()) && !"/_searchguard/health".equals(request.path())
                    && !("/_searchguard/auth/session".equals(request.path()) && request.method() == Method.POST);
        }

        private boolean checkRequest(RestHandler restHandler, RestRequest request, RestChannel channel, NodeClient client) throws Exception {

            threadContext.putTransient(ConfigConstants.SG_ORIGIN, Origin.REST.toString());

            if (containsBadHeader(request)) {
                final ElasticsearchException exception = ExceptionUtils.createBadHeaderException();
                log.error(exception);
                auditLog.logBadHeaders(request);
                channel.sendResponse(new BytesRestResponse(channel, RestStatus.FORBIDDEN, exception));
                return false;
            }

            if (SSLRequestHelper.containsBadHeader(threadContext, ConfigConstants.SG_CONFIG_PREFIX)) {
                final ElasticsearchException exception = ExceptionUtils.createBadHeaderException();
                log.error(exception);
                auditLog.logBadHeaders(request);
                channel.sendResponse(new BytesRestResponse(channel, RestStatus.FORBIDDEN, exception));
                return false;
            }

            final SSLInfo sslInfo;
            try {
                if ((sslInfo = SSLRequestHelper.getSSLInfo(settings, configPath, request, principalExtractor)) != null) {
                    if (sslInfo.getPrincipal() != null) {
                        threadContext.putTransient("_sg_ssl_principal", sslInfo.getPrincipal());
                    }

                    if (sslInfo.getX509Certs() != null) {
                        threadContext.putTransient("_sg_ssl_peer_certificates", sslInfo.getX509Certs());
                    }
                    threadContext.putTransient("_sg_ssl_protocol", sslInfo.getProtocol());
                    threadContext.putTransient("_sg_ssl_cipher", sslInfo.getCipher());
                }
            } catch (SSLPeerUnverifiedException e) {
                log.error("No ssl info", e);
                auditLog.logSSLException(request, e);
                channel.sendResponse(new BytesRestResponse(channel, RestStatus.FORBIDDEN, e));
                return false;
            }

            return true;
        }

        private void sendDebugInfo(RestChannel channel, AuthcResult authcResult) {
            RestResponse response = new BytesRestResponse(authcResult.getRestStatus(), "application/json", authcResult.toPrettyJsonString());
            if (!authcResult.getHeaders().isEmpty()) {
                authcResult.getHeaders().forEach((k, v) -> v.forEach((e) -> response.addHeader(k, e)));
            }
            channel.sendResponse(response);
        }

    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }

    public static class DebugApi extends RestApi {
        public static final String PATH = "/_searchguard/auth/debug";

        public DebugApi() {
            name(PATH);
            handlesGet(PATH).with((r, c) -> {
                return (RestChannel channel) -> {
                    channel.sendResponse(new StandardResponse(404,
                            new StandardResponse.Error("The /_searchguard/auth/debug API is not enabled. See the sg_authc configuration."))
                                    .toRestResponse());
                };
            });
        }

    }

    private static boolean containsBadHeader(RestRequest request) {
        for (String key : request.getHeaders().keySet()) {
            if (key != null && key.trim().toLowerCase().startsWith(ConfigConstants.SG_CONFIG_PREFIX.toLowerCase())) {
                return true;
            }
        }

        return false;
    }
}
