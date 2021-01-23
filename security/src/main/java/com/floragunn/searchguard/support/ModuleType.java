/*
 * Copyright 2015-2017 floragunn GmbH
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

package com.floragunn.searchguard.support;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.floragunn.searchguard.auth.HTTPAuthenticator;
import com.floragunn.searchguard.auth.api.AuthenticationBackend;
import com.floragunn.searchguard.auth.api.AuthorizationBackend;
import com.floragunn.searchguard.auth.internal.InternalAuthenticationBackend;
import com.floragunn.searchguard.auth.internal.NoOpAuthenticationBackend;
import com.floragunn.searchguard.auth.internal.NoOpAuthorizationBackend;
import com.floragunn.searchguard.http.HTTPBasicAuthenticator;
import com.floragunn.searchguard.http.HTTPClientCertAuthenticator;
import com.floragunn.searchguard.http.HTTPProxyAuthenticator;
import com.floragunn.searchguard.http.HTTPProxyAuthenticator2;
import com.floragunn.searchguard.ssl.transport.PrincipalExtractor;
import com.floragunn.searchguard.transport.InterClusterRequestEvaluator;

public enum ModuleType implements Serializable {

	REST_MANAGEMENT_API("REST Management API", "com.floragunn.searchguard.dlic.rest.api.SearchGuardRestApiActions", Boolean.TRUE),
	DLSFLS("Document- and Field-Level Security", "com.floragunn.searchguard.configuration.SearchGuardFlsDlsIndexSearcherWrapper", Boolean.TRUE),
	AUDITLOG("Audit Logging", "com.floragunn.searchguard.auditlog.impl.AuditLogImpl", Boolean.TRUE),
	MULTITENANCY("Kibana Multitenancy", "com.floragunn.searchguard.configuration.PrivilegesInterceptorImpl", Boolean.TRUE),
	LDAP_AUTHENTICATION_BACKEND("LDAP authentication backend", "com.floragunn.dlic.auth.ldap.backend.LDAPAuthenticationBackend", Boolean.TRUE),
	LDAP_AUTHORIZATION_BACKEND("LDAP authorization backend", "com.floragunn.dlic.auth.ldap.backend.LDAPAuthorizationBackend", Boolean.TRUE),
	KERBEROS_AUTHENTICATION_BACKEND("Kerberos authentication backend", "com.floragunn.dlic.auth.http.kerberos.HTTPSpnegoAuthenticator", Boolean.TRUE),
	JWT_AUTHENTICATION_BACKEND("JWT authentication backend", "com.floragunn.dlic.auth.http.jwt.HTTPJwtAuthenticator", Boolean.TRUE),
	OPENID_AUTHENTICATION_BACKEND("OpenID authentication backend", "com.floragunn.dlic.auth.http.jwt.keybyoidc.HTTPJwtKeyByOpenIdConnectAuthenticator", Boolean.TRUE),
	SAML_AUTHENTICATION_BACKEND("SAML authentication backend", "com.floragunn.dlic.auth.http.saml.HTTPSamlAuthenticator", Boolean.TRUE),
	INTERNAL_USERS_AUTHENTICATION_BACKEND("Internal users authentication backend", InternalAuthenticationBackend.class.getName(), Boolean.FALSE),
	NOOP_AUTHENTICATION_BACKEND("Noop authentication backend", NoOpAuthenticationBackend.class.getName(), Boolean.FALSE),
	NOOP_AUTHORIZATION_BACKEND("Noop authorization backend", NoOpAuthorizationBackend.class.getName(), Boolean.FALSE),
	HTTP_BASIC_AUTHENTICATOR("HTTP Basic Authenticator", HTTPBasicAuthenticator.class.getName(), Boolean.FALSE),
	HTTP_PROXY_AUTHENTICATOR("HTTP Proxy Authenticator", HTTPProxyAuthenticator.class.getName(), Boolean.FALSE),
	HTTP_PROXY_AUTHENTICATOR2("HTTP Proxy Authenticator 2", HTTPProxyAuthenticator2.class.getName(), Boolean.FALSE),
	HTTP_CLIENTCERT_AUTHENTICATOR("HTTP Client Certificate Authenticator", HTTPClientCertAuthenticator.class.getName(), Boolean.FALSE),
	CUSTOM_HTTP_AUTHENTICATOR("Custom HTTP authenticator", null, Boolean.TRUE),
	CUSTOM_AUTHENTICATION_BACKEND("Custom authentication backend", null, Boolean.TRUE),
	CUSTOM_AUTHORIZATION_BACKEND("Custom authorization backend", null, Boolean.TRUE),
	CUSTOM_INTERCLUSTER_REQUEST_EVALUATOR("Intercluster Request Evaluator", null, Boolean.FALSE),
	CUSTOM_PRINCIPAL_EXTRACTOR("TLS Principal Extractor", null, Boolean.FALSE),
	//COMPLIANCE("Compliance", "com.floragunn.searchguard.compliance.ComplianceIndexingOperationListenerImpl", Boolean.TRUE),
	UNKNOWN("Unknown type", null, Boolean.TRUE);

	private String description;
	private String defaultImplClass;
	private Boolean isEnterprise = Boolean.TRUE;
	private static Map<String, ModuleType> modulesMap = new HashMap<>();

	static{
		for(ModuleType module : ModuleType.values()) {
			if (module.defaultImplClass != null) {
				modulesMap.put(module.getDefaultImplClass(), module);
			}
		}
	}

	private ModuleType(String description, String defaultImplClass, Boolean isEnterprise) {
		this.description = description;
		this.defaultImplClass = defaultImplClass;
		this.isEnterprise = isEnterprise;
	}

	public static ModuleType getByDefaultImplClass(Class<?> clazz) {
		ModuleType moduleType = modulesMap.get(clazz.getName());
    	if(moduleType == null) {
    		if(HTTPAuthenticator.class.isAssignableFrom(clazz)) {
    			moduleType = ModuleType.CUSTOM_HTTP_AUTHENTICATOR;
    		}

    		if(AuthenticationBackend.class.isAssignableFrom(clazz)) {
    			moduleType = ModuleType.CUSTOM_AUTHENTICATION_BACKEND;
    		}

    		if(AuthorizationBackend.class.isAssignableFrom(clazz)) {
    			moduleType = ModuleType.CUSTOM_AUTHORIZATION_BACKEND;
    		}

    		if(AuthorizationBackend.class.isAssignableFrom(clazz)) {
    			moduleType = ModuleType.CUSTOM_AUTHORIZATION_BACKEND;
    		}

    		if(InterClusterRequestEvaluator.class.isAssignableFrom(clazz)) {
    			moduleType = ModuleType.CUSTOM_INTERCLUSTER_REQUEST_EVALUATOR;
    		}

    		if(PrincipalExtractor.class.isAssignableFrom(clazz)) {
    			moduleType = ModuleType.CUSTOM_PRINCIPAL_EXTRACTOR;
    		}
    	}
    	if(moduleType == null) {
    		moduleType = ModuleType.UNKNOWN;
    	}
    	return moduleType;
	}

	public String getDescription() {
		return this.description;
	}

	public String getDefaultImplClass() {
		return defaultImplClass;
	}

	public Boolean isEnterprise() {
		return isEnterprise;
	}


}
