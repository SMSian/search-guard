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

package com.floragunn.dlic.auth.http.jwt;

import com.floragunn.searchguard.auth.HTTPAuthenticator;
import com.floragunn.searchguard.user.AuthCredentials;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.PathNotFoundException;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.io.Decoders;
import io.jsonwebtoken.security.WeakKeyException;
import net.minidev.json.JSONArray;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import java.nio.file.Path;
import java.security.*;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

public class HTTPJwtAuthenticator implements HTTPAuthenticator {

    
    protected final Logger log = LogManager.getLogger(this.getClass());
    
    private static final String BEARER = "bearer ";
    private final JwtParser jwtParser;
    private final String jwtHeaderName;
    private final String jwtUrlParameter;
    private final String rolesKey;
    private final String subjectKey;
    private final String jsonSubjectPath;
    private final String jsonRolesPath;
    private Configuration jsonPathConfig;

    public HTTPJwtAuthenticator(final Settings settings, final Path configPath) {
        super();

        JwtParser _jwtParser = null;
        
        try {
            String signingKey = settings.get("signing_key");
            
            if(signingKey == null || signingKey.length() == 0) {
                log.error("signingKey must not be null or empty. JWT authentication will not work");
            } else {

                signingKey = signingKey.replace("-----BEGIN PUBLIC KEY-----\n", "");
                signingKey = signingKey.replace("-----END PUBLIC KEY-----", "");

                byte[] decoded = Decoders.BASE64.decode(signingKey);
                Key key = null;

                try {
                    key = getPublicKey(decoded, "RSA");
                } catch (Exception e) {
                    log.debug("No public RSA key, try other algos ({})", e.toString());
                }

                try {
                    key = getPublicKey(decoded, "EC");
                } catch (Exception e) {
                    log.debug("No public ECDSA key, try other algos ({})", e.toString());
                }

                if(key != null) {
                    _jwtParser = Jwts.parser().setSigningKey(key);
                } else {
                    _jwtParser = Jwts.parser().setSigningKey(decoded);
                }

            }  
        } catch (Throwable e) {
            log.error("Error creating JWT authenticator: "+e+". JWT authentication will not work", e);
        }
        
        jwtUrlParameter = settings.get("jwt_url_parameter");
        jwtHeaderName = settings.get("jwt_header","Authorization");
        rolesKey = settings.get("roles_key");
        subjectKey = settings.get("subject_key");
        jsonRolesPath = settings.get("roles_path");
        jsonSubjectPath = settings.get("subject_path");
        jwtParser = _jwtParser;

        if ((subjectKey != null && jsonSubjectPath != null) || (rolesKey != null && jsonRolesPath != null)) {
            throw new IllegalStateException("Both, subject_key and subject_path or roles_key and roles_path have simultaneously provided." +
                    " Please provide only one combination.");
        }

        jsonPathConfig = Configuration.builder().options(Option.ALWAYS_RETURN_LIST).build();
    }
    
    @Override
    public AuthCredentials extractCredentials(RestRequest request, ThreadContext context) throws ElasticsearchSecurityException {
        final SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        return AccessController.doPrivileged((PrivilegedAction<AuthCredentials>) () -> extractCredentials0(request));
    }

    private AuthCredentials extractCredentials0(final RestRequest request) {        
        if (jwtParser == null) {
            log.error("Missing Signing Key. JWT authentication will not work");
            return null;
        }
        
        String jwtToken = request.header(jwtHeaderName);
        
        if((jwtToken == null || jwtToken.isEmpty()) && jwtUrlParameter != null) {
            jwtToken = request.param(jwtUrlParameter);
        } else {
            //just consume to avoid "contains unrecognized parameter"
            request.param(jwtUrlParameter);
        }
        
        if (jwtToken == null || jwtToken.length() == 0) {
            if(log.isDebugEnabled()) {
                log.debug("No JWT token found in '{}' {} header", jwtUrlParameter==null?jwtHeaderName:jwtUrlParameter, jwtUrlParameter==null?"header":"url parameter");
            }
            return null;
        }
        
        final int index;
        if((index = jwtToken.toLowerCase().indexOf(BEARER)) > -1) { //detect Bearer 
            jwtToken = jwtToken.substring(index+BEARER.length());
        } else {
            if(log.isDebugEnabled()) {
        	    log.debug("No Bearer scheme found in header");
            }
        }
        
        try {
            final Claims claims = jwtParser.parseClaimsJws(jwtToken).getBody();
            
            final String subject = extractSubject(claims, request);
            
            if (subject == null) {
            	log.error("No subject found in JWT token");
            	return null;
            }
            
            final String[] roles = extractRoles(claims);
            
            final AuthCredentials ac = new AuthCredentials(subject, roles).markComplete(); 
            
            for(Entry<String, Object> claim: claims.entrySet()) {
                ac.addAttribute("attr.jwt."+claim.getKey(), String.valueOf(claim.getValue()));
            }
            
            return ac;        
            
        } catch (WeakKeyException e) {
            log.error("Cannot authenticate user with JWT because of "+e, e);
            return null;
        } catch (Exception e) {
            if(log.isDebugEnabled()) {
                log.debug("Invalid or expired JWT token.", e);
            }
            return null;
        }
    }

    @Override
    public boolean reRequestAuthentication(final RestChannel channel, AuthCredentials creds) {
        final BytesRestResponse wwwAuthenticateResponse = new BytesRestResponse(RestStatus.UNAUTHORIZED,"");
        wwwAuthenticateResponse.addHeader("WWW-Authenticate", "Bearer realm=\"Search Guard\"");
        channel.sendResponse(wwwAuthenticateResponse);
        return true;
    }

    @Override
    public String getType() {
        return "jwt";
    }
    
    protected String extractSubject(final Claims claims, final RestRequest request) {
        String subject = claims.getSubject();        
        if(subjectKey != null) {
    		// try to get roles from claims, first as Object to avoid having to catch the ExpectedTypeException
            Object subjectObject = claims.get(subjectKey, Object.class);
            if(subjectObject == null) {
                log.warn("Failed to get subject from JWT claims, check if subject_key '{}' is correct.", subjectKey);
                return null;
            }
        	// We expect a String. If we find something else, convert to String but issue a warning
            if(!(subjectObject instanceof String)) {
        		log.warn("Expected type String for roles in the JWT for subject_key {}, but value was '{}' ({}). Will convert this value to String.", subjectKey, subjectObject, subjectObject.getClass());    					
            }
            subject = String.valueOf(subjectObject);
        }
        else if (jsonSubjectPath != null) {
            try {
                subject = JsonPath.read(claims, jsonSubjectPath);
            } catch (PathNotFoundException e) {
                log.error("The provided JSON path {} could not be found ", jsonSubjectPath);
            }
        }
        return subject;
    }
    
    protected String[] extractRoles(final Claims claims) {
    	if(rolesKey == null && jsonRolesPath == null) {
    		return new String[0];
    	}

    	if (jsonRolesPath != null) {
            try {
                Object roles = JsonPath.using(jsonPathConfig).parse(claims).read(jsonRolesPath);
                return splitRoles(roles);
            } catch (PathNotFoundException e) {
                log.error("The provided JSON path {} could not be found ", jsonRolesPath);
                return null;
            }
        }
    	// try to get roles from claims, first as Object to avoid having to catch the ExpectedTypeException
    	final Object rolesObject = claims.get(rolesKey, Object.class);
    	if(rolesObject == null) {
    		log.warn("Failed to get roles from JWT claims with roles_key '{}'. Check if this key is correct and available in the JWT payload.", rolesKey);   
    		return new String[0];
    	}

        return splitRoles(rolesObject);
    }

    private static PublicKey getPublicKey(final byte[] keyBytes, final String algo) throws NoSuchAlgorithmException, InvalidKeySpecException {
        X509EncodedKeySpec spec = new X509EncodedKeySpec(keyBytes);
        KeyFactory kf = KeyFactory.getInstance(algo);
        return kf.generatePublic(spec);
    }

    private String[] splitRoles(Object rolesObject) {
        String[] roles = String.valueOf(rolesObject).split(",");

        // We expect a String or Collection. If we find something else, convert to
        // String but issue a warning
        if (!(rolesObject instanceof String) && !(rolesObject instanceof Collection<?>)) {
            log.warn(
                    "Expected type String or Collection for roles in the JWT for roles_key {}, but value was '{}' ({}). Will convert this value to String.",
                    rolesKey, rolesObject, rolesObject.getClass());
        } else if (rolesObject instanceof JSONArray) {
            List<String> roles0 = new ArrayList<>();
            for (Object o : ((JSONArray) rolesObject)) {
                if (o instanceof List) {
                    for (Object oo : (List) o) {
                        roles0.addAll(Arrays.asList(String.valueOf(oo).split(",")));
                    }
                } else {
                    roles0.addAll(Arrays.asList(String.valueOf(o).split(",")));
                }
            }
            roles = roles0.toArray(new String[0]);
        } else if (rolesObject instanceof Collection<?>) {
            roles = ((Collection<String>) rolesObject).toArray(new String[0]);
        }

        for (int i = 0; i < roles.length; i++) {
            roles[i] = roles[i].trim();
        }

        return roles;
    }
}
