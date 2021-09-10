package com.floragunn.searchguard.privileges;

import java.util.Set;

import org.opensearch.common.transport.TransportAddress;

import com.floragunn.searchguard.sgconf.SgRoles;
import com.floragunn.searchguard.user.User;

public interface SpecialPrivilegesEvaluationContext {
    User getUser();
    
    Set<String> getMappedRoles();

    SgRoles getSgRoles();
    
    default TransportAddress getCaller() {
        return null;
    }
    
    default boolean requiresPrivilegeEvaluationForLocalRequests() {
        return false;
    }
    
    default boolean isSgConfigRestApiAllowed() {
        return false;
    }
}
