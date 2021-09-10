/*
 * Copyright 2021 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.authtoken.api;

import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import com.floragunn.searchguard.auth.AuthInfoService;
import com.floragunn.searchguard.authtoken.AuthTokenService;
import com.floragunn.searchguard.privileges.PrivilegesEvaluator;

public class TransportAuthTokenInfoAction extends HandledTransportAction<AuthTokenInfoRequest, AuthTokenInfoResponse> {

    private final AuthTokenService authTokenService;

    @Inject
    public TransportAuthTokenInfoAction(TransportService transportService, ThreadPool threadPool, ActionFilters actionFilters,
            AuthTokenService authTokenService, AuthInfoService authInfoService, PrivilegesEvaluator privilegesEvaluator) {
        super(AuthTokenInfoAction.NAME, transportService, actionFilters, AuthTokenInfoRequest::new);

        this.authTokenService = authTokenService;

    }

    @Override
    protected final void doExecute(Task task, AuthTokenInfoRequest request, ActionListener<AuthTokenInfoResponse> listener) {
        listener.onResponse(new AuthTokenInfoResponse(authTokenService.getConfig().isEnabled(), authTokenService.isInitialized()));

    }
}