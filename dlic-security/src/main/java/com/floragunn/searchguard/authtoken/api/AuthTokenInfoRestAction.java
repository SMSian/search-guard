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

import static org.opensearch.rest.RestRequest.Method.GET;

import java.io.IOException;
import java.util.List;

import org.opensearch.client.node.NodeClient;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.rest.action.RestStatusToXContentListener;

import com.floragunn.searchsupport.client.rest.Responses;
import com.google.common.collect.ImmutableList;

public class AuthTokenInfoRestAction extends BaseRestHandler {

    public AuthTokenInfoRestAction() {
        super();
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(GET, "/_searchguard/authtoken/_info"));

    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {

        if (request.method() == GET) {
            return handleGet(request.param("id"), client);
        } else {
            return (RestChannel channel) -> Responses.sendError(channel, RestStatus.METHOD_NOT_ALLOWED, "Method not allowed: " + request.method());
        }
    }

    private RestChannelConsumer handleGet(String id, NodeClient client) {
        return (RestChannel channel) -> {

            try {
                client.execute(AuthTokenInfoAction.INSTANCE, new AuthTokenInfoRequest(),
                        new RestStatusToXContentListener<AuthTokenInfoResponse>(channel));
            } catch (Exception e) {
                Responses.sendError(channel, e);
            }
        };
    }

    @Override
    public String getName() {
        return "Search Guard Auth Token Service Info";
    }
}
