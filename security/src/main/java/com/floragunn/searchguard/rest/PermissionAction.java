/*
 * Copyright 2015-2019 floragunn GmbH
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

package com.floragunn.searchguard.rest;

import static org.opensearch.rest.RestRequest.Method.GET;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.opensearch.client.node.NodeClient;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.TransportAddress;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.threadpool.ThreadPool;

import com.floragunn.searchguard.privileges.PrivilegesEvaluator;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.User;
import com.google.common.collect.ImmutableList;

public class PermissionAction extends BaseRestHandler {
    private final PrivilegesEvaluator evaluator;
    private final ThreadContext threadContext;

    public PermissionAction(final Settings settings, final RestController controller,
            final PrivilegesEvaluator evaluator, final ThreadPool threadPool) {
        super();
        this.threadContext = threadPool.getThreadContext();
        this.evaluator = evaluator;
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(GET, "/_searchguard/permission"));
    }
    
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        List<String> permissions = Arrays.asList(request.paramAsStringArray("permissions", new String[0]));
        
        return new RestChannelConsumer() {

            @Override
            public void accept(RestChannel channel) throws Exception {
                
                User user = (User) threadContext.getTransient(ConfigConstants.SG_USER);
                TransportAddress caller = Objects.requireNonNull(
                        (TransportAddress) threadContext.getTransient(ConfigConstants.SG_REMOTE_ADDRESS));

                Map<String, Boolean> evaluationResult = evaluator.evaluateClusterAndTenantPrivileges(user, caller,
                        permissions);
                
                try (XContentBuilder builder = channel.newBuilder()) {
                    builder.startObject();
                    builder.startObject("permissions");

                    for (String permission : permissions) {
                        builder.field(permission, evaluationResult.get(permission));
                    }

                    builder.endObject();
                    builder.endObject();

                    BytesRestResponse response = new BytesRestResponse(RestStatus.OK, builder);
                    channel.sendResponse(response);

                } 

            }
        };
    }

    @Override
    public String getName() {
        return "Permission Action";
    }
}
