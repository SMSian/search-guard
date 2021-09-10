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

package com.floragunn.searchguard.test.plugin;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.http.HttpServerTransport.Dispatcher;
import org.opensearch.http.netty4.Netty4HttpServerTransport;
import org.opensearch.indices.breaker.CircuitBreakerService;
import org.opensearch.plugins.NetworkPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.SharedGroupFactory;

import com.floragunn.searchguard.support.ConfigConstants;

/**
 * Mimics the behavior of system integrators that run their own plugins (i.e. server transports)
 * in front of Search Guard. This transport just copies the user string from the
 * REST headers to the ThreadContext to test user injection.
 * @author jkressin
 */
public class UserInjectorPlugin extends Plugin implements NetworkPlugin {

    Settings settings;
    ThreadPool threadPool;
    protected final SharedGroupFactory sharedGroupFactory;

    public UserInjectorPlugin(final Settings settings, final Path configPath) {
        this.settings = settings;
        this.sharedGroupFactory = new SharedGroupFactory(settings);
    }

    @Override
    public Map<String, Supplier<HttpServerTransport>> getHttpTransports(Settings settings, ThreadPool threadPool, BigArrays bigArrays,
            PageCacheRecycler pageCacheRecycler, CircuitBreakerService circuitBreakerService, NamedXContentRegistry xContentRegistry,
            NetworkService networkService, Dispatcher dispatcher, ClusterSettings clusterSettings) {

        Map<String, Supplier<HttpServerTransport>> httpTransports = new HashMap<String, Supplier<HttpServerTransport>>(1);
        final UserInjectingDispatcher validatingDispatcher = new UserInjectingDispatcher(dispatcher);
        httpTransports.put("com.floragunn.searchguard.http.UserInjectingServerTransport",
                () -> new UserInjectingServerTransport(settings, networkService, bigArrays, threadPool, xContentRegistry, validatingDispatcher, clusterSettings, sharedGroupFactory));
        return httpTransports;
    }

    class UserInjectingServerTransport extends Netty4HttpServerTransport {

        public UserInjectingServerTransport(final Settings settings, final NetworkService networkService, final BigArrays bigArrays,
                final ThreadPool threadPool, final NamedXContentRegistry namedXContentRegistry, final Dispatcher dispatcher,
                ClusterSettings clusterSettings, SharedGroupFactory sharedGroupFactory) {
            super(settings, networkService, bigArrays, threadPool, namedXContentRegistry, dispatcher, clusterSettings, sharedGroupFactory);
        }
    }

    class UserInjectingDispatcher implements Dispatcher {

        private Dispatcher originalDispatcher;

        public UserInjectingDispatcher(final Dispatcher originalDispatcher) {
            super();
            this.originalDispatcher = originalDispatcher;
        }

        @Override
        public void dispatchRequest(RestRequest request, RestChannel channel, ThreadContext threadContext) {
            threadContext.putTransient(ConfigConstants.SG_INJECTED_USER, request.header(ConfigConstants.SG_INJECTED_USER));
            originalDispatcher.dispatchRequest(request, channel, threadContext);

        }

        @Override
        public void dispatchBadRequest(RestChannel channel, ThreadContext threadContext, Throwable cause) {
            threadContext.putTransient(ConfigConstants.SG_INJECTED_USER, channel.request().header(ConfigConstants.SG_INJECTED_USER));
            originalDispatcher.dispatchBadRequest(channel, threadContext, cause);
        }

    }

}
