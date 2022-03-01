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

package com.floragunn.searchguard.configuration;

import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequest;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.threadpool.ThreadPool;

import com.floragunn.searchguard.privileges.ActionRequestIntrospector.ResolvedIndices;
import com.floragunn.searchguard.sgconf.EvaluatedDlsFlsConfig;

public interface DlsFlsRequestValve {

    /**
     * Invoked for calls intercepted by ActionFilter
     * 
     * @param request
     * @param listener
     * @return false to stop
     */
    boolean invoke(String action, ActionRequest request, ActionListener<?> listener, EvaluatedDlsFlsConfig evaluatedDlsFlsConfig, boolean localHasingEnabled,
            ResolvedIndices resolved);

    void handleSearchContext(SearchContext context, ThreadPool threadPool, NamedXContentRegistry namedXContentRegistry);

    public static class NoopDlsFlsRequestValve implements DlsFlsRequestValve {

        @Override
        public boolean invoke(String action, ActionRequest request, ActionListener<?> listener, EvaluatedDlsFlsConfig evaluatedDlsFlsConfig,
                boolean localHasingEnabled, ResolvedIndices resolved) {
            return true;
        }

        @Override
        public void handleSearchContext(SearchContext context, ThreadPool threadPool, NamedXContentRegistry namedXContentRegistry) {

        }
    }

}
