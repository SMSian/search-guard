/*
 * Copyright 2023 floragunn GmbH
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
package com.floragunn.signals.actions.watch.put;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.diag.DiagnosticContext;
import com.floragunn.signals.NoSuchTenantException;
import com.floragunn.signals.Signals;
import com.floragunn.signals.SignalsTenant;
import com.floragunn.signals.SignalsUnavailableException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportPutWatchAction extends HandledTransportAction<PutWatchRequest, PutWatchResponse> {
    private static final Logger log = LogManager.getLogger(TransportPutWatchAction.class);

    private final Signals signals;

    private final ThreadPool threadPool;

    @Inject
    public TransportPutWatchAction(Signals signals, TransportService transportService, ScriptService scriptService, ThreadPool threadPool,
            ActionFilters actionFilters) {
        super(PutWatchAction.NAME, transportService, actionFilters, PutWatchRequest::new);

        this.signals = signals;
        this.threadPool = threadPool;
    }

    @Override
    protected final void doExecute(Task task, PutWatchRequest request, ActionListener<PutWatchResponse> listener) {

        try {
            ThreadContext threadContext = threadPool.getThreadContext();

            User user = threadContext.getTransient(ConfigConstants.SG_USER);

            if (user == null) {
                throw new Exception("Request did not contain user");
            }

            SignalsTenant signalsTenant = signals.getTenant(user);

            threadPool.generic().submit(threadPool.getThreadContext().preserveContext(() -> {
                try {
                    DiagnosticContext.fixupLoggingContext(threadContext);

                    IndexResponse response = signalsTenant.addWatch(request.getWatchId(), request.getBody().utf8ToString(), user);

                    listener.onResponse(
                            new PutWatchResponse(request.getWatchId(), response.getVersion(), response.getResult(), response.status(), null, null));
                } catch (ConfigValidationException e) {
                    log.info("Invalid watch supplied to PUT " + request.getWatchId() + ":\n" + e.toString(), e);
                    listener.onResponse(new PutWatchResponse(request.getWatchId(), -1, Result.NOOP, RestStatus.BAD_REQUEST,
                            "Watch is invalid: " + e.getMessage(), e.getValidationErrors().toJsonString()));
                } catch (Exception e) {
                    log.error("Error while saving watch: ", e);
                    listener.onFailure(e);
                }
            }));
        } catch (NoSuchTenantException e) {
            listener.onResponse(new PutWatchResponse(request.getWatchId(), -1, Result.NOT_FOUND, RestStatus.NOT_FOUND, e.getMessage(), null));
        } catch (SignalsUnavailableException e) {
            listener.onFailure(e.toElasticsearchException());
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
