package com.floragunn.signals.actions.watch.delete;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.jobs.actions.SchedulerConfigUpdateAction;
import com.floragunn.signals.Signals;
import com.floragunn.signals.SignalsTenant;

public class TransportDeleteWatchAction extends HandledTransportAction<DeleteWatchRequest, DeleteWatchResponse> {

    private final Signals signals;
    private final Client client;
    private final ThreadPool threadPool;

    @Inject
    public TransportDeleteWatchAction(Signals signals, TransportService transportService, ThreadPool threadPool, ActionFilters actionFilters,
            Client client) {
        super(DeleteWatchAction.NAME, transportService, actionFilters, DeleteWatchRequest::new);

        this.signals = signals;
        this.client = client;
        this.threadPool = threadPool;
    }

    @Override
    protected final void doExecute(Task task, DeleteWatchRequest request, ActionListener<DeleteWatchResponse> listener) {
        ThreadContext threadContext = threadPool.getThreadContext();

        User user = threadContext.getTransient(ConfigConstants.SG_USER);

        if (user == null) {
            listener.onResponse(
                    new DeleteWatchResponse(request.getWatchId(), -1, Result.NOOP, RestStatus.UNAUTHORIZED, "Request did not contain user"));
            return;
        }

        SignalsTenant signalsTenant = signals.getTenant(user);

        if (signalsTenant == null) {
            listener.onResponse(new DeleteWatchResponse(request.getWatchId(), -1, Result.NOT_FOUND, RestStatus.NOT_FOUND,
                    "No such tenant: " + (user != null ? user.getRequestedTenant() : null)));
            return;
        }

        Object originalRemoteAddress = threadContext.getTransient(ConfigConstants.SG_REMOTE_ADDRESS);
        Object originalOrigin = threadContext.getTransient(ConfigConstants.SG_ORIGIN);

        try (StoredContext ctx = threadContext.stashContext()) {

            threadContext.putHeader(ConfigConstants.SG_CONF_REQUEST_HEADER, "true");
            threadContext.putTransient(ConfigConstants.SG_USER, user);
            threadContext.putTransient(ConfigConstants.SG_REMOTE_ADDRESS, originalRemoteAddress);
            threadContext.putTransient(ConfigConstants.SG_ORIGIN, originalOrigin);
            client.prepareDelete(signalsTenant.getConfigIndexName(), null, signalsTenant.getWatchIdForConfigIndex(request.getWatchId()))
                    .setRefreshPolicy(RefreshPolicy.IMMEDIATE).execute(new ActionListener<DeleteResponse>() {
                        @Override
                        public void onResponse(DeleteResponse response) {

                            if (response.getResult() == Result.DELETED) {
                                SchedulerConfigUpdateAction.send(client, signalsTenant.getScopedName());
                            }

                            listener.onResponse(new DeleteWatchResponse(request.getWatchId(), response.getVersion(), response.getResult(),
                                    response.status(), null));
                        }

                        @Override
                        public void onFailure(Exception e) {
                            listener.onFailure(e);
                        }
                    });

        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}