/*
 * Copyright 2021 floragunn GmbH
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

package com.floragunn.searchguard.privileges.extended_action_handling;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.rest.RestStatus;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;

import com.floragunn.searchguard.SearchGuardPlugin.ProtectedIndices;
import com.floragunn.searchguard.privileges.extended_action_handling.ActionConfig.NewResource;
import com.floragunn.searchguard.privileges.extended_action_handling.ActionConfig.Resource;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.cleanup.IndexCleanupAgent;
import com.google.common.base.Objects;

public class ResourceOwnerService {

    public static final Setting<Integer> MAX_CHECK_RETRIES = Setting.intSetting("searchguard.resource_owner_handling.retry_owner_check.max", 1,
            Property.NodeScope, Property.Filtered);
    public static final Setting<Integer> CHECK_RETRY_DELAY = Setting.intSetting("searchguard.resource_owner_handling.retry_owner_check.delay_ms", 10,
            Property.NodeScope, Property.Filtered);
    public static final Setting<TimeValue> CLEANUP_INTERVAL = Setting.timeSetting("searchguard.resource_owner_handling.cleanup_interval",
            TimeValue.timeValueHours(1), TimeValue.timeValueMinutes(1), Property.NodeScope, Property.Filtered);
    public static final Setting<TimeValue> DEFAULT_RESOURCE_LIFETIME = Setting.timeSetting(
            "searchguard.resource_owner_handling.resource.default_lifetime", TimeValue.timeValueDays(7), TimeValue.timeValueMinutes(1),
            Property.NodeScope, Property.Filtered);
    public static final Setting<String> REFRESH_POLICY = Setting.simpleString("searchguard.resource_owner_handling.index.refresh_on_write",
            WriteRequest.RefreshPolicy.IMMEDIATE.getValue(), Property.NodeScope, Property.Filtered);

    public static final List<Setting<?>> SUPPORTED_SETTINGS = Arrays.asList(MAX_CHECK_RETRIES, CHECK_RETRY_DELAY, CLEANUP_INTERVAL,
            DEFAULT_RESOURCE_LIFETIME, REFRESH_POLICY);

    private final static Logger log = LogManager.getLogger(ResourceOwnerService.class);

    private final String index = ".searchguard_resource_owner";
    private final PrivilegedConfigClient privilegedConfigClient;
    private IndexCleanupAgent indexCleanupAgent;

    private final int maxCheckRetries;
    private final long checkRetryDelay;
    private final TimeValue defaultResourceLifetime;
    private final WriteRequest.RefreshPolicy refreshPolicy;

    public ResourceOwnerService(Client client, ClusterService clusterService, ThreadPool threadPool, ProtectedIndices protectedIndices,
            Settings settings) {
        this.privilegedConfigClient = PrivilegedConfigClient.adapt(client);
        this.maxCheckRetries = MAX_CHECK_RETRIES.get(settings);
        this.checkRetryDelay = CHECK_RETRY_DELAY.get(settings);
        this.defaultResourceLifetime = DEFAULT_RESOURCE_LIFETIME.get(settings);
        this.refreshPolicy = WriteRequest.RefreshPolicy.parse(REFRESH_POLICY.get(settings));

        this.indexCleanupAgent = new IndexCleanupAgent(index, CLEANUP_INTERVAL.get(settings), privilegedConfigClient, clusterService, threadPool);

        protectedIndices.add(index);
    }

    public void storeOwner(String resourceType, Object id, User owner, long expires, ActionListener<IndexResponse> actionListener) {
        if (log.isTraceEnabled()) {
            log.trace("storeOwner(" + resourceType + ", " + id + ", " + owner + ", " + expires + ")");
        }

        String docId = resourceType + "_" + id;
        privilegedConfigClient.prepareIndex(index, null, docId).setSource("user_name", owner.getName(), "expires", expires)
                .setRefreshPolicy(refreshPolicy).execute(actionListener);
    }

    public void deleteOwner(String resourceType, Object id) {
        String docId = resourceType + "_" + id;
        privilegedConfigClient.prepareDelete(index, null, docId).execute(new ActionListener<DeleteResponse>() {

            @Override
            public void onResponse(DeleteResponse response) {
                if (log.isTraceEnabled()) {
                    log.trace("Resource owner document deleted: " + docId + "; " + response);
                }
            }

            @Override
            public void onFailure(Exception e) {
                log.error("Error while deleting resource owner document " + docId, e);
            }
        });
    }

    public void checkOwner(String resourceType, Object id, User currentUser, ActionListener<CheckOwnerResponse> actionListener) {
        checkOwner(resourceType, id, currentUser, actionListener, 0);
    }

    private void checkOwner(String resourceType, Object id, User currentUser, ActionListener<CheckOwnerResponse> actionListener, int retry) {
        String docId = resourceType + "_" + id;

        privilegedConfigClient.prepareGet(index, null, docId).execute(new ActionListener<GetResponse>() {

            @Override
            public void onResponse(GetResponse response) {
                if (response.isExists()) {
                    Object storedUserName = response.getSourceAsMap().get("user_name");

                    if (log.isTraceEnabled()) {
                        log.trace("checkOwner for " + resourceType + ":" + id + ": " + storedUserName + " - " + currentUser);
                    }

                    if (isUserEqual(currentUser, storedUserName)) {
                        actionListener.onResponse(new CheckOwnerResponse(response));
                    } else {
                        actionListener.onFailure(new OpenSearchSecurityException(
                                "Resource " + resourceType + ":" + id + " is not owned by user " + currentUser.getName(), RestStatus.FORBIDDEN));
                    }

                } else if (retry < maxCheckRetries) {
                    if (log.isDebugEnabled()) {
                        log.debug("Retrying checkOwner(" + resourceType + ":" + id + ")");
                    }

                    if (checkRetryDelay > 0) {
                        try {
                            Thread.sleep(checkRetryDelay);
                        } catch (InterruptedException e1) {
                        }
                    }

                    checkOwner(resourceType, id, currentUser, actionListener, retry + 1);
                } else {
                    if (log.isTraceEnabled()) {
                        log.trace("checkOwner for " + resourceType + ":" + id + " failed: " + response);
                    }
                    actionListener.onFailure(new OpenSearchSecurityException(
                            "Owner information of " + resourceType + ":" + id + " could not be found", RestStatus.NOT_FOUND));
                }
            }

            @Override
            public void onFailure(Exception e) {
                if (retry < maxCheckRetries) {
                    if (log.isDebugEnabled()) {
                        log.debug("Retrying checkOwner(" + resourceType + ":" + id + ") after " + e, e);
                    }

                    if (checkRetryDelay > 0) {
                        try {
                            Thread.sleep(checkRetryDelay);
                        } catch (InterruptedException e1) {
                        }
                    }

                    checkOwner(resourceType, id, currentUser, actionListener, retry + 1);
                } else {
                    if (log.isWarnEnabled()) {
                        log.warn("checkOwner for " + resourceType + ":" + id + " failed: ", e);
                    }

                    actionListener.onFailure(new OpenSearchException("Checking owner of " + resourceType + ":" + id + " failed", e));
                }
            }
        });
    }

    public <Request extends ActionRequest, Response extends ActionResponse> ActionFilterChain<Request, Response> applyOwnerCheckPreAction(
            ActionConfig<Request, ?, ?> actionConfig, User currentUser, Task task, final String action, Request actionRequest,
            ActionListener<Response> listener, ActionFilterChain<Request, Response> chain) {

        ActionFilterChain<Request, Response> extendedChain = chain;

        for (Resource usesResource : actionConfig.getUsesResources()) {
            Object resourceId = usesResource.getId().apply(actionRequest);

            extendedChain = new OwnerCheckPreAction<Request, Response>(usesResource, resourceId, currentUser, extendedChain);
        }

        return extendedChain;
    }

    public <R extends ActionResponse> ActionListener<R> applyCreatePostAction(ActionConfig<?, ?, ?> actionConfig, User currentUser,
            ActionListener<R> actionListener) {

        return new ActionListener<R>() {

            @Override
            public void onResponse(R actionResponse) {
                NewResource newResource = actionConfig.getCreatesResource();
                Object id = newResource.getId().apply(actionResponse);

                if (log.isTraceEnabled()) {
                    log.trace("Id for new resource " + newResource + ": " + id);
                }

                if (id != null) {

                    long expiresMillis = System.currentTimeMillis() + defaultResourceLifetime.millis();

                    if (newResource.getExpiresAfter() != null) {
                        Instant expiresInstant = newResource.getExpiresAfter().apply(actionResponse);

                        if (expiresInstant != null) {
                            expiresMillis = expiresInstant.toEpochMilli();
                        }
                    }

                    storeOwner(newResource.getType(), id, currentUser, expiresMillis, new ActionListener<IndexResponse>() {

                        @Override
                        public void onResponse(IndexResponse indexResponse) {
                            actionListener.onResponse(actionResponse);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            actionListener.onFailure(new OpenSearchException("Failed to store owner of " + newResource.getType() + ":" + id, e));
                        }

                    });
                } else {
                    actionListener.onResponse(actionResponse);
                }

            }

            @Override
            public void onFailure(Exception e) {
                actionListener.onFailure(e);
            }
        };

    }

    public <Request extends ActionRequest, R extends ActionResponse> ActionListener<R> applyDeletePostAction(ActionConfig<?, ?, ?> actionConfig,
            Resource resource, User currentUser, Task task, final String action, Request actionRequest, ActionListener<R> actionListener) {

        return new ActionListener<R>() {

            @Override
            public void onResponse(R actionResponse) {
                Object id = resource.getId().apply(actionRequest);

                if (id != null) {
                    deleteOwner(resource.getType(), id);
                }

                actionListener.onResponse(actionResponse);
            }

            @Override
            public void onFailure(Exception e) {
                actionListener.onFailure(e);
            }
        };

    }

    private boolean isUserEqual(User currentUser, Object storedUserName) {
        return Objects.equal(currentUser.getName(), storedUserName);
    }

    public void shutdown() {
        this.indexCleanupAgent.shutdown();
    }

    static class CheckOwnerResponse {
        private GetResponse getResponse;

        CheckOwnerResponse(GetResponse getResponse) {
            this.getResponse = getResponse;
        }

        public GetResponse getGetResponse() {
            return getResponse;
        }
    }

    abstract class PreAction<Request extends ActionRequest, Response extends ActionResponse> implements ActionFilterChain<Request, Response> {
        protected final ActionFilterChain<Request, Response> next;

        PreAction(ActionFilterChain<Request, Response> next) {
            this.next = next;
        }

    }

    class OwnerCheckPreAction<Request extends ActionRequest, Response extends ActionResponse> extends PreAction<Request, Response> {
        private final Resource resource;
        private final Object resourceId;
        private final User currentUser;

        OwnerCheckPreAction(Resource resource, Object resourceId, User currentUser, ActionFilterChain<Request, Response> next) {
            super(next);
            this.currentUser = currentUser;
            this.resource = resource;
            this.resourceId = resourceId;
        }

        @Override
        public void proceed(Task task, String action, Request request, ActionListener<Response> listener) {
            checkOwner(resource.getType(), resourceId, currentUser, new ActionListener<CheckOwnerResponse>() {

                @Override
                public void onResponse(CheckOwnerResponse response) {
                    next.proceed(task, action, request, listener);
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }

            });
        }

    }

}
