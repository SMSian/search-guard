/*
 * Copyright 2020-2022 floragunn GmbH
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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Strings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.ThreadPool;

import com.floragunn.searchguard.SearchGuardPlugin.ProtectedIndices;
import com.floragunn.searchguard.modules.state.ComponentState;
import com.floragunn.searchsupport.indices.IndexMapping;
import com.google.common.collect.ImmutableMap;

public class ProtectedConfigIndexService {
    private final static Logger log = LogManager.getLogger(ProtectedConfigIndexService.class);

    private final Client client;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final ProtectedIndices protectedIndices;

    private final Set<ConfigIndexState> pendingIndices = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Set<ConfigIndexState> completedIndices = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private volatile boolean ready = false;

    public ProtectedConfigIndexService(Client client, ClusterService clusterService, ThreadPool threadPool, ProtectedIndices protectedIndices) {
        this.client = client;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.protectedIndices = protectedIndices;

        clusterService.addListener(clusterStateListener);
    }

    public ComponentState createIndex(ConfigIndex configIndex) {
        ConfigIndexState configIndexState = new ConfigIndexState(configIndex);

        protectedIndices.add(configIndex.getName());

        if (!ready) {
            pendingIndices.add(configIndexState);
        } else {
            createIndexNow(configIndexState, clusterService.state());
        }

        return configIndexState.moduleState;
    }

    public void flushPendingIndices(ClusterState clusterState) {
        try {
            if (this.pendingIndices.isEmpty()) {
                return;
            }

            Set<ConfigIndexState> pendingIndices = new HashSet<>(this.pendingIndices);

            this.pendingIndices.removeAll(pendingIndices);

            for (ConfigIndexState configIndex : pendingIndices) {
                createIndexNow(configIndex, clusterState);
            }
        } catch (Exception e) {
            log.error("Error in flushPendingIndices()", e);
        }
    }

    public void onNodeStart() {
        ready = true;

        checkClusterState(clusterService.state());
    }

    private void checkClusterState(ClusterState clusterState) {
        if (!ready) {
            return;
        }

        if (log.isTraceEnabled()) {
            log.trace("checkClusterState()\npendingIndices: " + pendingIndices);
        }

        if (clusterState.nodes().isLocalNodeElectedMaster() || clusterState.nodes().getMasterNode() != null) {
            flushPendingIndices(clusterState);
        }

        if (!this.pendingIndices.isEmpty()) {
            threadPool.scheduleUnlessShuttingDown(TimeValue.timeValueSeconds(30), ThreadPool.Names.GENERIC,
                    () -> checkClusterState(clusterService.state()));
        }
    }

    private void createIndexNow(ConfigIndexState configIndex, ClusterState clusterState) {

        if (log.isTraceEnabled()) {
            log.trace("createIndexNow(" + configIndex + ")");
        }

        if (completedIndices.contains(configIndex)) {
            if (log.isTraceEnabled()) {
                log.trace(configIndex + " is already completed");
            }
            return;
        }

        if (clusterState.getMetadata().getIndices().containsKey(configIndex.getName())) {
            if (log.isTraceEnabled()) {
                log.trace(configIndex + " does already exist.");
            }

            if (configIndex.mappingUpdates.size() != 0) {
                int mappingVersion = getMappingVersion(configIndex, clusterState);

                if (log.isTraceEnabled()) {
                    log.trace("Mapping version of index: " + mappingVersion);
                }

                SortedMap<Integer, Map<String, Object>> availableUpdates = configIndex.mappingUpdates.tailMap(mappingVersion);

                if (availableUpdates.size() != 0) {
                    Integer patchFrom = availableUpdates.firstKey();

                    Map<String, Object> patch = configIndex.mappingUpdates.get(patchFrom);

                    if (log.isInfoEnabled()) {
                        log.info("Updating mapping of index " + configIndex.getName() + " from version " + mappingVersion + " to version "
                                + configIndex.mappingVersion);
                    }

                    configIndex.moduleState.setState(ComponentState.State.INITIALIZING, "mapping_update");

                    PutMappingRequest putMappingRequest = new PutMappingRequest(configIndex.getName()).type("_doc").source(patch);

                    if (log.isDebugEnabled()) {
                        log.debug(Strings.toString(putMappingRequest));
                    }

                    client.admin().indices().putMapping(putMappingRequest, new ActionListener<AcknowledgedResponse>() {

                        @Override
                        public void onResponse(AcknowledgedResponse response) {
                            configIndex.moduleState.setState(ComponentState.State.INITIALIZING, "mapping_updated");
                            completedIndices.add(configIndex);
                            configIndex.setCreated(true);

                            if (configIndex.getListener() != null) {
                                configIndex.waitForYellowStatus();
                            } else {
                                configIndex.moduleState.setInitialized();
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            log.error("Mapping update failed for " + configIndex, e);
                            configIndex.moduleState.setFailed(e);
                            configIndex.moduleState.setState(ComponentState.State.FAILED, "mapping_update_failed");

                        }
                    });

                    return;
                }
            }

            completedIndices.add(configIndex);
            configIndex.setCreated(true);

            if (configIndex.getListener() != null) {
                configIndex.waitForYellowStatus();
            } else {
                configIndex.moduleState.setInitialized();
            }

            return;
        }

        if (!clusterState.nodes().isLocalNodeElectedMaster()) {
            pendingIndices.add(configIndex);
            configIndex.moduleState.setState(ComponentState.State.INITIALIZING, "waiting_for_master");
            return;
        }

        CreateIndexRequest request = new CreateIndexRequest(configIndex.getName());

        if (configIndex.getMapping() != null) {
            request.mapping("_doc", configIndex.getMapping());
        }

        request.settings(Settings.builder().put("index.hidden", true));

        if (log.isDebugEnabled()) {
            log.debug("Creating index " + request.index() + ":\n" + Strings.toString(request, true, true));
        }

        completedIndices.add(configIndex);
        configIndex.moduleState.setState(ComponentState.State.INITIALIZING, "creating");

        client.admin().indices().create(request, new ActionListener<CreateIndexResponse>() {

            @Override
            public void onResponse(CreateIndexResponse response) {
                configIndex.setCreated(true);

                if (log.isDebugEnabled()) {
                    log.debug("Created " + configIndex + ": " + Strings.toString(response));
                }

                if (configIndex.getListener() != null) {
                    configIndex.waitForYellowStatus();
                } else {
                    configIndex.moduleState.setInitialized();
                }
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof ResourceAlreadyExistsException) {
                    configIndex.setCreated(true);

                    if (configIndex.getListener() != null) {
                        configIndex.waitForYellowStatus();
                    } else {
                        configIndex.moduleState.setInitialized();
                    }
                } else {
                    log.error("Error while creating index " + configIndex, e);
                    configIndex.setFailed(e);
                }
            }
        });

    }

    private int getMappingVersion(ConfigIndexState configIndex, ClusterState clusterState) {
        IndexMetadata index = clusterState.getMetadata().getIndices().get(configIndex.getName());
        MappingMetadata mapping = index.mapping();

        if (mapping == null) {
            return 0;
        }

        Object meta = mapping.getSourceAsMap().get("_meta");

        if (!(meta instanceof Map)) {
            return 0;
        }

        Object version = ((Map<?, ?>) meta).get("version");

        if (version instanceof Number) {
            return ((Number) version).intValue();
        } else {
            return 0;
        }

    }

    private final ClusterStateListener clusterStateListener = new ClusterStateListener() {

        @Override
        public void clusterChanged(ClusterChangedEvent event) {
            checkClusterState(event.state());
        }
    };

    private class ConfigIndexState {
        private final String name;
        private final Map<String, Object> mapping;
        private final int mappingVersion;
        private final SortedMap<Integer, Map<String, Object>> mappingUpdates;
        private final IndexReadyListener listener;
        private final String[] allIndices;
        private final ComponentState moduleState;
        private volatile long createdAt;

        ConfigIndexState(ConfigIndex configIndex) {
            this.name = configIndex.name;
            this.mapping = configIndex.mapping;
            this.mappingVersion = configIndex.mappingVersion;
            this.listener = configIndex.listener;
            this.moduleState = new ComponentState(5, "index", configIndex.name);
            this.mappingUpdates = configIndex.mappingUpdates;

            if (configIndex.indexDependencies == null || configIndex.indexDependencies.length == 0) {
                allIndices = new String[] { name };
            } else {
                allIndices = new String[configIndex.indexDependencies.length + 1];
                allIndices[0] = name;
                System.arraycopy(configIndex.indexDependencies, 0, allIndices, 1, configIndex.indexDependencies.length);
            }
        }

        public String getName() {
            return name;
        }

        public Map<String, Object> getMapping() {
            return mapping;
        }

        @Override
        public String toString() {
            return "ConfigIndex [name=" + name + "]";
        }

        public void setFailed(Exception failed) {
            this.moduleState.setFailed(failed);
        }

        public void setCreated(boolean created) {
            if (created) {
                this.moduleState.setInitialized();
                this.createdAt = System.currentTimeMillis();
            }
        }

        public IndexReadyListener getListener() {
            return listener;
        }

        public void waitForYellowStatus() {
            if (log.isTraceEnabled()) {
                log.trace("waitForYellowStatus(" + this + ")");
            }

            this.moduleState.setState(ComponentState.State.INITIALIZING, "waiting_for_yellow_status");
            this.moduleState.startNextTry();

            client.admin().cluster().health(new ClusterHealthRequest(allIndices).waitForYellowStatus().timeout(TimeValue.timeValueMinutes(5)),
                    new ActionListener<ClusterHealthResponse>() {

                        @Override
                        public void onResponse(ClusterHealthResponse clusterHealthResponse) {
                            if (clusterHealthResponse.getStatus() == ClusterHealthStatus.YELLOW
                                    || clusterHealthResponse.getStatus() == ClusterHealthStatus.GREEN) {

                                if (log.isDebugEnabled()) {
                                    log.debug(ConfigIndexState.this + " reached status " + Strings.toString(clusterHealthResponse));
                                }

                                threadPool.generic().submit(() -> tryOnIndexReady());

                                return;
                            }

                            if (isTimedOut()) {
                                moduleState.setFailed("Index " + name + " is has not become ready. Giving up");
                                moduleState.setDetailJson(Strings.toString(clusterHealthResponse));
                                log.error("Index " + name + " is has not become ready:\n" + clusterHealthResponse + "\nGiving up.");
                                return;
                            }

                            if (isLate()) {
                                log.error("Index " + name + " is not yet ready:\n" + clusterHealthResponse + "\nRetrying.");
                                moduleState.setDetailJson(Strings.toString(clusterHealthResponse));
                            } else if (log.isTraceEnabled()) {
                                log.trace("Index " + name + " is not yet ready:\n" + clusterHealthResponse + "\nRetrying.");
                            }

                            threadPool.scheduleUnlessShuttingDown(TimeValue.timeValueSeconds(5), ThreadPool.Names.GENERIC,
                                    () -> waitForYellowStatus());
                        }

                        @Override
                        public void onFailure(Exception e) {
                            if (isTimedOut()) {
                                log.error("Index " + name + " is has not become ready. Giving up.", e);
                                moduleState.setFailed(e);
                                return;
                            }

                            if (isLate()) {
                                log.warn("Index " + name + " is not yet ready. Retrying.", e);
                                moduleState.addLastException("waiting_for_yellow_status", e);
                            } else if (log.isTraceEnabled()) {
                                log.trace("Index " + name + " is not yet ready. Retrying.", e);
                            }

                            threadPool.scheduleUnlessShuttingDown(TimeValue.timeValueSeconds(5), ThreadPool.Names.GENERIC,
                                    () -> waitForYellowStatus());
                        }
                    });
        }

        private void tryOnIndexReady() {
            try {
                if (log.isTraceEnabled()) {
                    log.trace("tryOnIndexReady(" + this + ")");
                }

                this.moduleState.setState(ComponentState.State.INITIALIZING, "final_probe");
                this.moduleState.startNextTry();

                listener.onIndexReady(new FailureListener() {

                    @Override
                    public void onFailure(Exception e) {
                        if (isTimedOut()) {
                            log.error("Initialization for " + name + " failed. Giving up.", e);
                            moduleState.setFailed(e);
                            return;
                        }

                        if (isLate()) {
                            log.warn("Initialization for " + name + " not yet successful. Retrying.", e);
                        } else if (log.isTraceEnabled()) {
                            log.trace("Initialization for " + name + " not yet successful. Retrying.", e);
                        }

                        threadPool.scheduleUnlessShuttingDown(TimeValue.timeValueSeconds(5), ThreadPool.Names.GENERIC, () -> tryOnIndexReady());

                    }

                    @Override
                    public void onSuccess() {
                        moduleState.setInitialized();
                    }

                });

            } catch (Exception e) {
                log.error("Error in onIndexReady of " + this, e);
            }
        }

        private boolean isTimedOut() {
            return System.currentTimeMillis() > (createdAt + 24 * 60 * 60 * 1000);
        }

        private boolean isLate() {
            return System.currentTimeMillis() > (createdAt + 60 * 1000);
        }

    }

    public static class ConfigIndex {
        private String name;
        private Map<String, Object> mapping;
        private int mappingVersion;
        private IndexReadyListener listener;
        private String[] indexDependencies = new String[0];
        private SortedMap<Integer, Map<String, Object>> mappingUpdates = new TreeMap<>();

        public ConfigIndex(String name) {
            this.name = name;
        }

        public ConfigIndex mapping(IndexMapping mapping) {
            this.mapping = mapping.toDocNode().toMap();
            return this;
        }

        public ConfigIndex mapping(Map<String, Object> mapping) {
            this.mapping = mapping;
            this.mappingVersion = 1;
            return this;
        }

        public ConfigIndex mapping(Map<String, Object> mapping, int mappingVersion) {
            this.mapping = new HashMap<>(mapping);
            this.mappingVersion = mappingVersion;

            this.mapping.put("_meta", ImmutableMap.of("version", mappingVersion));

            return this;
        }

        public ConfigIndex mappingUpdate(int fromVersion, Map<String, Object> mappingDelta) {
            if (this.mappingVersion == 0) {
                throw new IllegalStateException("A mapping needs to be defined first");
            }

            mappingDelta = new HashMap<>(mappingDelta);
            mappingDelta.put("_meta", ImmutableMap.of("version", this.mappingVersion));
            this.mappingUpdates.put(fromVersion, mappingDelta);
            return this;
        }

        public ConfigIndex onIndexReady(IndexReadyListener listener) {
            this.listener = listener;
            return this;
        }

        public ConfigIndex dependsOnIndices(String... indexDependencies) {
            if (indexDependencies != null) {
                this.indexDependencies = indexDependencies;
            }
            return this;
        }

        public String getName() {
            return name;
        }

        public Map<String, Object> getMapping() {
            return mapping;
        }

        public SortedMap<Integer, Map<String, Object>> getMappingUpdates() {
            return mappingUpdates;
        }

    }

    @FunctionalInterface
    public static interface IndexReadyListener {
        void onIndexReady(FailureListener failureListener);
    }

    public static interface FailureListener {
        void onSuccess();

        void onFailure(Exception e);
    }

}
