package com.floragunn.searchguard.modules;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.plugins.ActionPlugin.ActionHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchguard.DefaultObjectMapper;
import com.floragunn.searchguard.modules.SearchGuardModule.BaseDependencies;
import com.floragunn.searchguard.sgconf.DynamicConfigFactory;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;
import com.floragunn.searchsupport.config.validation.ConfigValidationException;

public class SearchGuardModulesRegistry {
    public static final SearchGuardModulesRegistry INSTANCE = new SearchGuardModulesRegistry();

    private static final Logger log = LogManager.getLogger(SearchGuardModulesRegistry.class);

    private List<SearchGuardModule<?>> subModules = new ArrayList<>();

    private SearchGuardModulesRegistry() {

    }

    public void add(String... classes) {
        for (String clazz : classes) {
            try {
                Object object = Class.forName(clazz).getDeclaredConstructor().newInstance();

                if (object instanceof SearchGuardModule) {
                    subModules.add((SearchGuardModule<?>) object);
                } else {
                    log.error(object + " does not implement SearchGuardSubModule");
                }

            } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException
                    | SecurityException | ClassNotFoundException e) {
                log.error("Error while instantiating " + clazz, e);
            }
        }
    }

    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<DiscoveryNodes> nodesInCluster) {
        List<RestHandler> result = new ArrayList<>();

        for (SearchGuardModule<?> module : subModules) {
            result.addAll(module.getRestHandlers(settings, restController, clusterSettings, indexScopedSettings, settingsFilter,
                    indexNameExpressionResolver, nodesInCluster));
        }

        return result;
    }

    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> result = new ArrayList<>();

        for (SearchGuardModule<?> module : subModules) {
            result.addAll(module.getActions());
        }

        return result;
    }

    public List<ScriptContext<?>> getContexts() {
        List<ScriptContext<?>> result = new ArrayList<>();

        for (SearchGuardModule<?> module : subModules) {
            result.addAll(module.getContexts());
        }

        return result;
    }

    public Collection<Object> createComponents(BaseDependencies baseDependencies) {
        List<Object> result = new ArrayList<>();

        for (SearchGuardModule<?> module : subModules) {
            result.addAll(module.createComponents(baseDependencies));
            
            registerConfigChangeListener(module, baseDependencies.getDynamicConfigFactory());
        }
        
        return result;
    }

    public List<Setting<?>> getSettings() {
        List<Setting<?>> result = new ArrayList<>();

        for (SearchGuardModule<?> module : subModules) {
            result.addAll(module.getSettings());
        }

        return result;
    }
    
    @SuppressWarnings("unchecked")
    private void registerConfigChangeListener(SearchGuardModule<?> module, DynamicConfigFactory dynamicConfigFactory) {
        SearchGuardModule.SgConfigMetadata<?> configMetadata = module.getSgConfigMetadata();
        
        if (configMetadata == null) {
            return;
        }
        
        dynamicConfigFactory.addConfigChangeListener(configMetadata.getSgConfigType(), (config) -> {
            Object convertedConfig = convert(configMetadata, config);
            
            @SuppressWarnings("rawtypes")
            Consumer consumer = configMetadata.getConfigConsumer();
            
            consumer.accept(convertedConfig);
        });
    }
        
    private <T> T convert(SearchGuardModule.SgConfigMetadata<T> configMetadata, SgDynamicConfiguration<?> value) {
        if (value == null) {
            return null;
        }
        
        Object entry = value.getCEntry(configMetadata.getEntry());

        if (entry == null) {
            return null;
        }

        JsonNode subNode = DefaultObjectMapper.objectMapper.valueToTree(entry).at(configMetadata.getJsonPointer());

        if (subNode == null || subNode.isMissingNode()) {
            return null;
        }

        try {
            return configMetadata.getConfigParser().parse(subNode);
        } catch (ConfigValidationException e) {
            log.error("Error while parsing configuration in " + this, e);
            return null;
        }
    }
    
    
}
