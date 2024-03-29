package com.floragunn.searchguard.enterprise.femt;

import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.LocalConfigChangeListener;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.Client;

import java.util.Objects;
import java.util.Optional;

class ExtendMappingsListener implements LocalConfigChangeListener<FeMultiTenancyConfig> {

    private static final Logger log = LogManager.getLogger(ExtendMappingsListener.class);

    private final Client client;

    public ExtendMappingsListener(Client localClient) {
        this.client = Objects.requireNonNull(localClient, "Local client is required");
    }

    @Override
    public CType<FeMultiTenancyConfig> getConfigType() {
        return FeMultiTenancyConfig.TYPE;
    }

    @Override
    public void onChange(SgDynamicConfiguration<FeMultiTenancyConfig> oldConfig, SgDynamicConfiguration<FeMultiTenancyConfig> newConfig) {
        boolean previouslyEnabled = Optional.ofNullable(oldConfig) //
            .map(dynamic -> dynamic.getCEntry("default")) //
            .map(FeMultiTenancyConfig::isEnabled) //
            .orElse(false);
        if(previouslyEnabled) {
            log.debug("MT is already enabled, nothing to be done");
            return;
        }
        boolean newEnabledValue = newConfig.getCEntry("default").isEnabled();
        log.debug("MT enabled flag in the new configuration '{}'", newEnabledValue);
        if(newEnabledValue) {
            log.info("The field sg_tenant will be added to mappings associated with frontend related indices");
        }
    }
}
