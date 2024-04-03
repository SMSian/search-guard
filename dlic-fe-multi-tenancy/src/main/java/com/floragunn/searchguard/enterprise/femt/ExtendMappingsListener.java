package com.floragunn.searchguard.enterprise.femt;

import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.LocalConfigChangeListener;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.enterprise.femt.tenants.TenantIndexMappingsExtender;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

class ExtendMappingsListener implements LocalConfigChangeListener<FeMultiTenancyConfig> {

    private static final Logger log = LogManager.getLogger(ExtendMappingsListener.class);

    private final TenantIndexMappingsExtender indexMappingsExtender;
    private final ExecutorService executor;

    public ExtendMappingsListener(TenantIndexMappingsExtender indexMappingsExtender, ExecutorService executor) {
        this.indexMappingsExtender = Objects.requireNonNull(indexMappingsExtender, "Tenant index mapping extender is required");
        this.executor = Objects.requireNonNull(executor, "Executor is required");
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
            .orElse(FeMultiTenancyConfig.DEFAULT.isEnabled());
        if (previouslyEnabled) {
            log.debug("MT is already enabled, nothing to be done");
            return;
        }
        boolean newEnabledValue = newConfig.getCEntry("default").isEnabled();
        log.debug("MT enabled flag in the new configuration '{}'", newEnabledValue);
        if (newEnabledValue) {
            extendMappings();
        }
    }

    private void extendMappings() {
        log.info("The field sg_tenant will be added to mappings associated with frontend related indices");
        executor.submit(indexMappingsExtender::extendTenantsIndexMappings);
    }
}
