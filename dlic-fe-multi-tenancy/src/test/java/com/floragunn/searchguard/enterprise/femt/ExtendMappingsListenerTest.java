package com.floragunn.searchguard.enterprise.femt;

import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.enterprise.femt.tenants.TenantIndexMappingsExtender;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.concurrent.ExecutorService;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class ExtendMappingsListenerTest {

    private static final FeMultiTenancyConfig
        DISABLED =
        new FeMultiTenancyConfig(null, false, "username", "index", true, Collections.emptyList());
    private static final FeMultiTenancyConfig
        ENABLED =
        new FeMultiTenancyConfig(null, true, "username", "index", true, Collections.emptyList());
    @Mock
    private TenantIndexMappingsExtender extender;
    @Mock
    private ExecutorService executorService;
    private ExtendMappingsListener listener;

    @Before
    public void before() {
        this.listener = new ExtendMappingsListener(extender, MoreExecutors.newDirectExecutorService());
    }

    @Test
    public void shouldRequestMultiTenancyConfigurationUpdates() {
        assertThat(listener.getConfigType(), equalTo(FeMultiTenancyConfig.TYPE));
    }

    @Test
    public void shouldExtendMappingsWhenMultitenancyWasEnabled() {
        var oldConfig = SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE, "default", DISABLED);
        var newConfig = SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE, "default", ENABLED);

        listener.onChange(oldConfig, newConfig);

        verify(extender).extendTenantsIndexMappings();
    }

    @Test
    public void shouldNotExtendMappingsWhenMultitenancyIsDisabled() {
        var oldConfig = SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE, "default", DISABLED);
        var newConfig = SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE, "default", DISABLED);

        listener.onChange(oldConfig, newConfig);

        verify(extender, never()).extendTenantsIndexMappings();
    }

    @Test
    public void shouldNotExtendMappingsWhenMultitenancyIsStillEnabled() {
        var oldConfig = SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE, "default", ENABLED);
        var newConfig = SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE, "default", ENABLED);

        listener.onChange(oldConfig, newConfig);

        verify(extender, never()).extendTenantsIndexMappings();
    }

    @Test
    public void shouldNotExtendMappingsWhenMultitenancyWasDisabled() {
        var oldConfig = SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE, "default", ENABLED);
        var newConfig = SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE, "default", DISABLED);

        listener.onChange(oldConfig, newConfig);

        verify(extender, never()).extendTenantsIndexMappings();
    }

    @Test
    public void shouldExtendMappingsWhenMultitenancyWasEnabledAndPreviousStateIsUnknown() {
        var newConfig = SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE, "default", ENABLED);

        listener.onChange(null, newConfig);

        verify(extender).extendTenantsIndexMappings();
    }

    @Test
    public void shouldNotExtendMappingsWhenMultitenancyWasDisabledAndPreviousStateIsUnknown() {
        var newConfig = SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE, "default", DISABLED);

        listener.onChange(null, newConfig);

        verify(extender, never()).extendTenantsIndexMappings();
    }

    @Test
    public void shouldUseExternalExecutor() {
        this.listener = new ExtendMappingsListener(extender, executorService);
        var oldConfig = SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE, "default", DISABLED);
        var newConfig = SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE, "default", ENABLED);

        listener.onChange(oldConfig, newConfig);

        verify(executorService).submit(any(Runnable.class));
    }

}