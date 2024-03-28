package com.floragunn.searchguard.configuration;

public interface LocalConfigChangeListener {
    CType<?> getConfigType();

    void onChange(SgDynamicConfiguration<?> newConfig);
}
