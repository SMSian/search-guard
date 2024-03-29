package com.floragunn.searchguard.configuration;

public interface LocalConfigChangeListener<T> {
    CType<T> getConfigType();

    void onChange(SgDynamicConfiguration<T> oldConfig, SgDynamicConfiguration<T> newConfig);
}
