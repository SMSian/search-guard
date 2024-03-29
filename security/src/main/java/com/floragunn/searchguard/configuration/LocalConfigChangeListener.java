package com.floragunn.searchguard.configuration;

import javax.annotation.Nullable;

public interface LocalConfigChangeListener<T> {
    CType<T> getConfigType();

    void onChange(@Nullable SgDynamicConfiguration<T> oldConfig, SgDynamicConfiguration<T> newConfig);
}
