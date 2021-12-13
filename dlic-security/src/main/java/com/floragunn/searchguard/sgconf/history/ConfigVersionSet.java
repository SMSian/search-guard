/*
 * Copyright 2020 by floragunn GmbH - All rights reserved
 * 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 
 * This software is free of charge for non-commercial and academic use. 
 * For commercial use in a production environment you have to obtain a license 
 * from https://floragunn.com
 * 
 */

package com.floragunn.searchguard.sgconf.history;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;
import com.floragunn.searchsupport.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.config.validation.InvalidAttributeValue;
import com.floragunn.searchsupport.config.validation.ValidationError;
import com.floragunn.searchsupport.config.validation.ValidationErrors;

public class ConfigVersionSet implements Iterable<ConfigVersion>, ToXContentObject, Writeable, Serializable {

    private static final long serialVersionUID = -1526475316346152188L;

    public static final ConfigVersionSet EMPTY = new Builder().build();

    public static Builder with(CType configurationType, long version) {
        return new Builder(configurationType, version);
    }

    public static ConfigVersionSet from(Map<CType, SgDynamicConfiguration<?>> configByType) {
        Builder builder = new Builder();

        for (Map.Entry<CType, SgDynamicConfiguration<?>> entry : configByType.entrySet()) {
            builder.add(entry.getKey(), entry.getValue().getDocVersion());
        }

        return builder.build();
    }

    private Map<CType, ConfigVersion> versionMap;

    public ConfigVersionSet(Map<CType, ConfigVersion> versionMap) {
        this.versionMap = Collections.unmodifiableMap(versionMap);
    }

    public ConfigVersionSet(Collection<ConfigVersion> versions) {
        Map<CType, ConfigVersion> versionMap = new HashMap<>(versions.size());

        for (ConfigVersion version : versions) {
            versionMap.put(version.getConfigurationType(), version);
        }

        this.versionMap = Collections.unmodifiableMap(versionMap);
    }

    public ConfigVersionSet(StreamInput in) throws IOException {
        this(in.readList(ConfigVersion::new));
    }

    public ConfigVersion get(CType configurationType) {
        return versionMap.get(configurationType);
    }

    @Override
    public Iterator<ConfigVersion> iterator() {
        return versionMap.values().iterator();
    }

    public int size() {
        return versionMap.size();
    }

    @Override
    public int hashCode() {
        return versionMap.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == null || !(other instanceof ConfigVersionSet)) {
            return false;
        }

        return this.versionMap.equals(((ConfigVersionSet) other).versionMap);
    }

    @Override
    public String toString() {
        return versionMap.values().toString();
    }

    public static class Builder {
        private Map<CType, ConfigVersion> versionMap = new HashMap<>();

        public Builder() {
        }

        private Builder(CType configurationType, long version) {
            add(configurationType, version);
        }

        public Builder add(CType configurationType, long version) {
            versionMap.put(configurationType, new ConfigVersion(configurationType, version));

            return this;
        }

        public Builder add(ConfigVersion configurationVersion) {
            versionMap.put(configurationVersion.getConfigurationType(), configurationVersion);

            return this;
        }

        public Builder and(CType configurationType, long version) {
            add(configurationType, version);

            return this;
        }

        public ConfigVersionSet build() {
            return new ConfigVersionSet(versionMap);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray();
        for (ConfigVersion configVersion : this) {
            configVersion.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    public XContentBuilder toCompactXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        for (ConfigVersion configVersion : this) {
            builder.field(configVersion.getConfigurationType().name(), configVersion.getVersion());
        }
        builder.endObject();
        return builder;
    }

    public static ConfigVersionSet parse(JsonNode jsonNode) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        Builder builder = new Builder();

        if (jsonNode.isArray()) {
            for (JsonNode subNode : jsonNode) {
                try {
                    builder.add(ConfigVersion.parse(subNode));
                } catch (ConfigValidationException e) {
                    validationErrors.add("_", e);
                }
            }
        } else if (jsonNode.isObject()) {
            ObjectNode objectNode = (ObjectNode) jsonNode;

            Iterator<String> fieldNames = objectNode.fieldNames();

            while (fieldNames.hasNext()) {
                String fieldName = fieldNames.next();
                CType configType;

                try {
                    configType = CType.valueOf(fieldName);
                } catch (Exception e) {
                    validationErrors.add(new ValidationError(fieldName, "Not a valid config type: " + fieldName, null).cause(e));
                    continue;
                }

                JsonNode value = objectNode.get(fieldName);

                if (value == null || !value.isNumber()) {
                    validationErrors.add(new InvalidAttributeValue(fieldName, value, "A version number"));
                    continue;
                }

                builder.add(configType, value.asLong());
            }
        } else {
            validationErrors.add(new ValidationError(null, "Unexpected type " + jsonNode));
        }

        validationErrors.throwExceptionForPresentErrors();

        return builder.build();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(new ArrayList<>(this.versionMap.values()));
    }

}
