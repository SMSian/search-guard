/*
 * Copyright 2024 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.enterprise.femt.tenants;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.enterprise.femt.RequestResponseTenantData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.index.IndexNotFoundException;

import java.util.Objects;

public class TenantIndexMappingsExtender {

    private static final Logger log = LogManager.getLogger(TenantIndexMappingsExtender.class);

    private final TenantRepository tenantRepository;

    public TenantIndexMappingsExtender(TenantRepository tenantRepository) {
        this.tenantRepository = Objects.requireNonNull(tenantRepository, "Tenant repository is required");
    }

    public void extendTenantsIndexMappings() {
        try {
            tenantRepository.extendTenantsIndexMappings(getSgTenantFieldMapping());
            log.debug("Successfully extended tenants index field mappings");
        } catch (IndexNotFoundException e) {
            log.debug("An error occurred while trying to extend tenants index field mappings", e);
        }
    }

    public static DocNode getSgTenantFieldMapping() {
        return DocNode.of(RequestResponseTenantData.getSgTenantField(), DocNode.of("type", "keyword"));
    }


}
