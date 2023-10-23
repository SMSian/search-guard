/*
 * Copyright 2023 by floragunn GmbH - All rights reserved
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
package com.floragunn.searchguard.enterprise.femt.datamigration880.service;

public enum StepExecutionStatus {

    OK(true),
    /**
     * Rollback step executed successfully
     */
    ROLLBACK(true),
    INDICES_NOT_FOUND_ERROR,
    UNEXPECTED_ERROR,
    STATUS_INDEX_ALREADY_EXISTS_ERROR,
    CANNOT_CREATE_STATUS_DOCUMENT_ERROR,
    MIGRATION_ALREADY_IN_PROGRESS_ERROR,
    CANNOT_UPDATE_STATUS_DOCUMENT_LOCK_ERROR,
    GLOBAL_TENANT_NOT_FOUND_ERROR,
    MULTI_TENANCY_CONFIG_NOT_AVAILABLE_ERROR,
    MULTI_TENANCY_DISABLED_ERROR,
    CANNOT_RESOLVE_INDEX_BY_ALIAS_ERROR,
    UNHEALTHY_INDICES_ERROR,
    DATA_INDICES_LOCKED_ERROR,
    INVALID_BACKUP_INDEX_NAME_ERROR,
    INVALID_DATE_IN_BACKUP_INDEX_NAME_ERROR,
    WRITE_BLOCK_ERROR,
    WRITE_UNBLOCK_ERROR,
    CANNOT_RETRIEVE_INDICES_STATE_ERROR,
    NO_SOURCE_INDEX_SETTINGS_ERROR,
    CANNOT_LOAD_INDEX_MAPPINGS_ERROR,
    CANNOT_CREATE_INDEX_ERROR,
    CANNOT_BULK_CREATE_DOCUMENT_ERROR,
    DOCUMENT_ALREADY_EXISTS_ERROR,
    DOCUMENT_ALREADY_MIGRATED_ERROR,
    INCORRECT_INDEX_NAME_PREFIX_ERROR,
    CANNOT_REFRESH_INDEX_ERROR,
    UNKNOWN_USER_PRIVATE_TENANT_NAME_ERROR,
    GLOBAL_AND_PRIVATE_TENANT_CONFLICT_ERROR,
    CANNOT_DELETE_INDEX_ERROR,
    REINDEX_BULK_ERROR,
    REINDEX_SEARCH_ERROR,
    REINDEX_TIMEOUT_ERROR,
    BACKUP_UNEXPECTED_OPERATION_ERROR,
    CANNOT_COUNT_DOCUMENTS,
    MISSING_DOCUMENTS_IN_BACKUP_ERROR,
    EMPTY_MAPPINGS_ERROR,
    BACKUP_NOT_FOUND_ERROR,
    BACKUP_DOES_NOT_EXIST_ERROR,
    BACKUP_IS_EMPTY_ERROR,
    BACKUP_CONTAINS_MIGRATED_DATA_ERROR,
    CANNOT_UPDATE_MAPPINGS_ERROR,
    DELETE_ALL_BULK_ERROR,
    DELETE_ALL_SEARCH_ERROR,
    NOT_EMPTY_INDEX,
    REINDEX_DATA_INTO_GLOBAL_TENANT_ERROR,
    MISSING_DOCUMENTS_IN_GLOBAL_TENANT_INDEX_ERROR,
    DELETE_ALL_TIMEOUT_ERROR,
    BACKUP_FROM_PREVIOUS_MIGRATION_NOT_AVAILABLE_ERROR,
    BACKUP_INDICES_CONTAIN_MIGRATION_MARKER,
    SLICE_PARTIAL_ERROR;

    private final boolean success;

    StepExecutionStatus(boolean success) {
        this.success = success;
    }

    StepExecutionStatus() {
        this(false);
    }

    public boolean isSuccess() {
        return success;
    }
}
