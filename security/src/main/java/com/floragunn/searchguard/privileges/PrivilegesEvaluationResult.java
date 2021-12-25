/*
 * Copyright 2021 floragunn GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.floragunn.searchguard.privileges;

import java.util.stream.Collectors;

import com.floragunn.searchsupport.util.CheckTable;
import com.floragunn.searchsupport.util.ImmutableSet;

public class PrivilegesEvaluationResult {

    public static final PrivilegesEvaluationResult PASS = new PrivilegesEvaluationResult(Status.PASS);
    public static final PrivilegesEvaluationResult STOP = new PrivilegesEvaluationResult(Status.STOP);
    public static final PrivilegesEvaluationResult PENDING = new PrivilegesEvaluationResult(Status.PENDING);

    private final Status status;
    private final CheckTable<String, String> indexToActionPrivilegeTable;
    private final ImmutableSet<Error> errors;
    private final String reason;

    PrivilegesEvaluationResult(Status status) {
        this.status = status;
        this.indexToActionPrivilegeTable = null;
        this.errors = ImmutableSet.empty();
        this.reason = null;
    }

    PrivilegesEvaluationResult(Status status, String reason, CheckTable<String, String> indexToActionPrivilegeTable, ImmutableSet<Error> errors) {
        this.status = status;
        this.indexToActionPrivilegeTable = indexToActionPrivilegeTable;
        this.errors = errors;
        this.reason = reason;
    }

    public PrivilegesEvaluationResult reason(String reason) {
        return new PrivilegesEvaluationResult(this.status, reason, this.indexToActionPrivilegeTable, this.errors);
    }

    public PrivilegesEvaluationResult reason(String reason, ImmutableSet<Error> errors) {
        return new PrivilegesEvaluationResult(this.status, reason, this.indexToActionPrivilegeTable, errors);
    }

    public PrivilegesEvaluationResult reason(String reason, Error error) {
        return new PrivilegesEvaluationResult(this.status, reason, this.indexToActionPrivilegeTable, ImmutableSet.of(errors));
    }

    public PrivilegesEvaluationResult with(CheckTable<String, String> indexToActionPrivilegeTable) {
        return new PrivilegesEvaluationResult(this.status, this.reason, indexToActionPrivilegeTable, this.errors);
    }

    public PrivilegesEvaluationResult with(CheckTable<String, String> indexToActionPrivilegeTable, ImmutableSet<Error> errors) {
        return new PrivilegesEvaluationResult(this.status, this.reason, indexToActionPrivilegeTable, errors);
    }
    
    public PrivilegesEvaluationResult with(String reason, CheckTable<String, String> indexToActionPrivilegeTable, ImmutableSet<Error> errors) {
        return new PrivilegesEvaluationResult(this.status, reason, indexToActionPrivilegeTable, errors);
    }

    public CheckTable<String, String> getIndexToActionPrivilegeTable() {
        return indexToActionPrivilegeTable;
    }

    public ImmutableSet<Error> getErrors() {
        return errors;
    }

    public Throwable getFirstThrowable() {
        if (errors.isEmpty()) {
            return null;
        }

        for (Error error : errors) {
            if (error.cause != null) {
                return error.cause;
            }
        }

        return null;
    }

    public boolean hasErrors() {
        return !errors.isEmpty();
    }

    public Status getStatus() {
        return status;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder("");

        if (reason != null) {
            result.append("Reason: ").append(reason).append("\n");
        }

        if (indexToActionPrivilegeTable != null) {
            String evaluatedPrivileges = indexToActionPrivilegeTable.toString();

            if (evaluatedPrivileges.length() > 30 || evaluatedPrivileges.contains("\n")) {
                result.append("Evaluated Privileges:\n").append(evaluatedPrivileges).append("\n");
            } else {
                result.append("Evaluated Privileges: ").append(evaluatedPrivileges).append("\n");
            }
        }

        if (errors.size() == 1) {
            result.append("Errors: ").append(errors.only());
        } else if (errors.size() > 1) {
            result.append("Errors:\n").append(errors.stream().map((e) -> " - " + e + "\n").collect(Collectors.toList())).append("\n");
        }

        return result.toString();
    }

    public static enum Status {
        PASS, STOP, PENDING;
    }

    public static class Error {

        private final String message;
        private final Throwable cause;

        public Error(String message, Throwable cause) {
            this.message = message;
            this.cause = cause;
        }

        public String getMessage() {
            return message;
        }

        public Throwable getCause() {
            return cause;
        }

        @Override
        public String toString() {
            if (cause != null) {
                return message + " [" + cause + "]";
            } else {
                return message;
            }
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((message == null) ? 0 : message.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof Error)) {
                return false;
            }
            Error other = (Error) obj;
            if (message == null) {
                if (other.message != null) {
                    return false;
                }
            } else if (!message.equals(other.message)) {
                return false;
            }
            return true;
        }
    }

}
