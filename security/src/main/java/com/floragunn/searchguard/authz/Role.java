/*
 * Copyright 2015-2022 floragunn GmbH
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

package com.floragunn.searchguard.authz;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.floragunn.codova.config.templates.Template;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.documents.Parser;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.ValidationResult;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.sgconf.Hideable;
import com.floragunn.searchguard.sgconf.StaticDefinable;
import com.floragunn.searchguard.support.Pattern;

public class Role implements Document<Role>, Hideable, StaticDefinable {

    public static ValidationResult<Role> parse(DocNode docNode, Parser.Context context) {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors, context);

        boolean reserved = vNode.get("reserved").withDefault(false).asBoolean();
        boolean hidden = vNode.get("hidden").withDefault(false).asBoolean();
        boolean isStatic = vNode.get("static").withDefault(false).asBoolean();

        // Just for validation:
        vNode.get("cluster_permissions").by(Pattern::parse);
        vNode.get("exclude_cluster_permissions").by(Pattern::parse);
        ImmutableList<String> clusterPermissions = ImmutableList.of(vNode.get("cluster_permissions").asList().withEmptyListAsDefault().ofStrings());
        ImmutableList<String> excludeClusterPermissions = ImmutableList
                .of(vNode.get("exclude_cluster_permissions").asList().withEmptyListAsDefault().ofStrings());

        ImmutableList<Index> indexPermissions = ImmutableList
                .of(vNode.get("index_permissions").asList().ofObjectsParsedBy((Parser<Index, Parser.Context>) Index::new));
        ImmutableList<Tenant> tenantPermissions = ImmutableList
                .of(vNode.get("tenant_permissions").asList().ofObjectsParsedBy((Parser<Tenant, Parser.Context>) Tenant::new));
        ImmutableList<ExcludeIndex> excludeIndexPermissions = ImmutableList
                .of(vNode.get("exclude_index_permissions").asList().ofObjectsParsedBy((Parser<ExcludeIndex, Parser.Context>) ExcludeIndex::new));
        String description = vNode.get("description").asString();

        vNode.checkForUnusedAttributes();

        return new ValidationResult<Role>(new Role(docNode, reserved, hidden, isStatic, description, clusterPermissions, indexPermissions,
                tenantPermissions, excludeClusterPermissions, excludeIndexPermissions), validationErrors);
    }

    private static final Logger log = LogManager.getLogger(RoleMapping.class);

    private final DocNode source;
    private final boolean reserved;
    private final boolean hidden;
    private final boolean isStatic;

    private final String description;
    private final ImmutableList<String> clusterPermissions;
    private final ImmutableList<Index> indexPermissions;
    private final ImmutableList<Tenant> tenantPermissions;
    private final ImmutableList<String> excludeClusterPermissions;
    private final ImmutableList<ExcludeIndex> excludeIndexPermissions;

    public Role(DocNode source, boolean reserved, boolean hidden, boolean isStatic, String description, ImmutableList<String> clusterPermissions,
            ImmutableList<Index> indexPermissions, ImmutableList<Tenant> tenantPermissions, ImmutableList<String> excludeClusterPermissions,
            ImmutableList<ExcludeIndex> excludeIndexPermissions) {
        super();
        this.source = source;
        this.reserved = reserved;
        this.isStatic = isStatic;
        this.hidden = hidden;
        this.description = description;
        this.clusterPermissions = clusterPermissions;
        this.indexPermissions = indexPermissions;
        this.tenantPermissions = tenantPermissions;
        this.excludeClusterPermissions = excludeClusterPermissions;
        this.excludeIndexPermissions = excludeIndexPermissions;
    }

    @Override
    public Object toBasicObject() {
        return source;
    }

    public boolean isReserved() {
        return reserved;
    }

    public boolean isHidden() {
        return hidden;
    }

    public static class Index {

        private final ImmutableList<Template<Pattern>> indexPatterns;

        private final Template<String> dls;
        private final ImmutableList<String> fls;
        private final ImmutableList<String> maskedFields;
        private final ImmutableList<String> allowedActions;

        Index(DocNode docNode, Parser.Context context) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors, context);

            this.dls = vNode.get("dls").asTemplate();
            this.fls = ImmutableList.of(vNode.get("fls").asListOfStrings());
            this.maskedFields = ImmutableList.of(vNode.get("masked_fields").asListOfStrings());

            // Just for validation: 
            vNode.get("allowed_actions").by(Pattern::parse);
            vNode.get("fls").asList().ofObjectsParsedByString(FlsPattern::new);

            this.allowedActions = ImmutableList.of(vNode.get("allowed_actions").asList().withEmptyListAsDefault().ofStrings());
            this.indexPatterns = ImmutableList.of(vNode.get("index_patterns").asList().withEmptyListAsDefault().ofTemplates(Pattern::create));

            validationErrors.throwExceptionForPresentErrors();
        }

        public Index(ImmutableList<Template<Pattern>> indexPatterns, Template<String> dls, ImmutableList<String> fls,
                ImmutableList<String> maskedFields, ImmutableList<String> allowedActions) {
            this.indexPatterns = indexPatterns;
            this.dls = dls;
            this.fls = fls;
            this.maskedFields = maskedFields;
            this.allowedActions = allowedActions;
        }

        public ImmutableList<Template<Pattern>> getIndexPatterns() {
            return indexPatterns;
        }

        public Template<String> getDls() {
            return dls;
        }

        public ImmutableList<String> getFls() {
            return fls;
        }

        public ImmutableList<String> getMaskedFields() {
            return maskedFields;
        }

        public ImmutableList<String> getAllowedActions() {
            return allowedActions;
        }

        static class FlsPattern {
            boolean negate;
            Pattern pattern;

            FlsPattern(String string) throws ConfigValidationException {
                if (string.startsWith("~") || string.startsWith("!")) {
                    negate = true;
                    pattern = Pattern.create(string.substring(1));
                } else {
                    pattern = Pattern.create(string);
                }
            }
        }

    }

    public static class Tenant {

        private final ImmutableList<Template<Pattern>> tenantPatterns;
        private final ImmutableList<String> allowedActions;

        public Tenant(DocNode docNode, Parser.Context context) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors, context);
            vNode.get("allowed_actions").by(Pattern::parse);

            this.tenantPatterns = ImmutableList.of(vNode.get("tenant_patterns").asList().withEmptyListAsDefault().ofTemplates(Pattern::create));
            this.allowedActions = ImmutableList.of(vNode.get("allowed_actions").asList().withEmptyListAsDefault().ofStrings());

            vNode.checkForUnusedAttributes();
            validationErrors.throwExceptionForPresentErrors();
        }

        public Tenant(ImmutableList<Template<Pattern>> tenantPatterns, ImmutableList<String> allowedActions) {
            this.tenantPatterns = tenantPatterns;
            this.allowedActions = allowedActions;
        }

        public ImmutableList<Template<Pattern>> getTenantPatterns() {
            return tenantPatterns;
        }

        public ImmutableList<String> getAllowedActions() {
            return allowedActions;
        }

    }

    public static class ExcludeIndex {

        private final ImmutableList<Template<Pattern>> indexPatterns;
        private final ImmutableList<String> actions;

        public ExcludeIndex(DocNode docNode, Parser.Context context) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors, context);

            vNode.get("actions").by(Pattern::parse);

            this.indexPatterns = ImmutableList.of(vNode.get("index_patterns").asList().withEmptyListAsDefault().ofTemplates(Pattern::create));
            this.actions = ImmutableList.of(vNode.get("actions").asList().withEmptyListAsDefault().ofStrings());

            vNode.checkForUnusedAttributes();
            validationErrors.throwExceptionForPresentErrors();
        }

        public ExcludeIndex(ImmutableList<Template<Pattern>> indexPatterns, ImmutableList<String> actions) {
            this.indexPatterns = indexPatterns;
            this.actions = actions;
        }

        public ImmutableList<Template<Pattern>> getIndexPatterns() {
            return indexPatterns;
        }

        public ImmutableList<String> getActions() {
            return actions;
        }
    }

    public ImmutableList<String> getClusterPermissions() {
        return clusterPermissions;
    }

    public ImmutableList<String> getExcludeClusterPermissions() {
        return excludeClusterPermissions;
    }

    public ImmutableList<Index> getIndexPermissions() {
        return indexPermissions;
    }

    public ImmutableList<ExcludeIndex> getExcludeIndexPermissions() {
        return excludeIndexPermissions;
    }

    public ImmutableList<Tenant> getTenantPermissions() {
        return tenantPermissions;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public boolean isStatic() {
        return isStatic;
    }
}
