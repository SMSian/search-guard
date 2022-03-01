/*
 * Copyright 2015-2018 floragunn GmbH
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

import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;

import com.floragunn.searchguard.privileges.ActionRequestIntrospector.ActionRequestInfo;
import com.floragunn.searchguard.sgconf.SgRoles;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.util.ImmutableSet;

public class TermsAggregationEvaluator {

    protected final Logger log = LogManager.getLogger(this.getClass());

    private static final ImmutableSet<String> READ_ACTIONS = ImmutableSet.of("indices:data/read/msearch", "indices:data/read/mget",
            "indices:data/read/get", "indices:data/read/search", "indices:data/read/field_caps*");
    
    private static final QueryBuilder NONE_QUERY = new MatchNoneQueryBuilder();
    
    public TermsAggregationEvaluator() {
    }
    
    public PrivilegesEvaluatorResponse evaluate(ActionRequestInfo requestInfo, final ActionRequest request, ClusterService clusterService, User user, SgRoles sgRoles,  IndexNameExpressionResolver resolver, PrivilegesEvaluatorResponse presponse, ActionRequestIntrospector actionRequestIntrospector) {
        try {
            
            if(request instanceof SearchRequest) {
                SearchRequest sr = (SearchRequest) request;

                if(     sr.source() != null
                        && sr.source().query() == null
                        && sr.source().aggregations() != null
                        && sr.source().aggregations().getAggregatorFactories() != null
                        && sr.source().aggregations().getAggregatorFactories().size() == 1
                        && sr.source().size() == 0) {
                   AggregationBuilder ab = sr.source().aggregations().getAggregatorFactories().iterator().next();
                   if(     ab instanceof TermsAggregationBuilder
                           && "terms".equals(ab.getType())
                           && "indices".equals(ab.getName())) {
                       if("_index".equals(((TermsAggregationBuilder) ab).field())
                               && ab.getPipelineAggregations().isEmpty()
                               && ab.getSubAggregations().isEmpty()) {

                           final Set<String> allPermittedIndices = sgRoles.getAllPermittedIndicesForKibana(requestInfo, user, READ_ACTIONS, resolver, clusterService, actionRequestIntrospector);
                           if(allPermittedIndices == null || allPermittedIndices.isEmpty()) {
                               sr.source().query(NONE_QUERY);
                           } else {
                               sr.source().query(new TermsQueryBuilder("_index", allPermittedIndices));
                           }
                           
                           presponse.allowed = true;
                           return presponse.markComplete();
                       }
                   }
                }
            }
        } catch (Exception e) {
            log.warn("Unable to evaluate terms aggregation",e);
            return presponse;
        }
        
        return presponse;
    }
}
