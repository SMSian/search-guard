/*
  * Copyright 2020 by floragunn GmbH - All rights reserved
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
package com.floragunn.searchguard.authtoken.api;

import org.elasticsearch.action.ActionType;

public class SearchAuthTokensAction extends ActionType<SearchAuthTokensResponse> {

    public static final SearchAuthTokensAction INSTANCE = new SearchAuthTokensAction();
    public static final String NAME = "cluster:admin:searchguard:authtoken/_own/search";
    public static final String NAME_ALL = NAME.replace("/_own/", "/_all/");

    protected SearchAuthTokensAction() {
        super(NAME, in -> {
            SearchAuthTokensResponse response = new SearchAuthTokensResponse(in);
            return response;
        });
    }
}
