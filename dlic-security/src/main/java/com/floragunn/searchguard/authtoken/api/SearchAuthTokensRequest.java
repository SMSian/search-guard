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

package com.floragunn.searchguard.authtoken.api;

import java.io.IOException;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.search.Scroll;
import org.opensearch.search.builder.SearchSourceBuilder;

public class SearchAuthTokensRequest extends ActionRequest {

    private SearchSourceBuilder searchSourceBuilder;
    private Scroll scroll;
    private int from = -1;
    private int size = -1;

    public SearchAuthTokensRequest() {
        super();
    }

    public SearchAuthTokensRequest(SearchSourceBuilder searchSourceBuilder, Scroll scroll) {
        super();
        this.searchSourceBuilder = searchSourceBuilder;
        this.scroll = scroll;
    }

    public SearchAuthTokensRequest(StreamInput in) throws IOException {
        super(in);
        scroll = in.readOptionalWriteable(Scroll::new);
        from = in.readInt();
        size = in.readInt();
        searchSourceBuilder = in.readOptionalWriteable(SearchSourceBuilder::new);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(scroll);
        out.writeInt(from);
        out.writeInt(size);
        out.writeOptionalWriteable(searchSourceBuilder);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public SearchSourceBuilder getSearchSourceBuilder() {
        return searchSourceBuilder;
    }

    public Scroll getScroll() {
        return scroll;
    }

    public void setSearchSourceBuilder(SearchSourceBuilder searchSourceBuilder) {
        this.searchSourceBuilder = searchSourceBuilder;
    }

    public void setScroll(Scroll scroll) {
        this.scroll = scroll;
    }

    public int getFrom() {
        return from;
    }

    public void setFrom(int from) {
        this.from = from;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

}
