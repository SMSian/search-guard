/*
 * Copyright 2023 floragunn GmbH
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
package com.floragunn.signals.api;

import com.google.common.base.Charsets;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

public abstract class SignalsBaseRestHandler extends BaseRestHandler {
    private static final Logger log = LogManager.getLogger(SignalsBaseRestHandler.class);

    protected SignalsBaseRestHandler(Settings settings) {
        super();
    }

    protected void errorResponse(RestChannel channel, RestStatus status, String error) {
        this.errorResponse(channel, status, error, null);
    }

    protected void errorResponse(RestChannel channel, RestStatus status, String error, String detailJsonDocument) {

        try {
            final XContentBuilder builder = channel.newBuilder();
            builder.prettyPrint();
            builder.humanReadable(true);
            builder.startObject();
            builder.field("status", status.getStatus());

            if (error != null) {
                builder.field("error", error);
            }

            if (detailJsonDocument != null) {
                builder.rawField("detail", new ByteArrayInputStream(detailJsonDocument.getBytes(Charsets.UTF_8)), XContentType.JSON);
            }
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(status, builder));
        } catch (Exception e) {
            log.error(e.toString(), e);
            throw ExceptionsHelper.convertToElastic(e);
        }
    }

    protected void errorResponse(RestChannel channel, Exception e) {
        RestStatus status = ExceptionsHelper.status(e);
        errorResponse(channel, status, e.getMessage());
    }

    protected void response(RestChannel channel, RestStatus status) {
        try {
            final XContentBuilder builder = channel.newBuilder();
            builder.prettyPrint();
            builder.humanReadable(true);
            builder.startObject();
            builder.field("status", status.getStatus());

            builder.endObject();
            channel.sendResponse(new BytesRestResponse(status, builder));
        } catch (Exception e) {
            log.error(e.toString(), e);
            throw ExceptionsHelper.convertToElastic(e);
        }
    }

    protected static XContentBuilder convertToJson(RestChannel channel, ToXContent toXContent, ToXContent.Params params) {
        try {
            XContentBuilder builder = channel.newBuilder();
            builder.prettyPrint();
            toXContent.toXContent(builder, params);
            return builder;
        } catch (IOException e) {
            log.error(e.toString(), e);
            throw ExceptionsHelper.convertToElastic(e);
        }
    }

}
