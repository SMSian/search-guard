package com.floragunn.searchguard.tools.sgadmin;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.apache.http.client.methods.HttpPost;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.Validatable;
import org.opensearch.common.CheckedFunction;

import com.floragunn.codova.documents.DocReader;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.UnexpectedDocumentStructureException;

public class SearchGuardAdminRestClient extends RestHighLevelClient {

    public SearchGuardAdminRestClient(RestClientBuilder restClientBuilder) {
        super(restClientBuilder);
    }

    public GenericResponse reloadHttpCerts() throws OpenSearchStatusException, IOException {
        return performRequest(new EmptyRequest(), r -> new Request(HttpPost.METHOD_NAME, "/_searchguard/api/ssl/http/reloadcerts/"),
                RequestOptions.DEFAULT, ResponseConverters.generic, Collections.emptySet());
    }

    public GenericResponse reloadTransportCerts() throws OpenSearchStatusException, IOException {
        return performRequest(new EmptyRequest(), r -> new Request(HttpPost.METHOD_NAME, "/_searchguard/api/ssl/transport/reloadcerts/"),
                RequestOptions.DEFAULT, ResponseConverters.generic, Collections.emptySet());
    }

    private static class ResponseConverters {

        static CheckedFunction<Response, GenericResponse, IOException> generic = (r) -> {
            try {
                return new GenericResponse(DocReader.json().readObject(r.getEntity().getContent()));
            } catch (DocumentParseException | UnexpectedDocumentStructureException | UnsupportedOperationException e) {
               throw new IOException(e);
            }
        };
    }

    public static class EmptyRequest implements Validatable {

    }

    public static class GenericResponse {
        private String message;
        private String errror;

        public GenericResponse(Map<String, Object> source) {
            this.message = String.valueOf(source.get("message"));
            this.errror = source.get("error") != null ? source.get("error").toString() : null;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        public String getErrror() {
            return errror;
        }

        public void setErrror(String errror) {
            this.errror = errror;
        }

    }
}
