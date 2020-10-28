package com.floragunn.searchguard;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestHandler.Route;
import org.elasticsearch.rest.RestHeaderDefinition;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchguard.test.helper.cluster.SimpleRestHandler;
import com.floragunn.searchguard.test.helper.cluster.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.rest.RestHelper;
import com.floragunn.searchguard.test.helper.rest.RestHelper.HttpResponse;

public class SearchGuardInterceptorIntegrationTests {

    @Test
    public void testAllowCustomHeaders() throws Exception {
        try (LocalCluster cluster = new LocalCluster.Builder().nodeSettings(ConfigConstants.SEARCHGUARD_ALLOW_CUSTOM_HEADERS, ".*").singleNode()
                .sslEnabled().plugin(MockActionPlugin.class)
                .user("header_test_user", "secret", new Role("header_test_user_role").indexPermissions("*").on("*").clusterPermissions("*"))
                .build()) {
            Header auth = basicAuth("header_test_user", "secret");
            RestHelper rh = cluster.restHelper();
            HttpResponse httpResponse = rh.executeGetRequest("/_header_test", auth, new BasicHeader("test_header_name", "test_header_value"));
            JsonNode headers = httpResponse.toJsonNode().get("headers");

            Assert.assertEquals("test_header_value", headers.get("test_header_name").textValue());
        }
    }

    public static class MockActionPlugin extends Plugin implements ActionPlugin {

        Settings settings;
        ThreadPool threadPool;

        public MockActionPlugin(final Settings settings, final Path configPath) {
            this.settings = settings;
        }

        public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
            return Arrays.asList(new ActionHandler<>(MockTransportAction.TYPE, MockTransportAction.class));
        }

        public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
                IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
                Supplier<DiscoveryNodes> nodesInCluster) {
            return Arrays.asList(new SimpleRestHandler<>(new Route(Method.GET, "/_header_test"), MockTransportAction.TYPE,
                    (request) -> new MockActionRequest(request.param("id"))));
        }

        public Collection<RestHeaderDefinition> getRestHeaders() {
            return Arrays.asList(new RestHeaderDefinition("test_header_name", true));
        }
    }

    public static class MockTransportAction extends HandledTransportAction<MockActionRequest, MockActionResponse> {
        static ActionType<MockActionResponse> TYPE = new ActionType<>("cluster:admin/header_test", MockActionResponse::new);

        private ThreadPool threadPool;

        @Inject
        public MockTransportAction(final Settings settings, final ThreadPool threadPool, final ClusterService clusterService,
                final TransportService transportService, final AdminDNs adminDNs, final ActionFilters actionFilters) {

            super(TYPE.name(), transportService, actionFilters, MockActionRequest::new);
            this.threadPool = threadPool;
        }

        @Override
        protected void doExecute(Task task, MockActionRequest request, ActionListener<MockActionResponse> listener) {
            listener.onResponse(new MockActionResponse(threadPool.getThreadContext().getHeaders()));
        }
    }

    public static class MockActionRequest extends ActionRequest {

        private String id;

        public MockActionRequest(String id) {
            this.id = id;
        }

        public MockActionRequest(StreamInput in) throws IOException {
            id = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(id);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public String getId() {
            return id;
        }

    }

    public static class MockActionResponse extends ActionResponse implements StatusToXContentObject {

        private Map<String, String> headers;

        public MockActionResponse(Map<String, String> headers) {
            this.headers = headers;
        }

        public MockActionResponse(StreamInput in) throws IOException {
            this.headers = in.readMap(StreamInput::readString, StreamInput::readString);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("headers", headers);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(headers, StreamOutput::writeString, StreamOutput::writeString);
        }

        @Override
        public RestStatus status() {
            return RestStatus.OK;
        }

    }

    private static Header basicAuth(String username, String password) {
        return new BasicHeader("Authorization",
                "Basic " + Base64.getEncoder().encodeToString((username + ":" + Objects.requireNonNull(password)).getBytes(StandardCharsets.UTF_8)));
    }
}
