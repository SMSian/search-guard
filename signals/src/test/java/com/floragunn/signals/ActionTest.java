package com.floragunn.signals;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.ScriptService;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.floragunn.searchguard.DefaultObjectMapper;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchguard.test.helper.file.FileHelper;
import com.floragunn.searchguard.test.helper.network.SocketUtils;
import com.floragunn.searchsupport.config.elements.InlineMustacheTemplate;
import com.floragunn.searchsupport.config.validation.ConfigValidationException;
import com.floragunn.signals.accounts.AccountRegistry;
import com.floragunn.signals.execution.ActionExecutionException;
import com.floragunn.signals.execution.ExecutionEnvironment;
import com.floragunn.signals.execution.WatchExecutionContext;
import com.floragunn.signals.execution.WatchExecutionContextData;
import com.floragunn.signals.support.NestedValueMap;
import com.floragunn.signals.watch.action.handlers.ActionExecutionResult;
import com.floragunn.signals.watch.action.handlers.IndexAction;
import com.floragunn.signals.watch.action.handlers.WebhookAction;
import com.floragunn.signals.watch.action.handlers.email.EmailAccount;
import com.floragunn.signals.watch.action.handlers.email.EmailAction;
import com.floragunn.signals.watch.action.handlers.email.EmailAction.Attachment;
import com.floragunn.signals.watch.action.handlers.slack.SlackAccount;
import com.floragunn.signals.watch.action.handlers.slack.SlackAction;
import com.floragunn.signals.watch.action.handlers.slack.SlackActionConf;
import com.floragunn.signals.watch.action.invokers.ActionInvocationType;
import com.floragunn.signals.watch.common.HttpClientConfig;
import com.floragunn.signals.watch.common.HttpRequestConfig;
import com.floragunn.signals.watch.common.TlsClientAuthConfig;
import com.floragunn.signals.watch.common.TlsConfig;
import com.floragunn.signals.watch.init.WatchInitializationService;
import com.google.common.collect.ImmutableMap;
import com.icegreen.greenmail.util.GreenMail;
import com.icegreen.greenmail.util.GreenMailUtil;
import com.icegreen.greenmail.util.ServerSetup;

import net.jcip.annotations.NotThreadSafe;

@PowerMockIgnore({ "javax.script.*", "javax.crypto.*", "javax.management.*", "sun.security.*", "java.security.*", "javax.net.ssl.*", "javax.net.*",
        "javax.security.*" })
@RunWith(PowerMockRunner.class)
@PrepareForTest(AccountRegistry.class)
@NotThreadSafe
public class ActionTest {

    private final static String ROOT_CA_CERT = "-----BEGIN CERTIFICATE-----\n" + "MIIDyDCCArCgAwIBAgIBATANBgkqhkiG9w0BAQsFADB1MRMwEQYKCZImiZPyLGQB\n"
            + "GRYDY29tMRcwFQYKCZImiZPyLGQBGRYHZXhhbXBsZTEaMBgGA1UECgwRRXhhbXBs\n"
            + "ZSBDb20sIEluYy4xCzAJBgNVBAsMAkNBMRwwGgYDVQQDDBNyb290LmNhLmV4YW1w\n"
            + "bGUuY29tMB4XDTE4MDcyMjA4MzIxNloXDTI4MDcxOTA4MzIxNlowdTETMBEGCgmS\n"
            + "JomT8ixkARkWA2NvbTEXMBUGCgmSJomT8ixkARkWB2V4YW1wbGUxGjAYBgNVBAoM\n"
            + "EUV4YW1wbGUgQ29tLCBJbmMuMQswCQYDVQQLDAJDQTEcMBoGA1UEAwwTcm9vdC5j\n"
            + "YS5leGFtcGxlLmNvbTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAJC0\n"
            + "QIvdszKl5IMDurJdaFnWTW7Kcmoos91dDjrD3WxdnEnDTgXskX7UuW7VPc1uuXyU\n"
            + "cK1eqYq0XJWqqxtU+ufW/BUb8wZr5I5gm0RdnQQUfBj+qDR4ACKE3XbG2hC/G+iN\n"
            + "Lz70EHQFukGMQkdMtnday9t5K2FN0rEs1H/B1G3C6ynlR9437gYRvwsS9WrJJ+Yj\n"
            + "D8pN277oQ23px9R3OjCVstV0cCmlVkjHncI4b6NGrscG4baOcGOlmzVuTf9orzFs\n"
            + "eg81B2ZiGE4uTyMnbbO3uYKhP/8bw4001Tx1VdDEHwTIDIYzkgGR+RWZFcKRKwSk\n"
            + "Vfvm8oRb+VakdpTeniECAwEAAaNjMGEwDwYDVR0TAQH/BAUwAwEB/zAfBgNVHSME\n"
            + "GDAWgBQ2TT9qHHGaxBJWtwnEixPl+xE7SzAdBgNVHQ4EFgQUNk0/ahxxmsQSVrcJ\n"
            + "xIsT5fsRO0swDgYDVR0PAQH/BAQDAgGGMA0GCSqGSIb3DQEBCwUAA4IBAQB3JoH4\n"
            + "PPM/49C5PuyUR6lh9+L7T38cEW5fOzwj3qFTD5P3N9ZpM87ivMgykOKYEVTnqFyL\n"
            + "RG5KIlEUQ0/6oSRKgdBa9G+ahaW3dbJ0Z7INkk4PJKnwG8+XDJIr3Gi8zDPrsYy/\n"
            + "WwCSQMlZ7bc04PDkvl8c4cETQWcVYJGWH0Fd/y35ATvO43V9KcXv8Fs4Pzx6S/Ma\n"
            + "zA0bO/sKwCb1ZI1wUHdGyk83k/ONgcdBMlta37piVdeLXv02w+gWhg0kvZY5UZjm\n"
            + "kS+ZGrFX+2Txu4N/JWHTBIEOD768G0aWR9pspgAEg+eiLRxY/qHqorCMmfHuXKz7\n" + "H7j2LXdTXQ6Aduk6\n" + "-----END CERTIFICATE-----\n" + "";

    private final static String KIRK_CERT = "-----BEGIN CERTIFICATE-----\n" //
            + "MIIEZjCCA06gAwIBAgIGAWTBHiXPMA0GCSqGSIb3DQEBCwUAMHgxEzARBgoJkiaJ\n"
            + "k/IsZAEZFgNjb20xFzAVBgoJkiaJk/IsZAEZFgdleGFtcGxlMRowGAYDVQQKDBFF\n"
            + "eGFtcGxlIENvbSwgSW5jLjELMAkGA1UECwwCQ0ExHzAdBgNVBAMMFnNpZ25pbmcu\n"
            + "Y2EuZXhhbXBsZS5jb20wHhcNMTgwNzIyMDgzMjE3WhcNMjgwNzE5MDgzMjE3WjBz\n"
            + "MRMwEQYKCZImiZPyLGQBGRYDY29tMRcwFQYKCZImiZPyLGQBGRYHZXhhbXBsZTEa\n"
            + "MBgGA1UECgwRRXhhbXBsZSBDb20sIEluYy4xDDAKBgNVBAsMA09wczEZMBcGA1UE\n"
            + "AwwQa2lyay5leGFtcGxlLmNvbTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoC\n"
            + "ggEBAMiwtCG4/mhIVpJgKlB5q2Rc8I+qOt8cBF9N08LwrpWwY0SMmuYdINOIlLVV\n"
            + "mCvRrh4PMyveDwmVEvIzK17/btE3W7hZtxllPnGUGAkJf6ciWTxFO4kxGS3Ojfl3\n"
            + "JnBA6nWU6/r8JfqhIa6A/bQR7lO84JSgEML6bxP35NeFgI6gZOsqH2RXdr8VEucJ\n"
            + "MXHGYhbLXq26TeNIqPgSW0MjNFJtvnJgAycfkSh0qkTBCk7ZrvX0jI4kcd47cPHg\n"
            + "kc/jt7MzWUc1KuB0pBY2ug6p9r2+pDT1dawlifH2y7K5mlyHJ0nj0VhPi0DaieNV\n"
            + "/EwopLs+TOe+Pz8RHSliyaPFqM8CAwEAAaOB+jCB9zCBnwYDVR0jBIGXMIGUgBQF\n"
            + "4IV2bYTghGOHzsbkqwZcTWu/vKF5pHcwdTETMBEGCgmSJomT8ixkARkWA2NvbTEX\n"
            + "MBUGCgmSJomT8ixkARkWB2V4YW1wbGUxGjAYBgNVBAoMEUV4YW1wbGUgQ29tLCBJ\n"
            + "bmMuMQswCQYDVQQLDAJDQTEcMBoGA1UEAwwTcm9vdC5jYS5leGFtcGxlLmNvbYIB\n"
            + "AjAdBgNVHQ4EFgQUYnhPfZqEVltKK5aEjhMYjISX0TQwDAYDVR0TAQH/BAIwADAO\n"
            + "BgNVHQ8BAf8EBAMCBeAwFgYDVR0lAQH/BAwwCgYIKwYBBQUHAwIwDQYJKoZIhvcN\n"
            + "AQELBQADggEBADZgGQP6io4giHoHlbIXc1gGTLcNN//2DyAO/W+Oq+CCMcMzPOHJ\n"
            + "3g7KmN257LN6sCGzqGTRZGpaYOnL4PMhxku62oGxs25puxHzG9lsVZ9shUr4f73N\n"
            + "qyxFer6GcVTehgKO5KgA1yR0MXH1vUQ6WqstjrnfiKZ+OLQvvkoczpASi+y81baG\n"
            + "kyDVLo96dfAiUv254q8IlEsO83scsdt7W4INHmHP45K79sr1lcxRC1jvAbOg5eGA\n"
            + "7aDD/Ve5lkR9eoXU6iYYfT/p2RjgDSDn7MJwBLuN1FSmB28bpp5Me9gPZTtT3uW+\n"//
            + "1k4enV7iJWXE8009a6Z0Ouwm2Bg68Wj7TAQ=\n" //
            + "-----END CERTIFICATE-----\n"//
            + "-----BEGIN CERTIFICATE-----\n"//
            + "MIIEUTCCAzmgAwIBAgIBAjANBgkqhkiG9w0BAQsFADB1MRMwEQYKCZImiZPyLGQB\n"
            + "GRYDY29tMRcwFQYKCZImiZPyLGQBGRYHZXhhbXBsZTEaMBgGA1UECgwRRXhhbXBs\n"
            + "ZSBDb20sIEluYy4xCzAJBgNVBAsMAkNBMRwwGgYDVQQDDBNyb290LmNhLmV4YW1w\n"
            + "bGUuY29tMB4XDTE4MDcyMjA4MzIxNloXDTI4MDcxOTA4MzIxNloweDETMBEGCgmS\n"
            + "JomT8ixkARkWA2NvbTEXMBUGCgmSJomT8ixkARkWB2V4YW1wbGUxGjAYBgNVBAoM\n"
            + "EUV4YW1wbGUgQ29tLCBJbmMuMQswCQYDVQQLDAJDQTEfMB0GA1UEAwwWc2lnbmlu\n"
            + "Zy5jYS5leGFtcGxlLmNvbTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEB\n"
            + "AK7YSOq+e6VdPmfh7CH4PoJ1Oy4bhtAwLReuT+BEzOfo5FGUoGhJ3TAhG//92BwL\n"
            + "sDem89BfxDnpUmkL2mO/lO969yjVGfd+wb5/PjIao0v5B+9tFTwZnezf5RISjJc8\n"
            + "qR55HeATt+xaRYmj7Wcdhe361p4GcnVgtNdslxzULW7+aRUZz7dkGtuxUHWALZWd\n"
            + "Um4owpZPcVbC0CF642CuxkGYLh8uolFPowrGuL39KdtWohiQOUi0nfoYWFpzhUn/\n"
            + "etLnyLmjEq41a2rykgiFC7qDW6wtvSbUbXg/DqPKiq5AIMR/R4HJ6bNEQJixsGqf\n"
            + "VC9NG2v5+hCeTJdRTyFOBNcCAwEAAaOB6DCB5TASBgNVHRMBAf8ECDAGAQH/AgEA\n"
            + "MIGfBgNVHSMEgZcwgZSAFDZNP2occZrEEla3CcSLE+X7ETtLoXmkdzB1MRMwEQYK\n"
            + "CZImiZPyLGQBGRYDY29tMRcwFQYKCZImiZPyLGQBGRYHZXhhbXBsZTEaMBgGA1UE\n"
            + "CgwRRXhhbXBsZSBDb20sIEluYy4xCzAJBgNVBAsMAkNBMRwwGgYDVQQDDBNyb290\n"
            + "LmNhLmV4YW1wbGUuY29tggEBMB0GA1UdDgQWBBQF4IV2bYTghGOHzsbkqwZcTWu/\n"
            + "vDAOBgNVHQ8BAf8EBAMCAYYwDQYJKoZIhvcNAQELBQADggEBADfPaMOnKpTLQPGW\n"
            + "vCvuAXvXnKUS2sI1l59mcVuMcVQnwND2viKQ92irL0jxyNLvnqC7S8ad135GvGVs\n"
            + "iFUWKEANg7EsSjKREuAKQYD2GmsYwIJjw2GMp0gpIJxocrzUfCDgXyDy2fZouvvw\n"
            + "M4UWds3G6zwWJWtCksRrzUTe8FoUdLW/+1HA4aaK3Y+6cQfYcgMvJZvJa9hGPuqm\n"
            + "uXvw6qfsmzNM+7EFEfJW70BoyuWWbo+/ft7wsZnOhQzVnbsMHHdOHnT4Ylr+fcvf\n"
            + "ONKfJy8w2D+YGQXdHXdDGZlcGfwykoTLjjS+SnOShqoCUiAngFNCoIOzxct9ViW5\n" //
            + "pDGaM90=\n" //
            + "-----END CERTIFICATE-----\n";

    private final static String KIRK_KEY = "-----BEGIN ENCRYPTED PRIVATE KEY-----\n"
            + "MIIE9jAoBgoqhkiG9w0BDAEDMBoEFJXPhgGfl3qpK2ps9gqNUx35uHCaAgIIAASC\n"
            + "BMjmI3bvVJNwKmLDaj2z4MRqn4h99ktK8mAd3rFH65QRHP0VrbTJ7ymLTsWCEwXr\n"
            + "QUjR87tDsgtsEFoazXv0HATPVLkAwQzHwDkps1WSaphh2MG/05QSpdMYqP8yxKVg\n"
            + "HdSOabKlwh8IQIA3QDCefCYwM+jRx1hw1B1hMXabUtqN1EENdNp6bZ76qxoiPyFm\n"
            + "zq3yruaBS0CjexYbdF1wOjIAtoTkD2v/B+kiVUlz+k12nK9Wk3uf4OHL26gMI/o5\n"
            + "J2tRJ5xCGHfOQaz/VCp8QV3qnpjUp/sBMNRL6O64flmbamwN5/8y1D1xP900ZSWS\n"
            + "LjrfvQAaSh52O8orcaFXSoPNRYyOsLMZ4/L7ysJP6RPLGI/MwQE/XF5p/JNcFM0X\n"
            + "n1DR6UJGWl7KfJy7LT2EEM3ztiH87OvSsnrYeoBTJUE5MSmhxeiWHoPus8OsxA8v\n"
            + "DNHKAMMiiaxL2Wmt+et4zpZJM7wRyRNVGqHKgCYudpCB2Del8RKm4zjF1i60EVc3\n"
            + "Nm9ngw3veZRhiNUrIwNqJ2dx/ZUzPQ13wUAJ9H+GKSl5SrL4JXxs1yQYClbL5TBU\n"
            + "luPUhzlgSHVMzl9UCevI6j6AbGCi1DkppUelR5LN7lTgiBcCMc8XoFGzhriepobX\n"
            + "tZeUM+HJtjLGcq1yGLApM775JIl8LgrpkpuACMPs6dFSqwp5612hFtbaOqFmQn2P\n"
            + "SC6Kk4LcV7UahCehXtLr/S5QMoJvE7HfW92/+7Ln4tc1KCBE0+7KDq5PtjQgFybo\n"
            + "UoGUvXtva0m0Ff6gt1fdyoK2/y+V91fwMc/sCrlfNIA8bz9Lk98mrppOs4vfbKlq\n"
            + "9BFoZwuAebO/nuXOqn4U4gKxDabDkcxuMqgxquqtpePEH583FDNKxBvVVuZpDAbb\n"
            + "KTEOxXEDZvvMJeD0P99C1L6XPhpj0olCLgiC51P8/2aoLQC9YLm5I0ne+j/La7qN\n"
            + "i89+0FvnaoS75VtIlIj+kbrYOzWrnGIWHsB4k1kiSfwjdOBgbb4fc3tq9xpO9Vya\n"
            + "VWecagNlNvIz6Oqi8HXX9HT73kZ0GL95DB+FZsKKy03Bvys3tPEnl/R5mJlZslfr\n"
            + "0rsknLRM39Kty2W577IhRY/OypLIUppfa/R1x+yE/zN3cAAiMFyQi9BQpcEF/66x\n"
            + "zjGoYanv6unKojMQJ1KOgxWRgdhZfRcUomZJdgoU6/+ZzvftHQ2/KmaGOHiVdk6E\n"
            + "ARnYUTwTBHoIpS8d7SfZYAjj0I51ICHWpNU2ecZ7bCJEFjRdwbOHqQOV25rpAjM9\n"
            + "JdUwfi/yuw+LwjmRFFe4wqn/Je15/DM+3fNIYYwSZ2tYMjeDJFvFDcqX7m/j0DLf\n"
            + "G1Q65pz3hiOedkxKobHvmEygbqaAXX7gxUXKQf7NuooBfIgGiiMv0RMYZxqxYAwy\n"
            + "MVG2C1+SjlhdQgkUgFfXikku4A+3b2I+UEaJ/Jot03WCzIpJ7KIFJo5Q/56dhK7e\n"
            + "lRWBjhGupivqlvgWdUYGwsfd0OVpSaUChOKnO2mGPZmnyoig2F2VaE9yX8KopWkk\n"
            + "k+4D/wGJWWkW0NJN/7bGVkq5nXORzCMvN3r/UcovhEbDKAiMfIZz4eGw76xpqCmR\n"//
            + "puulRd0958X0/eOUE8jLSHCJsdGmwfOoJ0U=\n" //
            + "-----END ENCRYPTED PRIVATE KEY-----\n";

    private static NamedXContentRegistry xContentRegistry;
    private static ScriptService scriptService;

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled()
            .nodeSettings("signals.enabled", true, "signals.enterprise.enabled", false).resources("sg_config/signals").build();

    @BeforeClass
    public static void setupTestData() {

        try (Client client = cluster.getAdminCertClient()) {
            client.index(new IndexRequest("testsource").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "a", "x", "b", "y"))
                    .actionGet();
            client.index(new IndexRequest("testsource").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "a", "xx", "b", "yy"))
                    .actionGet();
        }
    }

    @BeforeClass
    public static void setupDependencies() {
        xContentRegistry = cluster.getInjectable(NamedXContentRegistry.class);
        scriptService = cluster.getInjectable(ScriptService.class);
    }

    @Test
    public void testWebhookAction() throws Exception {

        try (Client client = cluster.getAdminCertClient(); MockWebserviceProvider webhookProvider = new MockWebserviceProvider("/hook")) {

            NestedValueMap runtimeData = new NestedValueMap();
            runtimeData.put("path", "hook");
            runtimeData.put("body", "stuff");

            WatchExecutionContext ctx = new WatchExecutionContext(client, scriptService, xContentRegistry, null, ExecutionEnvironment.SCHEDULED,
                    ActionInvocationType.ALERT, new WatchExecutionContextData(runtimeData));

            HttpRequestConfig httpRequestConfig = new HttpRequestConfig(HttpRequestConfig.Method.POST, new URI(webhookProvider.getUri()),
                    "/{{data.path}}", null, "{{data.body}}", null, null, null);
            HttpClientConfig httpClientConfig = new HttpClientConfig(null, null, null);
            WebhookAction webhookAction = new WebhookAction(httpRequestConfig, httpClientConfig);

            httpRequestConfig.compileScripts(new WatchInitializationService(null, scriptService));

            webhookAction.execute(ctx);

            Assert.assertEquals(runtimeData.get("body"), webhookProvider.getLastRequestBody());
        }
    }

    @Test
    public void testWebhookActionWithTlsCustomTrustStore() throws Exception {

        try (Client client = cluster.getAdminCertClient(); MockWebserviceProvider webhookProvider = new MockWebserviceProvider("/hook", true, false)) {

            NestedValueMap runtimeData = new NestedValueMap();
            runtimeData.put("path", "hook");
            runtimeData.put("body", "stuff");

            WatchExecutionContext ctx = new WatchExecutionContext(client, scriptService, xContentRegistry, null, ExecutionEnvironment.SCHEDULED,
                    ActionInvocationType.ALERT, new WatchExecutionContextData(runtimeData));

            HttpRequestConfig httpRequestConfig = new HttpRequestConfig(HttpRequestConfig.Method.POST, new URI(webhookProvider.getUri()),
                    "/{{data.path}}", null, "{{data.body}}", null, null, null);

            httpRequestConfig.compileScripts(new WatchInitializationService(null, scriptService));

            TlsConfig tlsConfig = new TlsConfig();
            tlsConfig.setInlineTruststorePem(ROOT_CA_CERT);
            tlsConfig.init();

            HttpClientConfig httpClientConfig = new HttpClientConfig(null, null, tlsConfig);
            WebhookAction webhookAction = new WebhookAction(httpRequestConfig, httpClientConfig);

            //           webhookAction = parseBackAndForth(new WatchInitializationService(null, scriptService), webhookAction, new WebhookAction.Factory());

            webhookAction.execute(ctx);

            Assert.assertEquals(runtimeData.get("body"), webhookProvider.getLastRequestBody());
        }
    }

    @Test
    public void testWebhookActionWithTlsCustomTrustStoreFailure() throws Exception {

        try (Client client = cluster.getAdminCertClient(); MockWebserviceProvider webhookProvider = new MockWebserviceProvider("/hook", true, false)) {

            NestedValueMap runtimeData = new NestedValueMap();
            runtimeData.put("path", "hook");
            runtimeData.put("body", "stuff");

            WatchExecutionContext ctx = new WatchExecutionContext(client, scriptService, xContentRegistry, null, ExecutionEnvironment.SCHEDULED,
                    ActionInvocationType.ALERT, new WatchExecutionContextData(runtimeData));

            HttpRequestConfig httpRequestConfig = new HttpRequestConfig(HttpRequestConfig.Method.POST, new URI(webhookProvider.getUri()),
                    "/{{data.path}}", null, "{{data.body}}", null, null, null);

            HttpClientConfig httpClientConfig = new HttpClientConfig(null, null, null);
            WebhookAction webhookAction = new WebhookAction(httpRequestConfig, httpClientConfig);

            httpRequestConfig.compileScripts(new WatchInitializationService(null, scriptService));

            webhookAction.execute(ctx);

            Assert.fail();
        } catch (ActionExecutionException e) {
            e.printStackTrace();
            Assert.assertTrue(e.getCause().getMessage().contains("The server certificate could not be validated using the current truststore"));
        }
    }

    @Test
    public void testWebhookActionWithTlsClientAuth() throws Exception {

        try (Client client = cluster.getAdminCertClient(); MockWebserviceProvider webhookProvider = new MockWebserviceProvider("/hook", true, true)) {

            NestedValueMap runtimeData = new NestedValueMap();
            runtimeData.put("path", "hook");
            runtimeData.put("body", "stuff");

            WatchExecutionContext ctx = new WatchExecutionContext(client, scriptService, xContentRegistry, null, ExecutionEnvironment.SCHEDULED,
                    ActionInvocationType.ALERT, new WatchExecutionContextData(runtimeData));

            HttpRequestConfig httpRequestConfig = new HttpRequestConfig(HttpRequestConfig.Method.POST, new URI(webhookProvider.getUri()),
                    "/{{data.path}}", null, "{{data.body}}", null, null, null);

            httpRequestConfig.compileScripts(new WatchInitializationService(null, scriptService));

            TlsClientAuthConfig tlsClientAuthConfig = new TlsClientAuthConfig();
            tlsClientAuthConfig.setInlineAuthCertsPem(KIRK_CERT);
            tlsClientAuthConfig.setInlineAuthKey(KIRK_KEY);
            tlsClientAuthConfig.setInlineAuthKeyPassword("secret");
            tlsClientAuthConfig.init();

            TlsConfig tlsConfig = new TlsConfig();
            tlsConfig.setInlineTruststorePem(ROOT_CA_CERT);
            tlsConfig.setClientAuthConfig(tlsClientAuthConfig);
            tlsConfig.init();

            HttpClientConfig httpClientConfig = new HttpClientConfig(null, null, tlsConfig);
            WebhookAction webhookAction = new WebhookAction(httpRequestConfig, httpClientConfig);

            //           webhookAction = parseBackAndForth(new WatchInitializationService(null, scriptService), webhookAction, new WebhookAction.Factory());

            webhookAction.execute(ctx);

            Assert.assertEquals(runtimeData.get("body"), webhookProvider.getLastRequestBody());
        }
    }

    @Test
    public void testWebhookActionWithTlsClientAuthFailure() throws Exception {

        try (Client client = cluster.getAdminCertClient(); MockWebserviceProvider webhookProvider = new MockWebserviceProvider("/hook", true, true)) {

            NestedValueMap runtimeData = new NestedValueMap();
            runtimeData.put("path", "hook");
            runtimeData.put("body", "stuff");

            WatchExecutionContext ctx = new WatchExecutionContext(client, scriptService, xContentRegistry, null, ExecutionEnvironment.SCHEDULED,
                    ActionInvocationType.ALERT, new WatchExecutionContextData(runtimeData));

            HttpRequestConfig httpRequestConfig = new HttpRequestConfig(HttpRequestConfig.Method.POST, new URI(webhookProvider.getUri()),
                    "/{{data.path}}", null, "{{data.body}}", null, null, null);

            TlsConfig tlsConfig = new TlsConfig();
            tlsConfig.setInlineTruststorePem(ROOT_CA_CERT);
            tlsConfig.init();

            HttpClientConfig httpClientConfig = new HttpClientConfig(null, null, tlsConfig);
            WebhookAction webhookAction = new WebhookAction(httpRequestConfig, httpClientConfig);

            httpRequestConfig.compileScripts(new WatchInitializationService(null, scriptService));

            webhookAction.execute(ctx);

            Assert.fail();
        } catch (ActionExecutionException e) {
            // TODO
            // Assert.assertTrue(e.getCause().getMessage(), e.getCause().getMessage()
            //       .contains("Certificate validation failed. Check if the host requires client certificate authentication"));
        }
    }

    @Test
    public void testWebhookActionTimeout() throws Exception {

        try (Client client = cluster.getAdminCertClient(); MockWebserviceProvider webhookProvider = new MockWebserviceProvider("/hook")) {
            webhookProvider.setResponseDelayMs(3330);

            NestedValueMap runtimeData = new NestedValueMap();
            runtimeData.put("path", "hook");
            runtimeData.put("body", "stuff");

            WatchExecutionContext ctx = new WatchExecutionContext(client, scriptService, xContentRegistry, null, ExecutionEnvironment.SCHEDULED,
                    ActionInvocationType.ALERT, new WatchExecutionContextData(runtimeData));

            HttpRequestConfig httpRequestConfig = new HttpRequestConfig(HttpRequestConfig.Method.POST, new URI(webhookProvider.getUri()),
                    "/{{data.path}}", null, "{{data.body}}", null, null, null);
            HttpClientConfig httpClientConfig = new HttpClientConfig(1, 1, null);
            WebhookAction webhookAction = new WebhookAction(httpRequestConfig, httpClientConfig);

            httpRequestConfig.compileScripts(new WatchInitializationService(null, scriptService));

            webhookAction.execute(ctx);

            Assert.fail();
        } catch (ActionExecutionException e) {
            Assert.assertTrue(e.toString(), e.getCause().toString().contains("Read timed out"));
        }
    }

    @Test
    public void testIndexAction() throws Exception {

        try (Client client = cluster.getAdminCertClient()) {

            NestedValueMap runtimeData = new NestedValueMap();
            runtimeData.put("_id", "my_doc");
            runtimeData.put(new NestedValueMap.Path("o1", "oa"), 10);
            runtimeData.put(new NestedValueMap.Path("o1", "ob"), 20);
            runtimeData.put(new NestedValueMap.Path("o2"), "test");

            WatchExecutionContext ctx = new WatchExecutionContext(client, scriptService, xContentRegistry, null, ExecutionEnvironment.SCHEDULED,
                    ActionInvocationType.ALERT, new WatchExecutionContextData(runtimeData));

            IndexAction indexAction = new IndexAction("index_action_sink", RefreshPolicy.IMMEDIATE);

            indexAction.execute(ctx);

            GetResponse getResponse = client.get(new GetRequest("index_action_sink", "my_doc")).actionGet();

            Assert.assertEquals("test", getResponse.getSource().get("o2"));
        }
    }

    @Test
    public void testIndexActionWithIdTemplate() throws Exception {

        try (Client client = cluster.getAdminCertClient()) {

            NestedValueMap runtimeData = new NestedValueMap();
            runtimeData.put("id_from_data", "my_doc_2");
            runtimeData.put(new NestedValueMap.Path("o1", "oa"), 10);
            runtimeData.put(new NestedValueMap.Path("o1", "ob"), 20);
            runtimeData.put(new NestedValueMap.Path("o2"), "test_2");

            WatchExecutionContext ctx = new WatchExecutionContext(client, scriptService, xContentRegistry, null, ExecutionEnvironment.SCHEDULED,
                    ActionInvocationType.ALERT, new WatchExecutionContextData(runtimeData));

            IndexAction indexAction = new IndexAction("index_action_sink", RefreshPolicy.IMMEDIATE);

            indexAction.setDocId(InlineMustacheTemplate.parse(scriptService, "{{data.id_from_data}}"));

            indexAction.execute(ctx);

            GetResponse getResponse = client.get(new GetRequest("index_action_sink", "my_doc_2")).actionGet();

            Assert.assertEquals("test_2", getResponse.getSource().get("o2"));
        }
    }

    @Test
    public void testMultiDocIndexAction() throws Exception {

        try (Client client = cluster.getAdminCertClient()) {

            List<NestedValueMap> docs = new ArrayList<>();

            NestedValueMap doc1 = new NestedValueMap();
            doc1.put("_id", "my_doc_1");
            doc1.put("_index", "multidoc_index_action_sink");
            doc1.put(new NestedValueMap.Path("a"), "test_1");
            docs.add(doc1);

            NestedValueMap doc2 = new NestedValueMap();
            doc2.put("_id", "my_doc_2");
            doc2.put("_index", "multidoc_index_action_sink");
            doc2.put(new NestedValueMap.Path("a"), "test_2");
            docs.add(doc2);

            NestedValueMap runtimeData = new NestedValueMap();
            runtimeData.put("_doc", docs);

            WatchExecutionContext ctx = new WatchExecutionContext(client, scriptService, xContentRegistry, null, ExecutionEnvironment.SCHEDULED,
                    ActionInvocationType.ALERT, new WatchExecutionContextData(runtimeData));

            IndexAction indexAction = new IndexAction("index_action_sink", RefreshPolicy.IMMEDIATE);

            indexAction.execute(ctx);

            GetResponse getResponse = client.get(new GetRequest("multidoc_index_action_sink", "my_doc_1")).actionGet();
            Assert.assertTrue(getResponse.isExists());
            Assert.assertEquals("test_1", getResponse.getSource().get("a"));

            getResponse = client.get(new GetRequest("multidoc_index_action_sink", "my_doc_2")).actionGet();
            Assert.assertTrue(getResponse.isExists());
            Assert.assertEquals("test_2", getResponse.getSource().get("a"));
        }
    }

    @Test
    public void testMultiDocIndexActionWithArray() throws Exception {

        try (Client client = cluster.getAdminCertClient()) {

            NestedValueMap[] docs = new NestedValueMap[2];

            NestedValueMap doc1 = new NestedValueMap();
            doc1.put("_id", "my_doc_1");
            doc1.put("_index", "multidoc_index_action_sink_2");
            doc1.put(new NestedValueMap.Path("a"), "test_1");
            docs[0] = doc1;

            NestedValueMap doc2 = new NestedValueMap();
            doc2.put("_id", "my_doc_2");
            doc2.put("_index", "multidoc_index_action_sink_2");
            doc2.put(new NestedValueMap.Path("a"), "test_2");
            docs[1] = doc2;

            NestedValueMap runtimeData = new NestedValueMap();
            runtimeData.put("_doc", docs);

            WatchExecutionContext ctx = new WatchExecutionContext(client, scriptService, xContentRegistry, null, ExecutionEnvironment.SCHEDULED,
                    ActionInvocationType.ALERT, new WatchExecutionContextData(runtimeData));

            IndexAction indexAction = new IndexAction("index_action_sink", RefreshPolicy.IMMEDIATE);

            indexAction.execute(ctx);

            GetResponse getResponse = client.get(new GetRequest("multidoc_index_action_sink_2", "my_doc_1")).actionGet();
            Assert.assertTrue(getResponse.isExists());
            Assert.assertEquals("test_1", getResponse.getSource().get("a"));

            getResponse = client.get(new GetRequest("multidoc_index_action_sink_2", "my_doc_2")).actionGet();
            Assert.assertTrue(getResponse.isExists());
            Assert.assertEquals("test_2", getResponse.getSource().get("a"));
        }
    }

    @Test
    public void testSlackAction() throws Exception {

        try (Client client = cluster.getAdminCertClient(); MockWebserviceProvider webhookProvider = new MockWebserviceProvider("/slack")) {

            SlackAccount slackDestination = new SlackAccount();
            slackDestination.setUrl(new URI(webhookProvider.getUri()));

            AccountRegistry accountRegistry = Mockito.mock(AccountRegistry.class);
            Mockito.when(accountRegistry.lookupAccount("test_destination", SlackAccount.class)).thenReturn(slackDestination);

            NestedValueMap runtimeData = new NestedValueMap();
            runtimeData.put("path", "hook");
            runtimeData.put("body", "stuff");

            WatchExecutionContext ctx = new WatchExecutionContext(client, scriptService, xContentRegistry, accountRegistry,
                    ExecutionEnvironment.SCHEDULED, ActionInvocationType.ALERT, new WatchExecutionContextData(runtimeData));

            SlackActionConf c = new SlackActionConf();
            c.setAccount("test_destination");
            c.setChannel("test_channel");
            c.setFrom("test_from");
            c.setText("{{data.body}}");

            SlackAction slackAction = new SlackAction(c);
            slackAction.compileScripts(new WatchInitializationService(accountRegistry, scriptService));

            slackAction.execute(ctx);

            Assert.assertEquals("{\"channel\":\"test_channel\",\"username\":\"test_from\",\"text\":\"stuff\"}", webhookProvider.getLastRequestBody());
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testSlackActionWithBlocks() throws Exception {

        try (Client client = cluster.getAdminCertClient(); MockWebserviceProvider webhookProvider = new MockWebserviceProvider("/slack")) {

            SlackAccount slackDestination = new SlackAccount();
            slackDestination.setUrl(new URI(webhookProvider.getUri()));

            AccountRegistry accountRegistry = Mockito.mock(AccountRegistry.class);
            Mockito.when(accountRegistry.lookupAccount("test_destination", SlackAccount.class)).thenReturn(slackDestination);

            NestedValueMap runtimeData = new NestedValueMap();
            runtimeData.put("path", "hook");
            runtimeData.put("body", "stuff");

            WatchExecutionContext ctx = new WatchExecutionContext(client, scriptService, xContentRegistry, accountRegistry,
                    ExecutionEnvironment.SCHEDULED, ActionInvocationType.ALERT, new WatchExecutionContextData(runtimeData));

            String blocksRawJson = "[\n" +
                    "\t\t{\n" +
                    "\t\t\t\"type\": \"section\",\n" +
                    "\t\t\t\"text\": {\n" +
                    "\t\t\t\t\"type\": \"mrkdwn\",\n" +
                    "\t\t\t\t\"text\": \"A message *with some bold text* and {{data.body}}.\"\n" +
                    "\t\t\t}\n" +
                    "\t\t}\n" +
                    "\t]";

            List blocks = DefaultObjectMapper.readValue(blocksRawJson, List.class);

            SlackActionConf c = new SlackActionConf();
            c.setAccount("test_destination");
            c.setChannel("test_channel");
            c.setFrom("test_from");
            c.setBlocks(blocks);

            SlackAction slackAction = new SlackAction(c);
            slackAction.compileScripts(new WatchInitializationService(accountRegistry, scriptService));

            slackAction.execute(ctx);

            String expected = "{\"channel\":\"test_channel\",\"username\":\"test_from\",\"blocks\":\"[{\\\"type\\\":\\\"section\\\",\\\"text\\\":{\\\"type\\\":\\\"mrkdwn\\\",\\\"text\\\":\\\"A message *with some bold text* and stuff.\\\"}}]\"}";

            Assert.assertEquals(expected, webhookProvider.getLastRequestBody());
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testSlackActionWithBlocksAndQuotesInMustacheTemplate() throws Exception {

        try (Client client = cluster.getAdminCertClient(); MockWebserviceProvider webhookProvider = new MockWebserviceProvider("/slack")) {

            SlackAccount slackDestination = new SlackAccount();
            slackDestination.setUrl(new URI(webhookProvider.getUri()));

            AccountRegistry accountRegistry = Mockito.mock(AccountRegistry.class);
            Mockito.when(accountRegistry.lookupAccount("test_destination", SlackAccount.class)).thenReturn(slackDestination);

            NestedValueMap runtimeData = new NestedValueMap();
            runtimeData.put("path", "hook");
            runtimeData.put("body", "stuff");
            runtimeData.put("someQuote", "\"a quote\"");

            WatchExecutionContext ctx = new WatchExecutionContext(client, scriptService, xContentRegistry, accountRegistry,
                    ExecutionEnvironment.SCHEDULED, ActionInvocationType.ALERT, new WatchExecutionContextData(runtimeData));

            String blocksRawJson = "[\n" +
                    "\t\t{\n" +
                    "\t\t\t\"type\": \"section\",\n" +
                    "\t\t\t\"text\": {\n" +
                    "\t\t\t\t\"type\": \"mrkdwn\",\n" +
                    "\t\t\t\t\"text\": \"A message *with some bold text* and {{data.body}} and {{data.someQuote}}.\"\n" +
                    "\t\t\t}\n" +
                    "\t\t}\n" +
                    "\t]";

            List blocks = DefaultObjectMapper.readValue(blocksRawJson, List.class);

            SlackActionConf c = new SlackActionConf();
            c.setAccount("test_destination");
            c.setChannel("test_channel");
            c.setFrom("test_from");
            c.setBlocks(blocks);

            SlackAction slackAction = new SlackAction(c);
            slackAction.compileScripts(new WatchInitializationService(accountRegistry, scriptService));

            slackAction.execute(ctx);

            String expected = "{\"channel\":\"test_channel\",\"username\":\"test_from\",\"blocks\":\"[{\\\"type\\\":\\\"section\\\",\\\"text\\\":{\\\"type\\\":\\\"mrkdwn\\\",\\\"text\\\":\\\"A message *with some bold text* and stuff and \\\\\\\"a quote\\\\\\\".\\\"}}]\"}";

            Assert.assertEquals(expected, webhookProvider.getLastRequestBody());
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testSlackActionWithBlocksAndText() throws Exception {

        try (Client client = cluster.getAdminCertClient(); MockWebserviceProvider webhookProvider = new MockWebserviceProvider("/slack")) {

            SlackAccount slackDestination = new SlackAccount();
            slackDestination.setUrl(new URI(webhookProvider.getUri()));

            AccountRegistry accountRegistry = Mockito.mock(AccountRegistry.class);
            Mockito.when(accountRegistry.lookupAccount("test_destination", SlackAccount.class)).thenReturn(slackDestination);

            NestedValueMap runtimeData = new NestedValueMap();
            runtimeData.put("path", "hook");
            runtimeData.put("body", "stuff");

            WatchExecutionContext ctx = new WatchExecutionContext(client, scriptService, xContentRegistry, accountRegistry,
                    ExecutionEnvironment.SCHEDULED, ActionInvocationType.ALERT, new WatchExecutionContextData(runtimeData));

            String blocksRawJson = "[\n" +
                    "\t\t{\n" +
                    "\t\t\t\"type\": \"section\",\n" +
                    "\t\t\t\"text\": {\n" +
                    "\t\t\t\t\"type\": \"mrkdwn\",\n" +
                    "\t\t\t\t\"text\": \"A message *with some bold text* and {{data.body}}.\"\n" +
                    "\t\t\t}\n" +
                    "\t\t}\n" +
                    "\t]";

            List blocks = DefaultObjectMapper.readValue(blocksRawJson, List.class);

            SlackActionConf c = new SlackActionConf();
            c.setAccount("test_destination");
            c.setChannel("test_channel");
            c.setFrom("test_from");
            c.setText("{{data.body}}");
            c.setBlocks(blocks);

            SlackAction slackAction = new SlackAction(c);
            slackAction.compileScripts(new WatchInitializationService(accountRegistry, scriptService));

            slackAction.execute(ctx);

            String expected = "{\"channel\":\"test_channel\",\"username\":\"test_from\",\"text\":\"stuff\",\"blocks\":\"[{\\\"type\\\":\\\"section\\\",\\\"text\\\":{\\\"type\\\":\\\"mrkdwn\\\",\\\"text\\\":\\\"A message *with some bold text* and stuff.\\\"}}]\"}";
            Assert.assertEquals(expected, webhookProvider.getLastRequestBody());
        }
    }

    @Test
    public void testSlackActionWithoutBlockAndText() {
            AccountRegistry accountRegistry = Mockito.mock(AccountRegistry.class);

            SlackActionConf c = new SlackActionConf();
            c.setAccount("test_destination");
            c.setChannel("test_channel");
            c.setFrom("test_from");

            SlackAction slackAction = new SlackAction(c);

            try {
                slackAction.compileScripts(new WatchInitializationService(accountRegistry, scriptService));
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage().contains("'text': Required attribute is missing"));
            }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testSlackActionWithAttachments() throws Exception {

        try (Client client = cluster.getAdminCertClient(); MockWebserviceProvider webhookProvider = new MockWebserviceProvider("/slack")) {

            SlackAccount slackDestination = new SlackAccount();
            slackDestination.setUrl(new URI(webhookProvider.getUri()));

            AccountRegistry accountRegistry = Mockito.mock(AccountRegistry.class);
            Mockito.when(accountRegistry.lookupAccount("test_destination", SlackAccount.class)).thenReturn(slackDestination);

            NestedValueMap runtimeData = new NestedValueMap();
            runtimeData.put("path", "hook");
            runtimeData.put("body", "stuff");

            WatchExecutionContext ctx = new WatchExecutionContext(client, scriptService, xContentRegistry, accountRegistry,
                    ExecutionEnvironment.SCHEDULED, ActionInvocationType.ALERT, new WatchExecutionContextData(runtimeData));

            String attachmentRawJson = "[\n" +
                    "      {\n" +
                    "          \"fallback\": \"Plain-text summary of the attachment.\",\n" +
                    "          \"color\": \"#2eb886\",\n" +
                    "          \"pretext\": \"Optional text that appears above the attachment block\",\n" +
                    "          \"author_name\": \"Bobby Tables\",\n" +
                    "          \"author_link\": \"http://flickr.com/bobby/\",\n" +
                    "          \"author_icon\": \"http://flickr.com/icons/bobby.jpg\",\n" +
                    "          \"title\": \"Slack API Documentation\",\n" +
                    "          \"title_link\": \"https://api.slack.com/\",\n" +
                    "          \"text\": \"Optional text that appears within the attachment\",\n" +
                    "          \"fields\": [\n" +
                    "              {\n" +
                    "                  \"title\": \"Priority\",\n" +
                    "                  \"value\": \"High\",\n" +
                    "                  \"short\": false\n" +
                    "              }\n" +
                    "          ],\n" +
                    "          \"image_url\": \"http://my-website.com/path/to/image.jpg\",\n" +
                    "          \"thumb_url\": \"http://example.com/path/to/thumb.png\",\n" +
                    "          \"footer\": \"Slack API\",\n" +
                    "          \"footer_icon\": \"https://platform.slack-edge.com/img/default_application_icon.png\",\n" +
                    "          \"ts\": 123456789\n" +
                    "      }\n" +
                    "  ]";

            List attachments = DefaultObjectMapper.readValue(attachmentRawJson, List.class);

            SlackActionConf c = new SlackActionConf();
            c.setAccount("test_destination");
            c.setChannel("test_channel");
            c.setFrom("test_from");
            c.setAttachments(attachments);

            SlackAction slackAction = new SlackAction(c);
            slackAction.compileScripts(new WatchInitializationService(accountRegistry, scriptService));

            slackAction.execute(ctx);

            String expected = "{\"channel\":\"test_channel\",\"username\":\"test_from\",\"attachments\":\"[{\\\"fallback\\\":\\\"Plain-text summary of the attachment.\\\",\\\"color\\\":\\\"#2eb886\\\",\\\"pretext\\\":\\\"Optional text that appears above the attachment block\\\",\\\"author_name\\\":\\\"Bobby Tables\\\",\\\"author_link\\\":\\\"http://flickr.com/bobby/\\\",\\\"author_icon\\\":\\\"http://flickr.com/icons/bobby.jpg\\\",\\\"title\\\":\\\"Slack API Documentation\\\",\\\"title_link\\\":\\\"https://api.slack.com/\\\",\\\"text\\\":\\\"Optional text that appears within the attachment\\\",\\\"fields\\\":[{\\\"title\\\":\\\"Priority\\\",\\\"value\\\":\\\"High\\\",\\\"short\\\":false}],\\\"image_url\\\":\\\"http://my-website.com/path/to/image.jpg\\\",\\\"thumb_url\\\":\\\"http://example.com/path/to/thumb.png\\\",\\\"footer\\\":\\\"Slack API\\\",\\\"footer_icon\\\":\\\"https://platform.slack-edge.com/img/default_application_icon.png\\\",\\\"ts\\\":123456789}]\"}";

            Assert.assertEquals(expected, webhookProvider.getLastRequestBody());
        }
    }

    @Test
    public void testEmailAction() throws Exception {

        final int smtpPort = SocketUtils.findAvailableTcpPort();

        GreenMail greenMail = new GreenMail(new ServerSetup(smtpPort, "127.0.0.1", ServerSetup.PROTOCOL_SMTP));
        greenMail.start();

        try (Client client = cluster.getAdminCertClient()) {

            EmailAccount emailAccount = new EmailAccount();
            emailAccount.setHost("localhost");
            emailAccount.setPort(smtpPort);
            emailAccount.setDefaultFrom("from@default.sgtest");
            emailAccount.setDefaultBcc("bcc1@default.sgtest", "bcc2@default.sgtest");

            AccountRegistry accountRegistry = Mockito.mock(AccountRegistry.class);
            Mockito.when(accountRegistry.lookupAccount("test_destination", EmailAccount.class)).thenReturn(emailAccount);

            EmailAction emailAction = new EmailAction();
            emailAction.setBody("We searched {{data.x}} shards");
            emailAction.setSubject("Test Subject");
            emailAction.setTo(Collections.singletonList("to@specific.sgtest"));
            emailAction.setAccount("test_destination");

            Attachment attachment1 = new EmailAction.Attachment();
            attachment1.setType(Attachment.AttachmentType.RUNTIME);

            Attachment attachment2 = new EmailAction.Attachment();
            attachment2.setType(Attachment.AttachmentType.RUNTIME);

            emailAction.setAttachments(ImmutableMap.of("test2", attachment2, "test1", attachment1));

            emailAction.compileScripts(new WatchInitializationService(accountRegistry, scriptService));

            NestedValueMap runtimeData = new NestedValueMap();
            runtimeData.put("x", "y");

            WatchExecutionContext ctx = new WatchExecutionContext(client, scriptService, xContentRegistry, accountRegistry,
                    ExecutionEnvironment.SCHEDULED, ActionInvocationType.ALERT, new WatchExecutionContextData(runtimeData));

            ActionExecutionResult result = emailAction.execute(ctx);
            Assert.assertTrue(result.getRequest(), result.getRequest().contains("Content-Type: text/plain"));
            Assert.assertFalse(result.getRequest(), result.getRequest().contains("Content-Type: multipart/alternative"));

            if (!greenMail.waitForIncomingEmail(20000, 1)) {
                Assert.fail("Timeout waiting for mails");
            }

            String receivedMail = GreenMailUtil.getWholeMessage(greenMail.getReceivedMessages()[0]);

            Assert.assertTrue(receivedMail, receivedMail.contains("We searched y shards"));
            Assert.assertTrue(receivedMail, receivedMail.contains("Subject: Test Subject"));
            Assert.assertTrue(receivedMail, receivedMail.contains("From: from@default.sgtest"));
            Assert.assertTrue(receivedMail, receivedMail.contains("To: to@specific.sgtest"));
            Assert.assertTrue(receivedMail.indexOf("Content-ID: <test2>") < receivedMail.indexOf("Content-ID: <test1>"));

        } finally {
            greenMail.stop();
        }

    }
    
    @Test
    public void testEmailActionTls() throws Exception {

        final int smtpPort = SocketUtils.findAvailableTcpPort();

        GreenMail greenMail = new GreenMail(new ServerSetup(smtpPort, "127.0.0.1", ServerSetup.PROTOCOL_SMTPS));
        greenMail.start();

        try (Client client = cluster.getAdminCertClient()) {

            EmailAccount emailAccount = new EmailAccount();
            emailAccount.setHost("localhost");
            emailAccount.setPort(smtpPort);
            emailAccount.setEnableTls(true);
            emailAccount.setTrustAll(true);
            emailAccount.setDefaultFrom("from@default.sgtest");
            emailAccount.setDefaultBcc("bcc1@default.sgtest", "bcc2@default.sgtest");

            AccountRegistry accountRegistry = Mockito.mock(AccountRegistry.class);
            Mockito.when(accountRegistry.lookupAccount("test_destination", EmailAccount.class)).thenReturn(emailAccount);

            EmailAction emailAction = new EmailAction();
            emailAction.setBody("We searched {{data.x}} shards");
            emailAction.setSubject("Test Subject");
            emailAction.setTo(Collections.singletonList("to@specific.sgtest"));
            emailAction.setAccount("test_destination");

            Attachment attachment1 = new EmailAction.Attachment();
            attachment1.setType(Attachment.AttachmentType.RUNTIME);

            Attachment attachment2 = new EmailAction.Attachment();
            attachment2.setType(Attachment.AttachmentType.RUNTIME);

            emailAction.setAttachments(ImmutableMap.of("test2", attachment2, "test1", attachment1));

            emailAction.compileScripts(new WatchInitializationService(accountRegistry, scriptService));

            NestedValueMap runtimeData = new NestedValueMap();
            runtimeData.put("x", "y");

            WatchExecutionContext ctx = new WatchExecutionContext(client, scriptService, xContentRegistry, accountRegistry,
                    ExecutionEnvironment.SCHEDULED, ActionInvocationType.ALERT, new WatchExecutionContextData(runtimeData));

            ActionExecutionResult result = emailAction.execute(ctx);
            Assert.assertTrue(result.getRequest(), result.getRequest().contains("Content-Type: text/plain"));
            Assert.assertFalse(result.getRequest(), result.getRequest().contains("Content-Type: multipart/alternative"));

            if (!greenMail.waitForIncomingEmail(20000, 1)) {
                Assert.fail("Timeout waiting for mails");
            }

            String receivedMail = GreenMailUtil.getWholeMessage(greenMail.getReceivedMessages()[0]);

            Assert.assertTrue(receivedMail, receivedMail.contains("We searched y shards"));
            Assert.assertTrue(receivedMail, receivedMail.contains("Subject: Test Subject"));
            Assert.assertTrue(receivedMail, receivedMail.contains("From: from@default.sgtest"));
            Assert.assertTrue(receivedMail, receivedMail.contains("To: to@specific.sgtest"));
            Assert.assertTrue(receivedMail.indexOf("Content-ID: <test2>") < receivedMail.indexOf("Content-ID: <test1>"));

        } finally {
            greenMail.stop();
        }

    }

    @Test
    public void testEmailActionWithHtmlBody() throws Exception {

        final int smtpPort = SocketUtils.findAvailableTcpPort();

        //SMTP server for unittesting
        GreenMail greenMail = new GreenMail(new ServerSetup(smtpPort, "127.0.0.1", ServerSetup.PROTOCOL_SMTP));
        greenMail.start();

        try (Client client = cluster.getAdminCertClient()) {

            EmailAccount emailDestination = new EmailAccount();
            emailDestination.setHost("localhost");
            emailDestination.setPort(smtpPort);
            emailDestination.setDefaultFrom("from@default.sgtest");
            emailDestination.setDefaultBcc("bcc1@default.sgtest", "bcc2@default.sgtest");

            AccountRegistry accountRegistry = Mockito.mock(AccountRegistry.class);
            Mockito.when(accountRegistry.lookupAccount("test_destination", EmailAccount.class)).thenReturn(emailDestination);

            EmailAction emailAction = new EmailAction();
            emailAction.setHtmlBody("<p>We searched {{data.x}} shards<p/>");
            emailAction.setSubject("Test Subject");
            emailAction.setTo(Collections.singletonList("to@specific.sgtest"));
            emailAction.setAccount("test_destination");

            Attachment attachment = new EmailAction.Attachment();
            attachment.setType(Attachment.AttachmentType.RUNTIME);

            emailAction.setAttachments(ImmutableMap.of("test", attachment));

            emailAction.compileScripts(new WatchInitializationService(accountRegistry, scriptService));

            NestedValueMap runtimeData = new NestedValueMap();
            runtimeData.put("x", "y");

            WatchExecutionContext ctx = new WatchExecutionContext(client, scriptService, xContentRegistry, accountRegistry,
                    ExecutionEnvironment.SCHEDULED, ActionInvocationType.ALERT, new WatchExecutionContextData(runtimeData));

            ActionExecutionResult result = emailAction.execute(ctx);

            Assert.assertTrue(result.getRequest().contains("<p>We searched y shards<p/>"));
            
            if (!greenMail.waitForIncomingEmail(20000, 1)) {
                Assert.fail("Timeout waiting for mails");
            }

            String receivedMail = GreenMailUtil.getWholeMessage(greenMail.getReceivedMessages()[0]);
            
            Assert.assertTrue(receivedMail, receivedMail.contains("<p>We searched y shards<p/>"));
            Assert.assertTrue(receivedMail, receivedMail.contains("Content-Type: text/html"));
            Assert.assertTrue(receivedMail, receivedMail.contains("Subject: Test Subject"));
            Assert.assertTrue(receivedMail, receivedMail.contains("From: from@default.sgtest"));
            Assert.assertTrue(receivedMail, receivedMail.contains("To: to@specific.sgtest"));

        } finally {
            greenMail.stop();
        }

    }

    @Test
    public void testEmailActionWithHtmlBodyAndTextBody() throws Exception {

        final int smtpPort = SocketUtils.findAvailableTcpPort();

        //SMTP server for unittesting
        GreenMail greenMail = new GreenMail(new ServerSetup(smtpPort, "127.0.0.1", ServerSetup.PROTOCOL_SMTP));
        greenMail.start();

        try (Client client = cluster.getAdminCertClient()) {

            EmailAccount emailDestination = new EmailAccount();
            emailDestination.setHost("localhost");
            emailDestination.setPort(smtpPort);
            emailDestination.setDefaultFrom("from@default.sgtest");
            emailDestination.setDefaultBcc("bcc1@default.sgtest", "bcc2@default.sgtest");

            AccountRegistry accountRegistry = Mockito.mock(AccountRegistry.class);
            Mockito.when(accountRegistry.lookupAccount("test_destination", EmailAccount.class)).thenReturn(emailDestination);

            EmailAction emailAction = new EmailAction();
            emailAction.setBody("{{data.x}} shards have been searched for");
            emailAction.setHtmlBody("<p>We searched {{data.x}} shards<p/>");
            emailAction.setSubject("Test Subject");
            emailAction.setTo(Collections.singletonList("to@specific.sgtest"));
            emailAction.setAccount("test_destination");

            Attachment attachment = new EmailAction.Attachment();
            attachment.setType(Attachment.AttachmentType.RUNTIME);

            emailAction.setAttachments(ImmutableMap.of("test", attachment));

            emailAction.compileScripts(new WatchInitializationService(accountRegistry, scriptService));

            NestedValueMap runtimeData = new NestedValueMap();
            runtimeData.put("x", "y");

            WatchExecutionContext ctx = new WatchExecutionContext(client, scriptService, xContentRegistry, accountRegistry,
                    ExecutionEnvironment.SCHEDULED, ActionInvocationType.ALERT, new WatchExecutionContextData(runtimeData));

            ActionExecutionResult result = emailAction.execute(ctx);

            Assert.assertTrue(result.getRequest().contains("y shards have been searched for"));
            Assert.assertTrue(result.getRequest().contains("<p>We searched y shards<p/>"));
            Assert.assertTrue(result.getRequest().contains("Content-Type: multipart/alternative"));
            Assert.assertTrue(result.getRequest().contains("Content-Type: text/plain"));
            Assert.assertTrue(result.getRequest().contains("Content-Type: text/html"));
            
            if (!greenMail.waitForIncomingEmail(20000, 1)) {
                Assert.fail("Timeout waiting for mails");
            }

            String receivedMail = GreenMailUtil.getWholeMessage(greenMail.getReceivedMessages()[0]);
            
            Assert.assertTrue(receivedMail, receivedMail.contains("We searched y shards"));
            Assert.assertTrue(receivedMail, receivedMail.contains("<p>We searched y shards<p/>"));
            Assert.assertTrue(receivedMail, receivedMail.contains("Content-Type: text/html"));
            Assert.assertTrue(receivedMail, receivedMail.contains("Subject: Test Subject"));
            Assert.assertTrue(receivedMail, receivedMail.contains("From: from@default.sgtest"));
            Assert.assertTrue(receivedMail, receivedMail.contains("To: to@specific.sgtest"));

        } finally {
            greenMail.stop();
        }

    }

    @Test
    public void testEmailActionWithMissingHtmlBodyAndMissingBody() throws Exception {
            EmailAccount emailDestination = new EmailAccount();
            emailDestination.setHost("localhost");
            emailDestination.setPort(1234);
            emailDestination.setDefaultFrom("from@default.sgtest");
            emailDestination.setDefaultBcc("bcc1@default.sgtest", "bcc2@default.sgtest");

            AccountRegistry accountRegistry = Mockito.mock(AccountRegistry.class);
            Mockito.when(accountRegistry.lookupAccount("test_destination", EmailAccount.class)).thenReturn(emailDestination);

            EmailAction emailAction = new EmailAction();
            emailAction.setSubject("Test Subject");
            emailAction.setTo(Collections.singletonList("to@specific.sgtest"));
            emailAction.setAccount("test_destination");

            Attachment attachment = new EmailAction.Attachment();
            attachment.setType(Attachment.AttachmentType.RUNTIME);

            emailAction.setAttachments(ImmutableMap.of("test", attachment));

            try {
                emailAction.compileScripts(new WatchInitializationService(accountRegistry, scriptService));
            } catch (ConfigValidationException e) {
                Assert.assertTrue(e.getMessage().contains("Both body and html_body are empty"));
            }
    }

    @Test
    public void testEmailActionWithBasicTextRequestAndRuntimeData() throws Exception {
        final int smtpPort = SocketUtils.findAvailableTcpPort();

        GreenMail greenMail = new GreenMail(new ServerSetup(smtpPort, "127.0.0.1", ServerSetup.PROTOCOL_SMTP));
        greenMail.start();

        try (Client client = cluster.getAdminCertClient()) {

            try (MockWebserviceProvider webhookProvider = new MockWebserviceProvider("/hook")) {

                NestedValueMap runtimeData = new NestedValueMap();
                runtimeData.put("path", "hook");
                runtimeData.put("body", "stuff");
                runtimeData.put("x", "y");

                HttpRequestConfig httpRequestConfig = new HttpRequestConfig(HttpRequestConfig.Method.POST, new URI(webhookProvider.getUri()),
                        "/{{data.path}}", null, "{{data.body}}", null, null, null);
                HttpClientConfig httpClientConfig = new HttpClientConfig(null, null, null);

                httpRequestConfig.compileScripts(new WatchInitializationService(null, scriptService));

                EmailAccount emailDestination = new EmailAccount();
                emailDestination.setHost("localhost");
                emailDestination.setPort(smtpPort);
                emailDestination.setDefaultFrom("from@default.sgtest");
                emailDestination.setDefaultBcc("bcc1@default.sgtest", "bcc2@default.sgtest");

                AccountRegistry accountRegistry = Mockito.mock(AccountRegistry.class);
                Mockito.when(accountRegistry.lookupAccount("test_destination", EmailAccount.class)).thenReturn(emailDestination);

                EmailAction emailAction = new EmailAction();
                emailAction.setBody("We searched {{data.x}} shards");
                emailAction.setSubject("Test Subject");
                emailAction.setTo(Collections.singletonList("to@specific.sgtest"));
                emailAction.setAccount("test_destination");

                Attachment attachment = new EmailAction.Attachment();
                attachment.setType(Attachment.AttachmentType.RUNTIME);

                Attachment attachment2 = new EmailAction.Attachment();
                attachment2.setType(Attachment.AttachmentType.REQUEST);
                attachment2.setRequestConfig(httpRequestConfig);
                attachment2.setHttpClientConfig(httpClientConfig);

                emailAction.setAttachments(ImmutableMap.of("test", attachment, "test2", attachment2));

                emailAction.compileScripts(new WatchInitializationService(accountRegistry, scriptService));

                WatchExecutionContext ctx = new WatchExecutionContext(client, scriptService, xContentRegistry, accountRegistry,
                        ExecutionEnvironment.SCHEDULED, ActionInvocationType.ALERT, new WatchExecutionContextData(runtimeData));

                emailAction.execute(ctx);

                if (!greenMail.waitForIncomingEmail(20000, 1)) {
                    Assert.fail("Timeout waiting for mails");
                }

                String receivedMail = GreenMailUtil.getWholeMessage(greenMail.getReceivedMessages()[0]);

                Assert.assertTrue(receivedMail, receivedMail.contains("We searched y shards"));
                Assert.assertTrue(receivedMail, receivedMail.contains("Content-Disposition: attachment; filename=test"));
                Assert.assertTrue(receivedMail, receivedMail.contains("{\"path\":\"hook\",\"x\":\"y\",\"body\":\"stuff\"}"));
                Assert.assertTrue(receivedMail, receivedMail.contains("Content-Disposition: attachment; filename=test2"));
                Assert.assertTrue(receivedMail, receivedMail.contains("Mockery"));
                Assert.assertTrue(receivedMail, receivedMail.contains("Subject: Test Subject"));
                Assert.assertTrue(receivedMail, receivedMail.contains("From: from@default.sgtest"));
                Assert.assertTrue(receivedMail, receivedMail.contains("To: to@specific.sgtest"));
            }
        } finally {
            greenMail.stop();
        }
    }

    @Test
    public void testEmailActionWithPDFRequestAndRuntimeData() throws Exception {
        final int smtpPort = SocketUtils.findAvailableTcpPort();

        GreenMail greenMail = new GreenMail(new ServerSetup(smtpPort, "127.0.0.1", ServerSetup.PROTOCOL_SMTP));
        greenMail.start();

        try (Client client = cluster.getAdminCertClient()) {

            byte[] pdf = IOUtils.toByteArray(FileHelper.getAbsoluteFilePathFromClassPath("blank_email_attachment.pdf").toUri());

            try (MockWebserviceProvider webhookProvider = new MockWebserviceProvider("/hook", pdf, "application/pdf")) {

                NestedValueMap runtimeData = new NestedValueMap();
                runtimeData.put("path", "hook");
                runtimeData.put("body", "stuff");
                runtimeData.put("x", "y");
                HttpRequestConfig httpRequestConfig = new HttpRequestConfig(HttpRequestConfig.Method.POST, new URI(webhookProvider.getUri()),
                        "/{{data.path}}", null, "{{data.body}}", null, null, null);
                HttpClientConfig httpClientConfig = new HttpClientConfig(null, null, null);

                httpRequestConfig.compileScripts(new WatchInitializationService(null, scriptService));

                EmailAccount emailDestination = new EmailAccount();
                emailDestination.setHost("localhost");
                emailDestination.setPort(smtpPort);
                emailDestination.setDefaultFrom("from@default.sgtest");
                emailDestination.setDefaultBcc("bcc1@default.sgtest", "bcc2@default.sgtest");

                AccountRegistry accountRegistry = Mockito.mock(AccountRegistry.class);
                Mockito.when(accountRegistry.lookupAccount("test_destination", EmailAccount.class)).thenReturn(emailDestination);

                EmailAction emailAction = new EmailAction();
                emailAction.setBody("We searched {{data.x}} shards");
                emailAction.setSubject("Test Subject");
                emailAction.setTo(Collections.singletonList("to@specific.sgtest"));
                emailAction.setAccount("test_destination");

                Attachment attachment = new EmailAction.Attachment();
                attachment.setType(Attachment.AttachmentType.RUNTIME);

                Attachment attachment2 = new EmailAction.Attachment();
                attachment2.setType(Attachment.AttachmentType.REQUEST);
                attachment2.setRequestConfig(httpRequestConfig);
                attachment2.setHttpClientConfig(httpClientConfig);

                emailAction.setAttachments(ImmutableMap.of("test", attachment, "test2", attachment2));

                emailAction.compileScripts(new WatchInitializationService(accountRegistry, scriptService));

                WatchExecutionContext ctx = new WatchExecutionContext(client, scriptService, xContentRegistry, accountRegistry,
                        ExecutionEnvironment.SCHEDULED, ActionInvocationType.ALERT, new WatchExecutionContextData(runtimeData));

                emailAction.execute(ctx);

                if (!greenMail.waitForIncomingEmail(20000, 1)) {
                    Assert.fail("Timeout waiting for mails");
                }

                String receivedMail = GreenMailUtil.getWholeMessage(greenMail.getReceivedMessages()[0]);

                Assert.assertTrue(receivedMail, receivedMail.contains("We searched y shards"));
                Assert.assertTrue(receivedMail, receivedMail.contains("Content-Disposition: attachment; filename=test"));
                Assert.assertTrue(receivedMail, receivedMail.contains("{\"path\":\"hook\",\"x\":\"y\",\"body\":\"stuff\"}"));
                Assert.assertTrue(receivedMail, receivedMail.contains("Content-Disposition: attachment; filename=test2"));
                Assert.assertTrue(receivedMail, receivedMail.contains("Subject: Test Subject"));
                Assert.assertTrue(receivedMail, receivedMail.contains("From: from@default.sgtest"));
                Assert.assertTrue(receivedMail, receivedMail.contains("To: to@specific.sgtest"));
            }
        } finally {
            greenMail.stop();
        }
    }

    @Test
    public void testEmailActionWithJSONRequestsAndRuntimeData() throws Exception {
        final int smtpPort = SocketUtils.findAvailableTcpPort();

        GreenMail greenMail = new GreenMail(new ServerSetup(smtpPort, "127.0.0.1", ServerSetup.PROTOCOL_SMTP));
        greenMail.start();

        try (Client client = cluster.getAdminCertClient()) {

            String helloWorld = "{\n" + "   \"hello\":\"world\"\n" + "}";

            try (MockWebserviceProvider webhookProvider = new MockWebserviceProvider("/hook", helloWorld.getBytes(), "application/json")) {

                NestedValueMap runtimeData = new NestedValueMap();
                runtimeData.put("path", "hook");
                runtimeData.put("body", "stuff");
                runtimeData.put("x", "y");

                HttpRequestConfig httpRequestConfig = new HttpRequestConfig(HttpRequestConfig.Method.POST, new URI(webhookProvider.getUri()),
                        "/{{data.path}}", null, "{{data.body}}", null, null, null);
                HttpClientConfig httpClientConfig = new HttpClientConfig(null, null, null);

                httpRequestConfig.compileScripts(new WatchInitializationService(null, scriptService));

                EmailAccount emailDestination = new EmailAccount();
                emailDestination.setHost("localhost");
                emailDestination.setPort(smtpPort);
                emailDestination.setDefaultFrom("from@default.sgtest");
                emailDestination.setDefaultBcc("bcc1@default.sgtest", "bcc2@default.sgtest");

                AccountRegistry accountRegistry = Mockito.mock(AccountRegistry.class);
                Mockito.when(accountRegistry.lookupAccount("test_destination", EmailAccount.class)).thenReturn(emailDestination);

                EmailAction emailAction = new EmailAction();
                emailAction.setBody("We searched {{data.x}} shards");
                emailAction.setSubject("Test Subject");
                emailAction.setTo(Collections.singletonList("to@specific.sgtest"));
                emailAction.setAccount("test_destination");

                Attachment attachment = new EmailAction.Attachment();
                attachment.setType(Attachment.AttachmentType.RUNTIME);

                Attachment attachment2 = new EmailAction.Attachment();
                attachment2.setType(Attachment.AttachmentType.REQUEST);
                attachment2.setRequestConfig(httpRequestConfig);
                attachment2.setHttpClientConfig(httpClientConfig);

                Attachment attachment3 = new EmailAction.Attachment();
                attachment3.setType(Attachment.AttachmentType.REQUEST);
                attachment3.setRequestConfig(httpRequestConfig);
                attachment3.setHttpClientConfig(httpClientConfig);

                emailAction.setAttachments(ImmutableMap.of("attachment3", attachment3, "test", attachment, "test2", attachment2));

                emailAction.compileScripts(new WatchInitializationService(accountRegistry, scriptService));

                WatchExecutionContext ctx = new WatchExecutionContext(client, scriptService, xContentRegistry, accountRegistry,
                        ExecutionEnvironment.SCHEDULED, ActionInvocationType.ALERT, new WatchExecutionContextData(runtimeData));

                emailAction.execute(ctx);

                if (!greenMail.waitForIncomingEmail(20000, 1)) {
                    Assert.fail("Timeout waiting for mails");
                }

                String receivedMail = GreenMailUtil.getWholeMessage(greenMail.getReceivedMessages()[0]);

                Assert.assertTrue(receivedMail, receivedMail.contains("We searched y shards"));
                Assert.assertTrue(receivedMail, receivedMail.contains("Content-Disposition: attachment; filename=test"));
                Assert.assertTrue(receivedMail, receivedMail.contains("{\"path\":\"hook\",\"x\":\"y\",\"body\":\"stuff\"}"));
                Assert.assertTrue(receivedMail, receivedMail.contains("Content-Disposition: attachment; filename=test2"));
                Assert.assertTrue(receivedMail, receivedMail.contains("\"hello\":\"world\""));
                Assert.assertTrue(receivedMail, receivedMail.contains("Content-Disposition: attachment; filename=attachment3"));
                Assert.assertTrue(receivedMail, receivedMail.contains("\"hello\":\"world\""));
                Assert.assertTrue(receivedMail, receivedMail.contains("Subject: Test Subject"));
                Assert.assertTrue(receivedMail, receivedMail.contains("From: from@default.sgtest"));
                Assert.assertTrue(receivedMail, receivedMail.contains("To: to@specific.sgtest"));
            }
        } finally {
            greenMail.stop();
        }
    }
}
