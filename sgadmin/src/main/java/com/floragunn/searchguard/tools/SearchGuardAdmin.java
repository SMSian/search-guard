/*
 * Copyright 2015-2017 floragunn GmbH
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

package com.floragunn.searchguard.tools;

import java.io.ByteArrayInputStream;
import java.io.Console;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.http.HttpHost;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksRequest;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest.Feature;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginInfo;
import org.elasticsearch.transport.Netty4Plugin;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchguard.DefaultObjectMapper;
import com.floragunn.searchguard.SearchGuardPlugin;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateAction;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateNodeResponse;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateRequest;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateResponse;
import com.floragunn.searchguard.action.licenseinfo.LicenseInfoAction;
import com.floragunn.searchguard.action.licenseinfo.LicenseInfoRequest;
import com.floragunn.searchguard.action.licenseinfo.LicenseInfoResponse;
import com.floragunn.searchguard.action.whoami.WhoAmIAction;
import com.floragunn.searchguard.action.whoami.WhoAmIRequest;
import com.floragunn.searchguard.action.whoami.WhoAmIResponse;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.ssl.util.ExceptionUtils;
import com.floragunn.searchguard.ssl.util.SSLConfigConstants;
import com.floragunn.searchguard.ssl.util.config.ClientAuthCredentials;
import com.floragunn.searchguard.ssl.util.config.GenericSSLConfig;
import com.floragunn.searchguard.ssl.util.config.TrustStore;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.SgJsonNode;
import com.floragunn.searchguard.support.SgUtils;
import com.floragunn.searchguard.tools.sgadmin.SearchGuardAdminRestClient;
import com.floragunn.searchguard.tools.sgadmin.SearchGuardAdminRestClient.GenericResponse;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;

@SuppressWarnings("deprecation")
public class SearchGuardAdmin {

    private static final boolean CREATE_AS_LEGACY = Boolean.parseBoolean(System.getenv("SG_ADMIN_CREATE_AS_LEGACY"));
    private static final boolean ALLOW_MIXED = Boolean.parseBoolean(System.getenv("SG_ADMIN_ALLOW_MIXED_CLUSTER"));
    private static final String SG_TS_PASS = "SG_TS_PASS";
    private static final String SG_KS_PASS = "SG_KS_PASS";
    private static final String SG_KEYPASS = "SG_KEYPASS";
    //not used in multithreaded fashion, so it's okay to define it as a constant here
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MMM-dd_HH-mm-ss", Locale.ENGLISH); //NOSONAR
    private static final Settings ENABLE_ALL_ALLOCATIONS_SETTINGS = Settings.builder()
            .put("cluster.routing.allocation.enable", "all")
            .build();
    
    public static void main(final String[] args) {
        try {
            final int returnCode = execute(args);
            System.exit(returnCode);
        } catch (NoNodeAvailableException e) {
            System.out.println("ERR: Cannot connect to Elasticsearch. Please refer to elasticsearch logfile for more information");
            System.out.println("Trace:");
            System.out.println(ExceptionsHelper.stackTrace(e));
            System.out.println();
            System.exit(-1);
        } 
        catch (IndexNotFoundException e) {
            System.out.println("ERR: No Search Guard configuration index found. Please execute sgadmin with different command line parameters");
            System.out.println("When you run it for the first time do not specify -us, -era, -dra or -rl");
            System.out.println("For more information please look here: https://docs.search-guard.com/latest/troubleshooting-sgadmin");
            System.out.println();
            System.exit(-1);
        }
        catch (Throwable e) {
            
            if (e instanceof ElasticsearchException 
                    && e.getMessage() != null 
                    && e.getMessage().contains("no permissions")) {

                System.out.println("ERR: You try to connect with a TLS node certificate instead of an admin client certificate");
                System.out.println("For more information please look here: https://docs.search-guard.com/latest/troubleshooting-sgadmin");
                System.out.println();
                System.exit(-1);
            }
            
            System.out.println("ERR: An unexpected "+e.getClass().getSimpleName()+" occured: "+e.getMessage());
            System.out.println("Trace:");
            System.out.println(ExceptionsHelper.stackTrace(e));
            System.out.println();
            System.exit(-1);
        }
    }

    public static int execute(final String[] args) throws Exception {
        
        System.out.println("Search Guard Admin v7");
        System.setProperty("sg.nowarn.client","true");
        System.setProperty("jdk.tls.rejectClientInitiatedRenegotiation","true");

        final HelpFormatter formatter = new HelpFormatter();
        Options options = new Options();
        options.addOption( "nhnv", "disable-host-name-verification", false, "Disable hostname verification" );
        options.addOption( "nrhn", "disable-resolve-hostname", false, "Disable DNS lookup of hostnames" );
        options.addOption(Option.builder("ts").longOpt("truststore").hasArg().argName("file").desc("Path to truststore (JKS/PKCS12 format)").build());
        options.addOption(Option.builder("ks").longOpt("keystore").hasArg().argName("file").desc("Path to keystore (JKS/PKCS12 format").build());
        options.addOption(Option.builder("tst").longOpt("truststore-type").hasArg().argName("type").desc("JKS or PKCS12, if not given we use the file extension to dectect the type").build());
        options.addOption(Option.builder("kst").longOpt("keystore-type").hasArg().argName("type").desc("JKS or PKCS12, if not given we use the file extension to dectect the type").build());
        options.addOption(Option.builder("tspass").longOpt("truststore-password").hasArg().argName("password").desc("Truststore password").build());
        options.addOption(Option.builder("kspass").longOpt("keystore-password").hasArg().argName("password").desc("Keystore password").build());
        options.addOption(Option.builder("cd").longOpt("configdir").hasArg().argName("directory").desc("Directory for config files").build());
        options.addOption(Option.builder("h").longOpt("hostname").hasArg().argName("host").desc("Elasticsearch host (default: localhost)").build());
        options.addOption(Option.builder("p").longOpt("port").hasArg().argName("port").desc("Elasticsearch transport port (default: 9300)").build());
        options.addOption(Option.builder("cn").longOpt("clustername").hasArg().argName("clustername").desc("Clustername (do not use together with -icl)").build());
        options.addOption( "sniff", "enable-sniffing", false, "Enable client.transport.sniff" );
        options.addOption( "icl", "ignore-clustername", false, "Ignore clustername (do not use together with -cn)" );
        options.addOption(Option.builder("r").longOpt("retrieve").desc("retrieve current config").build());
        options.addOption(Option.builder("f").longOpt("file").hasArg().argName("file").desc("file").build());
        options.addOption(Option.builder("t").longOpt("type").hasArg().argName("file-type").desc("file-type").build());
        options.addOption(Option.builder("tsalias").longOpt("truststore-alias").hasArg().argName("alias").desc("Truststore alias").build());
        options.addOption(Option.builder("ksalias").longOpt("keystore-alias").hasArg().argName("alias").desc("Keystore alias").build());
        options.addOption(Option.builder("ec").longOpt("enabled-ciphers").hasArg().argName("cipers").desc("Comma separated list of enabled TLS ciphers").build());
        options.addOption(Option.builder("ep").longOpt("enabled-protocols").hasArg().argName("protocols").desc("Comma separated list of enabled TLS protocols").build());
        //TODO mark as deprecated and replace it with "era" if "era" is mature enough
        options.addOption(Option.builder("us").longOpt("update_settings").hasArg().argName("number of replicas").desc("Update the number of Search Guard index replicas, reload configuration on all nodes and exit").build());
        options.addOption(Option.builder("i").longOpt("index").hasArg().argName("indexname").desc("The index Search Guard uses to store the configuration").build());
        options.addOption(Option.builder("era").longOpt("enable-replica-autoexpand").desc("Enable replica auto expand and exit").build());
        options.addOption(Option.builder("dra").longOpt("disable-replica-autoexpand").desc("Disable replica auto expand and exit").build());
        options.addOption(Option.builder("rl").longOpt("reload").desc("Reload the configuration on all nodes, flush all Search Guard caches and exit").build());
        options.addOption(Option.builder("ff").longOpt("fail-fast").desc("fail-fast if something goes wrong").build());
        options.addOption(Option.builder("dg").longOpt("diagnose").desc("Log diagnostic trace into a file").build());
        options.addOption(Option.builder("dci").longOpt("delete-config-index").desc("Delete 'searchguard' config index and exit.").build());
        options.addOption(Option.builder("esa").longOpt("enable-shard-allocation").desc("Enable all shard allocation and exit.").build());
        options.addOption(Option.builder("arc").longOpt("accept-red-cluster").desc("Also operate on a red cluster. If not specified the cluster state has to be at least yellow.").build());

        options.addOption(Option.builder("cacert").hasArg().argName("file").desc("Path to trusted cacert (PEM format)").build());
        options.addOption(Option.builder("cert").hasArg().argName("file").desc("Path to admin certificate in PEM format").build());
        options.addOption(Option.builder("key").hasArg().argName("file").desc("Path to the key of admin certificate").build());
        options.addOption(Option.builder("keypass").hasArg().argName("password").desc("Password of the key of admin certificate (optional)").build());

        options.addOption(Option.builder("si").longOpt("show-info").desc("Show system and license info").build());

        options.addOption(Option.builder("w").longOpt("whoami").desc("Show information about the used admin certificate").build());

        options.addOption(Option.builder("prompt").longOpt("prompt-for-password").desc("Prompt for password if not supplied").build());

        options.addOption(Option.builder("er").longOpt("explicit-replicas").hasArg().argName("number of replicas").desc("Set explicit number of replicas or autoexpand expression for searchguard index").build());

        options.addOption(Option.builder("backup").hasArg().argName("folder").desc("Backup configuration to folder").build());

        options.addOption(Option.builder("migrate").hasArg().argName("folder").desc("Migrate and use folder to store migrated files").build());
        
        options.addOption(Option.builder("rev").longOpt("resolve-env-vars").desc("Resolve/Substitute env vars in config with their value before uploading").build());

        options.addOption(Option.builder("vc").numberOfArgs(1).optionalArg(true).argName("version").longOpt("validate-configs").desc("Validate config for version 6 or 7 (default 7)").build());

        options.addOption(Option.builder("mo").longOpt("migrate-offline").hasArg().argName("folder").desc("Migrate and use folder to store migrated files").build());

        options.addOption(Option.builder("rlhttpcerts").longOpt("reload-http-certs").desc("Trigger reloading the HTTP TLS certificiate files installed on the nodes").build());
        options.addOption(Option.builder("rltransportcerts").longOpt("reload-transport-certs").desc("Trigger reloading the transport TLS certificiate files installed on the nodes").build());

        //when adding new options also adjust validate(CommandLine line)
        
        String hostname = "localhost";
        int port = 9300;
        String kspass = System.getenv(SG_KS_PASS);
        String tspass = System.getenv(SG_TS_PASS);
        String cd = ".";
        String ks = null;
        String ts = null;
        String kst = null;
        String tst = null;
        boolean nhnv = false;
        boolean nrhn = false;
        boolean sniff = false;
        boolean icl = false;
        String clustername = "elasticsearch";
        String file = null;
        String type = null;
        boolean retrieve = false;
        String ksAlias = null;
        String tsAlias = null;
        String[] enabledProtocols = new String[0];
        String[] enabledCiphers = new String[0];
        Integer updateSettings = null;
        String index = ConfigConstants.SG_DEFAULT_CONFIG_INDEX;
        Boolean replicaAutoExpand = null;
        boolean reload = false;
        boolean failFast = false;
        boolean diagnose = false;
        boolean deleteConfigIndex = false;
        boolean enableShardAllocation = false;
        boolean acceptRedCluster = false;
        
        String keypass = System.getenv(SG_KEYPASS);
        //boolean simpleAuth = false;
        String cacert = null;
        String cert = null;
        String key = null;
        boolean si;
        boolean whoami;
        final boolean promptForPassword;
        String explicitReplicas = null;
        String backup = null;
        final boolean resolveEnvVars;
        Integer validateConfig = null;
        boolean reloadHttpCerts = false;
        boolean reloadTransportCerts = false;
        
        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine line = parser.parse( options, args );
            
            validate(line);
            
            hostname = line.getOptionValue("h", hostname);
            port = Integer.parseInt(line.getOptionValue("p", String.valueOf(port)));

            promptForPassword = line.hasOption("prompt");
            
            if(kspass == null || kspass.isEmpty()) {
                kspass = line.getOptionValue("kspass",promptForPassword?null:"changeit");
            }
            
            if(tspass == null || tspass.isEmpty()) {
                tspass = line.getOptionValue("tspass",promptForPassword?null:kspass);
            }

            cd = line.getOptionValue("cd", cd);
            
            if(!cd.endsWith(File.separator)) {
                cd += File.separator;
            }
            
            ks = line.getOptionValue("ks",ks);
            ts = line.getOptionValue("ts",ts);
            kst = line.getOptionValue("kst", kst);
            tst = line.getOptionValue("tst", tst);
            nhnv = line.hasOption("nhnv");
            nrhn = line.hasOption("nrhn");
            clustername = line.getOptionValue("cn", clustername);
            sniff = line.hasOption("sniff");
            icl = line.hasOption("icl");
            file = line.getOptionValue("f", file);
            type = line.getOptionValue("t", type);
            retrieve = line.hasOption("r");
            ksAlias = line.getOptionValue("ksalias", ksAlias);
            tsAlias = line.getOptionValue("tsalias", tsAlias);
            index = line.getOptionValue("i", index);
            
            String enabledCiphersString = line.getOptionValue("ec", null);
            String enabledProtocolsString = line.getOptionValue("ep", null);
            
            if(enabledCiphersString != null) {
                enabledCiphers = enabledCiphersString.split(",");
            }
            
            if(enabledProtocolsString != null) {
                enabledProtocols = enabledProtocolsString.split(",");
            }
            
            updateSettings = line.hasOption("us")?Integer.parseInt(line.getOptionValue("us")):null;

            reload = line.hasOption("rl");
            
            if(line.hasOption("era")) {
                replicaAutoExpand = true;
            }
            
            if(line.hasOption("dra")) {
                replicaAutoExpand = false;
            }
            
            failFast = line.hasOption("ff");
            diagnose = line.hasOption("dg");
            deleteConfigIndex = line.hasOption("dci");
            enableShardAllocation = line.hasOption("esa");
            acceptRedCluster = line.hasOption("arc");
            
            cacert = line.getOptionValue("cacert");
            cert = line.getOptionValue("cert");
            key = line.getOptionValue("key");
            keypass = line.getOptionValue("keypass", keypass);
                        
            si = line.hasOption("si");
            
            whoami = line.hasOption("w");
            
            explicitReplicas = line.getOptionValue("er", explicitReplicas);
            
            backup = line.getOptionValue("backup");
                        
            resolveEnvVars = line.hasOption("rev");
            
            validateConfig = !line.hasOption("vc")?null:Integer.parseInt(line.getOptionValue("vc", "7"));
            
            if(validateConfig != null && validateConfig.intValue() != 6 && validateConfig.intValue() != 7) {
                throw new ParseException("version must be 6 or 7");
            }
                        
            reloadHttpCerts = line.hasOption("reload-http-certs");
            reloadTransportCerts = line.hasOption("reload-transport-certs");

        }
        catch( ParseException exp ) {
            System.out.println("ERR: Parsing failed.  Reason: " + exp.getMessage());
            formatter.printHelp("sgadmin.sh", options, true);
            return -1;
        }
        
        if (reloadHttpCerts || reloadTransportCerts) {
            TrustStore trustStore = null;

            if (cacert != null) {
                try {
                    trustStore = TrustStore.from().certPem(new File(cacert)).build();
                } catch (Exception e) {
                    System.err.println("ERR: Error while loading trust store file" + ts + ": " + e);
                    e.printStackTrace();
                    return -1;
                }
            } else if (ts != null) {
                try {
                    trustStore = TrustStore.from().keyStore(new File(ts), tspass, tst).build();

                    if (tsAlias != null) {
                        System.err.println("ERR: The tsalias option is not supported for REST actions");
                        return -1;
                    }
                } catch (Exception e) {
                    System.err.println("ERR: " + e);
                    e.printStackTrace();
                    return -1;
                }
            }

            ClientAuthCredentials clientAuthCredentials = null;

            if (cert != null) {
                try {
                    clientAuthCredentials = ClientAuthCredentials.from().certPem(new File(cert)).certKeyPem(new File(key), keypass).build();
                } catch (Exception e) {
                    System.err.println("ERR: " + e);
                    e.printStackTrace();
                    return -1;
                }
            } else if (ks != null) {
                try {
                    clientAuthCredentials = ClientAuthCredentials.from().keyStore(new File(ks), ksAlias, kspass, kst).build();
                } catch (Exception e) {
                    System.err.println("ERR: " + e);
                    e.printStackTrace();
                    return -1;
                }
            }

            SSLIOSessionStrategy sslIoSessionStrategy = new GenericSSLConfig.Builder().useTrustStore(trustStore).useClientAuth(clientAuthCredentials)
                    .verifyHostnames(!nhnv).useCiphers(enabledCiphers.length > 0 ? enabledCiphers : null)
                    .useProtocols(enabledProtocols.length > 0 ? enabledProtocols : null).toSSLIOSessionStrategy();

            RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost(hostname, port, "https"))
                    .setHttpClientConfigCallback(builder -> builder.setSSLStrategy(sslIoSessionStrategy));

            try (SearchGuardAdminRestClient client = new SearchGuardAdminRestClient(restClientBuilder)) {
                if (reloadHttpCerts) {
                    try {
                        System.out.println("Reloading HTTP certificates ...");

                        GenericResponse response = client.reloadHttpCerts();
                        
                        System.out.println(response.getMessage());
                    } catch (ElasticsearchStatusException e) {
                        System.err.println("ERR: Error while reloading HTTP certificates: " + e);
                        return -1;
                    }
                }

                if (reloadTransportCerts) {
                    try {
                        System.out.println("Reloading transport certificates ...");
                        
                        GenericResponse response = client.reloadTransportCerts();
                        
                        System.out.println(response.getMessage());
                    } catch (ElasticsearchStatusException e) {
                        System.err.println("ERR: Error while reloading HTTP certificates: " + e);
                        return -1;
                    }
                }
            }

            return 0;
        }
        
        if(port < 9300) {
            System.out.println("WARNING: Seems you want connect to the Elasticsearch HTTP port."+System.lineSeparator()
                             + "         sgadmin connects on the transport port which is normally 9300.");
        }
        
        System.out.print("Will connect to "+hostname+":"+port);
        Socket socket = new Socket();
        
        try {
            
            socket.connect(new InetSocketAddress(hostname, port));
            
          } catch (java.net.ConnectException ex) {
            System.out.println();
            System.out.println("ERR: Seems there is no Elasticsearch running on "+hostname+":"+port+" - Will exit");
            return (-1);
          } finally {
              try {
                socket.close();
            } catch (Exception e) {
                //ignore
            }
          }

        System.out.println(" ... done");
        
        final Settings.Builder settingsBuilder = Settings
                .builder()
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENFORCE_HOSTNAME_VERIFICATION, !nhnv)
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENFORCE_HOSTNAME_VERIFICATION_RESOLVE_HOST_NAME, !nrhn)
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENABLED, true)
                .putList(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENABLED_CIPHERS, enabledCiphers)
                .putList(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENABLED_PROTOCOLS, enabledProtocols)
                
                .put("cluster.name", clustername)
                .put("client.transport.ignore_cluster_name", icl)
                .put("client.transport.sniff", sniff);
                
                if(ksAlias != null) {
                    settingsBuilder.put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_ALIAS, ksAlias);
                }
                
                if(tsAlias != null) {
                    settingsBuilder.put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_ALIAS, tsAlias);
                }
                
                if(ks != null) {
                    settingsBuilder.put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_FILEPATH, ks);
                    settingsBuilder.put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_TYPE, kst==null?(ks.endsWith(".jks")?"JKS":"PKCS12"):kst);
                    
                    if(kspass == null && promptForPassword) {
                        kspass = promptForPassword("Keystore", "kspass", SG_KS_PASS);
                    }
                    
                    if(kspass != null) {
                        settingsBuilder.put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_PASSWORD, kspass);
                    }
                }
                
                if(ts != null) {
                    settingsBuilder.put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_FILEPATH, ts);
                    settingsBuilder.put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_TYPE, tst==null?(ts.endsWith(".jks")?"JKS":"PKCS12"):tst);
                    
                    if(tspass == null && promptForPassword) {
                        tspass = promptForPassword("Truststore", "tspass", SG_TS_PASS);
                    }
                    
                    if(tspass != null) {
                        settingsBuilder.put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_PASSWORD, tspass);
                    }
                }            
                
                if(cacert != null) {
                    settingsBuilder.put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMTRUSTEDCAS_FILEPATH, cacert);
                }
                
                if(cert != null) {
                    settingsBuilder.put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMCERT_FILEPATH, cert);
                }
                
                if(key != null) {
                    settingsBuilder.put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMKEY_FILEPATH, key);
                    
                    if(keypass == null && promptForPassword) {
                        keypass = promptForPassword("Pemkey", "keypass", SG_KEYPASS);
                    }
                    
                    if(keypass != null) {
                        settingsBuilder.put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMKEY_PASSWORD, keypass);
                    }
                }

                Settings settings = settingsBuilder.build();  

        try (@SuppressWarnings("resource")
        TransportClient tc = new TransportClientImpl(settings, asCollection(Netty4Plugin.class, SearchGuardPlugin.class))
                .addTransportAddress(new TransportAddress(new InetSocketAddress(hostname, port)))) {
            
            final WhoAmIResponse whoAmIRes = tc.execute(WhoAmIAction.INSTANCE, new WhoAmIRequest()).actionGet();
            System.out.println("Connected as "+whoAmIRes.getDn());

            if(!whoAmIRes.isAdmin()) {
                
                System.out.println("ERR: "+whoAmIRes.getDn()+" is not an admin user");
                
                if(!whoAmIRes.isNodeCertificateRequest()) {
                    System.out.println("Seems you use a client certificate but this one is not registered as admin_dn");
                    System.out.println("Make sure elasticsearch.yml on all nodes contains:");
                    System.out.println("searchguard.authcz.admin_dn:"+System.lineSeparator()+
                                       "  - \""+whoAmIRes.getDn()+"\"");
                } else {
                    System.out.println("Seems you use a node certificate. This is not permitted, you have to use a client certificate and register it as admin_dn in elasticsearch.yml");
                }
                return (-1);
            } else if(whoAmIRes.isNodeCertificateRequest()) {
                System.out.println("ERR: Seems you use a node certificate which is also an admin certificate");
                System.out.println("     That may have worked with older Search Guard versions but it indicates");
                System.out.println("     a configuration error and is therefore forbidden now.");
                System.out.println("     Pls refer to https://docs.search-guard.com/latest/tls-in-production");
                
                if(failFast) {
                    return (-1);
                }

            }
            
            try {
                if(issueWarnings(tc) != 0) {
                    return (-1);
                }
            } catch (Exception e1) {
                System.out.println("Unable to check whether cluster is sane");
                throw e1;
            }

            if(updateSettings != null) { 
                Settings indexSettings = Settings.builder().put("index.number_of_replicas", updateSettings).build();                
                ConfigUpdateResponse res = tc.execute(ConfigUpdateAction.INSTANCE, new ConfigUpdateRequest(getTypes(false))).actionGet();
                if(res.hasFailures()) {
                    System.out.println("ERR: Unabe to reload config due to "+res.failures());
                }
                final AcknowledgedResponse response = tc.admin().indices().updateSettings((new UpdateSettingsRequest(index).settings(indexSettings))).actionGet();
                System.out.println("Reload config on all nodes");
                System.out.println("Update number of replicas to "+(updateSettings) +" with result: "+response.isAcknowledged());
                return ((response.isAcknowledged() && !res.hasFailures())?0:-1);
            }
            
            if(reload) { 
                ConfigUpdateResponse res = tc.execute(ConfigUpdateAction.INSTANCE, new ConfigUpdateRequest(getTypes(false))).actionGet();                
                if(res.hasFailures()) {
                    System.out.println("ERR: Unabe to reload config due to "+res.failures());
                    return -1;
                }
                System.out.println("Reload config on all nodes");
                return 0;
            }
            
            if(si) { 
                LicenseInfoResponse res = tc.execute(LicenseInfoAction.INSTANCE, new LicenseInfoRequest()).actionGet();
                if(res.hasFailures()) {
                    System.out.println("ERR: Unabe to load license due to "+res.failures());
                    return -1;
                }
                System.out.println(res.toString());
                return (0);
            }
            
            if(whoami) {
                System.out.println(whoAmIRes.toString());
                return (0);
            }
            
            
            if(replicaAutoExpand != null) { 
                Settings indexSettings = Settings.builder()
                        .put("index.auto_expand_replicas", replicaAutoExpand?"0-all":"false")
                        .build();                
                ConfigUpdateResponse res = tc.execute(ConfigUpdateAction.INSTANCE, new ConfigUpdateRequest(getTypes(false))).actionGet();                
                if(res.hasFailures()) {
                    System.out.println("ERR: Unabe to reload config due to "+res.failures());
                }
                final AcknowledgedResponse response = tc.admin().indices().updateSettings((new UpdateSettingsRequest(index).settings(indexSettings))).actionGet();
                System.out.println("Reload config on all nodes");
                System.out.println("Auto-expand replicas "+(replicaAutoExpand?"enabled":"disabled"));
                return ((response.isAcknowledged() && !res.hasFailures())?0:-1);
            }   
            
            if(enableShardAllocation) { 
                final boolean successful = tc.admin().cluster()
                        .updateSettings(new ClusterUpdateSettingsRequest()
                        .transientSettings(ENABLE_ALL_ALLOCATIONS_SETTINGS)
                        .persistentSettings(ENABLE_ALL_ALLOCATIONS_SETTINGS))
                        .actionGet()
                        .isAcknowledged();
                
                if(successful) {
                    System.out.println("Persistent and transient shard allocation enabled");
                } else {
                    System.out.println("ERR: Unable to enable shard allocation");
                }
                
                return (successful?0:-1);
            }   
            
            if(failFast) {
                System.out.println("Fail-fast is activated");
            }
            
            if(diagnose) {
                generateDiagnoseTrace(tc);
            }
            
            System.out.println("Contacting elasticsearch cluster '"+clustername+"'"+(acceptRedCluster?"":" and wait for YELLOW clusterstate")+" ...");
            
            ClusterHealthResponse chr = null;
            
            while(chr == null) {
                try {
                    final ClusterHealthRequest chrequest = new ClusterHealthRequest().timeout(TimeValue.timeValueMinutes(5));
                    if(!acceptRedCluster) {
                        chrequest.waitForYellowStatus();
                    }
                    chr = tc.admin().cluster().health(chrequest).actionGet();
                } catch (Exception e) {
                    
                    Throwable rootCause = ExceptionUtils.getRootCause(e);
                    
                    if(!failFast) {
                        System.out.println("Cannot retrieve cluster state due to: "+e.getMessage()+". This is not an error, will keep on trying ...");
                        System.out.println("  Root cause: "+rootCause+" ("+e.getClass().getName()+"/"+rootCause.getClass().getName()+")");
                        System.out.println("   * Try running sgadmin.sh with -icl (but no -cl) and -nhnv (If that works you need to check your clustername as well as hostnames in your TLS certificates)");   
                        System.out.println("   * Make sure that your keystore or PEM certificate is a client certificate (not a node certificate) and configured properly in elasticsearch.yml"); 
                        System.out.println("   * If this is not working, try running sgadmin.sh with --diagnose and see diagnose trace log file)");
                        System.out.println("   * Add --accept-red-cluster to allow sgadmin to operate on a red cluster.");

                    } else {
                        System.out.println("ERR: Cannot retrieve cluster state due to: "+e.getMessage()+".");
                        System.out.println("  Root cause: "+rootCause+" ("+e.getClass().getName()+"/"+rootCause.getClass().getName()+")");
                        System.out.println("   * Try running sgadmin.sh with -icl (but no -cl) and -nhnv (If that works you need to check your clustername as well as hostnames in your TLS certificates)");
                        System.out.println("   * Make also sure that your keystore or PEM certificate is a client certificate (not a node certificate) and configured properly in elasticsearch.yml"); 
                        System.out.println("   * If this is not working, try running sgadmin.sh with --diagnose and see diagnose trace log file)"); 
                        System.out.println("   * Add --accept-red-cluster to allow sgadmin to operate on a red cluster.");

                        return (-1);
                    }
                    
                    Thread.sleep(3000);
                    continue;
                }
            }

            final boolean timedOut = chr.isTimedOut();
            
            if (!acceptRedCluster && timedOut) {
                System.out.println("ERR: Timed out while waiting for a green or yellow cluster state.");
                System.out.println("   * Try running sgadmin.sh with -icl (but no -cl) and -nhnv (If that works you need to check your clustername as well as hostnames in your TLS certificates)");
                System.out.println("   * Make also sure that your keystore or PEM certificate is a client certificate (not a node certificate) and configured properly in elasticsearch.yml"); 
                System.out.println("   * If this is not working, try running sgadmin.sh with --diagnose and see diagnose trace log file)"); 
                System.out.println("   * Add --accept-red-cluster to allow sgadmin to operate on a red cluster.");
                return (-1);
            }
            
            System.out.println("Clustername: "+chr.getClusterName());
            System.out.println("Clusterstate: "+chr.getStatus());
            System.out.println("Number of nodes: "+chr.getNumberOfNodes());
            System.out.println("Number of data nodes: "+chr.getNumberOfDataNodes());
            
            GetIndexResponse sgIndex = null;
            try {
                sgIndex = tc.admin().indices().getIndex(new GetIndexRequest().indices(index).addFeatures(Feature.MAPPINGS)).actionGet();
            } catch (IndexNotFoundException e1) {
                //ignore
            }
            final boolean indexExists = sgIndex != null;
            
            final NodesInfoResponse nodesInfo = tc.admin().cluster().nodesInfo(new NodesInfoRequest()).actionGet();

            if(deleteConfigIndex) {
                return deleteConfigIndex(tc, index, indexExists);
            }
               
            if (!indexExists) {
                System.out.print(index +" index does not exists, attempt to create it ... ");
                final int created = createConfigIndex(tc, index, explicitReplicas);
                if(created != 0) {
                    return created;
                }

            } else {
                System.out.println(index+" index already exists, so we do not need to create one.");
                
                try {
                    ClusterHealthResponse chrsg = tc.admin().cluster().health(new ClusterHealthRequest(index)).actionGet();
                             
                    if (chrsg.isTimedOut()) {
                        System.out.println("ERR: Timed out while waiting for "+index+" index state.");
                    }
                    
                    if (chrsg.getStatus() == ClusterHealthStatus.RED) {
                        System.out.println("ERR: "+index+" index state is RED.");
                    }
                    
                    if (chrsg.getStatus() == ClusterHealthStatus.YELLOW) {
                        System.out.println("INFO: "+index+" index state is YELLOW, it seems you miss some replicas");
                    }
                    
                } catch (Exception e) {
                    if(!failFast) {
                        System.out.println("Cannot retrieve "+index+" index state state due to "+e.getMessage()+". This is not an error, will keep on trying ...");
                    } else {
                        System.out.println("ERR: Cannot retrieve "+index+" index state state due to "+e.getMessage()+".");
                        return (-1);
                    }
                }
            }
            
            final boolean createLegacyMode = !indexExists && CREATE_AS_LEGACY;

            if(createLegacyMode) {
                System.out.println("We forcibly create the new index in legacy mode so that ES 6 config can be uploaded. To move to v7 configs youneed to migrate.");
            }
            
            final boolean legacy = createLegacyMode || (indexExists 
                    && sgIndex.getMappings() != null
                    && sgIndex.getMappings().get(index) != null
                    && sgIndex.getMappings().get(index).containsKey("sg"));
            
            
            
            if(legacy) {
                System.out.println("Legacy index '"+index+"' (ES 6) detected (or forced). You should migrate the configuration!");
                System.out.println("See https://docs.search-guard.com/latest/upgrading-6-7 for more details.");
            }
            
            if(retrieve) {
                String date = DATE_FORMAT.format(new Date());
                
                boolean success = retrieveFile(tc, cd+"sg_config_"+date+".yml", index, "config", legacy);
                success = retrieveFile(tc, cd+"sg_roles_"+date+".yml", index, "roles", legacy) && success;
                success = retrieveFile(tc, cd+"sg_roles_mapping_"+date+".yml", index, "rolesmapping", legacy) && success;
                success = retrieveFile(tc, cd+"sg_internal_users_"+date+".yml", index, "internalusers", legacy) && success;
                success = retrieveFile(tc, cd+"sg_action_groups_"+date+".yml", index, "actiongroups", legacy) && success;
                if(!legacy) {
                    success = retrieveFile(tc, cd+"sg_tenants_"+date+".yml", index, "tenants", legacy) && success;
                    success = retrieveFile(tc, cd+"sg_blocks_"+date+".yml", index, "blocks", legacy) && success;
                    success = retrieveFile(tc, cd+"sg_frontend_authc_"+date+".yml", index, "frontend_authc", legacy) && success;
                }
                return (success?0:-1);
            }
            
            if(backup != null) {
                return backup(tc, index, new File(backup), legacy);
            }

            boolean isCdAbs = new File(cd).isAbsolute();

            System.out.println("Populate config from "+(isCdAbs?cd:new File(".", cd).getCanonicalPath()));
            
            if(file != null) {
                if(type != null) {
                    System.out.println("Force type: "+type);
                } else {
                    type = readTypeFromFile(new File(file));
                    if(type == null) {
                        System.out.println("ERR: Unable to read type from file");
                        return (-1);
                    }
                }
                
                if(!CType.lcStringValues().contains(type)) {
                    System.out.println("ERR: Invalid type '"+type+"'");
                    return (-1);
                }
                
                boolean success = uploadFile(tc, file, index, type, legacy, resolveEnvVars);
                
                if(!success) {
                    System.out.println("ERR: cannot upload configuration, see errors above");
                    return -1;
                }
                
                ConfigUpdateResponse cur = tc.execute(ConfigUpdateAction.INSTANCE, new ConfigUpdateRequest(new String[]{type})).actionGet();
                
                success = checkConfigUpdateResponse(cur, nodesInfo, 1) && success;
                
                System.out.println("Done with "+(success?"success":"failures"));
                return (success?0:-1);
            }
            
            return upload(tc, index, cd, legacy, nodesInfo, resolveEnvVars);
        }
        // TODO audit changes to searchguard index
    }

    private static boolean checkConfigUpdateResponse(ConfigUpdateResponse response, NodesInfoResponse nir, int expectedConfigCount) {
        
        final int expectedNodeCount =  nir.getNodes().size();

        if(response.hasFailures()) {
            System.out.println("FAIL: "+response.failures().size()+" nodes reported failures. First failure is "+response.failures().get(0));
        }
        
        boolean success = response.getNodes().size() == expectedNodeCount;
        if(!success) {
            System.out.println("FAIL: Expected "+expectedNodeCount+" nodes to return response, but got "+response.getNodes().size());
        }

        for(String nodeId: response.getNodesMap().keySet()) {
            ConfigUpdateNodeResponse node = response.getNodesMap().get(nodeId);
            boolean successNode = (node.getUpdatedConfigTypes() != null && node.getUpdatedConfigTypes().length == expectedConfigCount);
            if(!successNode) {
                System.out.println("FAIL: Expected "+expectedConfigCount+" config types for node "+nodeId+" but got "+node.getUpdatedConfigTypes().length+" ("+Arrays.toString(node.getUpdatedConfigTypes()) + ") due to: "+(node.getMessage()==null?"unknown reason":node.getMessage()));
            }
            
            success = success && successNode;
        }

        return success && !response.hasFailures();
    }
    
    private static boolean uploadFile(final Client tc, final String filepath, final String index, final String _id, final boolean legacy, boolean resolveEnvVars) {
        
        String type = "_doc";
        String id = _id;
                
        if(legacy) {
            type = "sg";
            id = _id;            
        } 
        
        System.out.println("Will update '"+type+"/" + id + "' with " + filepath+" "+(legacy?"(legacy mode)":""));
        
        try (Reader reader = new FileReader(filepath)) {
            final String content = CharStreams.toString(reader);
            final String res = tc
                    .index(new IndexRequest(index).type(type).id(id).setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                            .source(_id, readXContent(resolveEnvVars?SgUtils.replaceEnvVars(content, Settings.EMPTY):content, XContentType.YAML))).actionGet().getId();

            if (id.equals(res)) {
                System.out.println("   SUCC: Configuration for '" + _id + "' created or updated");
                return true;
            } else {
                System.out.println("   FAIL: Configuration for '" + _id
                        + "' failed for unknown reasons. Please consult the Elasticsearch logfile.");
            }
        } catch (Exception e) {
            System.out.println("   FAIL: Configuration for '" + _id + "' failed because of " + e.toString());
        }

        return false;
    }
    
    private static boolean retrieveFile(final Client tc, final String filepath, final String index, final String _id, final boolean legacy) {
        
        String type = "_doc";
        String id = _id;
                
        if(legacy) {
            type = "sg";
            id = _id;

        } 
        
        System.out.println("Will retrieve '"+type+"/" +id+"' into "+filepath+" "+(legacy?"(legacy mode)":""));
        try (Writer writer = new FileWriter(filepath)) {

            final GetResponse response = tc.get(new GetRequest(index).type(type).id(id).refresh(true).realtime(false)).actionGet();

            if (response.isExists()) {
                if(response.isSourceEmpty()) {
                    System.out.println("   FAIL: Configuration for '"+_id+"' failed because of empty source");
                    return false;
                }
                
                String yaml = convertToYaml(_id, response.getSourceAsBytesRef(), true);
                
                writer.write(yaml);
                System.out.println("   SUCC: Configuration for '"+_id+"' stored in "+filepath);
                return true;
            } else {
                System.out.println("   FAIL: Get configuration for '"+_id+"' because it does not exist");
            }
        } catch (Exception e) {
            System.out.println("   FAIL: Get configuration for '"+_id+"' failed because of "+e.toString());
        }
        
        return false;
    }

    private static BytesReference readXContent(final String content, final XContentType xContentType) throws IOException {
        BytesReference retVal;
        XContentParser parser = null;
        try {
            parser = XContentFactory.xContent(xContentType).createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, content);
            parser.nextToken();
            final XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.copyCurrentStructure(parser);
            retVal = BytesReference.bytes(builder);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
        
        //validate
        return retVal;
    }
    
    private static String convertToYaml(String type, BytesReference bytes, boolean prettyPrint) throws IOException {
        
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, bytes.streamInput())) {
            
            parser.nextToken();
            parser.nextToken();
            
            if(!type.equals((parser.currentName()))) {
                return null;
            }
            
            parser.nextToken();
            
            XContentBuilder builder = XContentFactory.yamlBuilder();
            if (prettyPrint) {
                builder.prettyPrint();
            }
            builder.rawValue(new ByteArrayInputStream(parser.binaryValue()), XContentType.YAML);
            return Strings.toString(builder);
        }
    }

    protected static class TransportClientImpl extends TransportClient {

        public TransportClientImpl(Settings settings, Collection<Class<? extends Plugin>> plugins) {
            super(settings, plugins);
        }

        public TransportClientImpl(Settings settings, Settings defaultSettings, Collection<Class<? extends Plugin>> plugins) {
            super(settings, defaultSettings, plugins, null);
        }       
    }
    
    @SafeVarargs
    protected static Collection<Class<? extends Plugin>> asCollection(Class<? extends Plugin>... plugins) {
        return Arrays.asList(plugins);
    }

    protected static void generateDiagnoseTrace(final Client tc) {
        
        final String date = DATE_FORMAT.format(new Date());
        
        final StringBuilder sb = new StringBuilder();
        sb.append("Diagnostic sgadmin trace"+System.lineSeparator());
        sb.append("ES client version: "+Version.CURRENT+System.lineSeparator());
        sb.append("Client properties: "+System.getProperties()+System.lineSeparator());
        sb.append(date+System.lineSeparator());
        sb.append(System.lineSeparator());
        
        try {
            sb.append("Who am i:"+System.lineSeparator());
            final WhoAmIResponse whoAmIRes = tc.execute(WhoAmIAction.INSTANCE, new WhoAmIRequest()).actionGet();
            sb.append(Strings.toString(whoAmIRes,true, true));
        } catch (Exception e1) {
            sb.append(ExceptionsHelper.stackTrace(e1));
        }
        
        try {
            sb.append("License:"+System.lineSeparator());
            LicenseInfoResponse res = tc.execute(LicenseInfoAction.INSTANCE, new LicenseInfoRequest()).actionGet();
            sb.append(Strings.toString(res,true, true));
        } catch (Exception e1) {
            sb.append(ExceptionsHelper.stackTrace(e1));
        }
        
        
        try {
            sb.append("ClusterHealthRequest:"+System.lineSeparator());
            ClusterHealthResponse nir = tc.admin().cluster().health(new ClusterHealthRequest()).actionGet();
            sb.append(Strings.toString(nir,true, true));
        } catch (Exception e1) {
            sb.append(ExceptionsHelper.stackTrace(e1));
        }
        
        try {
            sb.append(System.lineSeparator()+"NodesInfoResponse:"+System.lineSeparator());
            NodesInfoResponse nir = tc.admin().cluster().nodesInfo(new NodesInfoRequest()).actionGet();
            sb.append(Strings.toString(nir,true, true));
        } catch (Exception e1) {
            sb.append(ExceptionsHelper.stackTrace(e1));
        }
        
        try {
            sb.append(System.lineSeparator()+"NodesStatsRequest:"+System.lineSeparator());
            NodesStatsResponse nir = tc.admin().cluster().nodesStats(new NodesStatsRequest()).actionGet();
            sb.append(Strings.toString(nir,true, true));
        } catch (Exception e1) {
            sb.append(ExceptionsHelper.stackTrace(e1));
        }
        
        try {
            sb.append(System.lineSeparator()+"PendingClusterTasksRequest:"+System.lineSeparator());
            PendingClusterTasksResponse nir = tc.admin().cluster().pendingClusterTasks(new PendingClusterTasksRequest()).actionGet();
            sb.append(Strings.toString(nir,true, true));
        } catch (Exception e1) {
            sb.append(ExceptionsHelper.stackTrace(e1));
        }
        
        try {
            sb.append(System.lineSeparator()+"IndicesStatsRequest:"+System.lineSeparator());
            IndicesStatsResponse nir = tc.admin().indices().stats(new IndicesStatsRequest()).actionGet();
            sb.append(Strings.toString(nir, true, true));
        } catch (Exception e1) {
            sb.append(ExceptionsHelper.stackTrace(e1));
        }
        
        try {
            File dfile = new File("sgadmin_diag_trace_"+date+".txt");
            Files.asCharSink(dfile, StandardCharsets.UTF_8).write(sb);
            System.out.println("Diagnostic trace written to: "+dfile.getAbsolutePath());
        } catch (Exception e1) {
            System.out.println("ERR: cannot write diag trace file due to "+e1);
        }
    }
    
    private static void validate(CommandLine line) throws ParseException{
        
        if(line.hasOption("ts") && line.hasOption("cacert")) {
            System.out.println("WARN: It makes no sense to specify -ts as well as -cacert");
        }
        
        if(line.hasOption("ks") && line.hasOption("cert")) {
            System.out.println("WARN: It makes no sense to specify -ks as well as -cert");
        }
        
        if(line.hasOption("ks") && line.hasOption("key")) {
            System.out.println("WARN: It makes no sense to specify -ks as well as -key");
        }
        
        if(line.hasOption("cd") && line.hasOption("rl")) {
            System.out.println("WARN: It makes no sense to specify -cd as well as -r");
        }
        
        if(line.hasOption("cd") && line.hasOption("f")) {
            System.out.println("WARN: It makes no sense to specify -cd as well as -f");
        }
        
        if(line.hasOption("cn") && line.hasOption("icl")) {
            throw new ParseException("Only set one of -cn or -icl");
        }
        
        if(line.hasOption("vc") && !line.hasOption("cd") && !line.hasOption("f")) {
            throw new ParseException("Specify at least -cd or -f together with vc");
        }

        if(!line.hasOption("vc") && !line.hasOption("mo") && !line.hasOption("ks") 
                && !line.hasOption("cert")) {
            throw new ParseException("Specify at least -ks or -cert");
        }
        
        if(!line.hasOption("vc")  && !line.hasOption("mo") 
                && !line.hasOption("ts") && !line.hasOption("cacert")) {
            throw new ParseException("Specify at least -ts or -cacert");
        }
        
        //TODO add more validation rules
    }
    
    private static String promptForPassword(String passwordName, String commandLineOption, String envVarName) throws Exception {
        final Console console = System.console();
        if(console == null) {
            throw new Exception("Cannot allocate a console. Set env var "+envVarName+" or "+commandLineOption+" on commandline in that case");
        }
        return new String(console.readPassword("[%s]", passwordName+" password:"));
    }
    
    private static int issueWarnings(Client tc) {
        NodesInfoResponse nir = tc.admin().cluster().nodesInfo(new NodesInfoRequest()).actionGet();
        Version maxVersion = nir.getNodes().stream().max((n1,n2) -> n1.getVersion().compareTo(n2.getVersion())).get().getVersion();
        Version minVersion = nir.getNodes().stream().min((n1,n2) -> n1.getVersion().compareTo(n2.getVersion())).get().getVersion();
        
        if(!maxVersion.equals(minVersion)) {
            System.out.println("ERR: Your cluster consists of different node versions. It is not allowed to run sgadmin against a mixed cluster.");
            System.out.println("         Minimum node version is "+minVersion.toString());
            System.out.println("         Maximum node version is "+maxVersion.toString());
            if(!ALLOW_MIXED) {
                return -1;
            }
           
        } else {
            System.out.println("Elasticsearch Version: "+minVersion.toString());
        }
        
        if(nir.getNodes().size() > 0) {
            List<PluginInfo> pluginInfos = nir.getNodes().get(0).getInfo(PluginsAndModules.class).getPluginInfos();
            String sgVersion = pluginInfos.stream().filter(p->p.getClassname().equals("com.floragunn.searchguard.SearchGuardPlugin")).map(p->p.getVersion()).findFirst().orElse("<unknown>");
            System.out.println("Search Guard Version: "+sgVersion);
        }
        
        return 0;
    }
    
    private static int deleteConfigIndex(TransportClient tc, String index, boolean indexExists) {
        boolean success = true;
        
        if(indexExists) {
            success = tc.admin().indices().delete(new DeleteIndexRequest(index)).actionGet().isAcknowledged();
            System.out.print("Deleted index '"+index+"'");
        } else {
            System.out.print("No index '"+index+"' exists, so no need to delete it");
        }
        
        return (success?0:-1);
    }
    
    private static int createConfigIndex(TransportClient tc, String index, String explicitReplicas) {
        Map<String, Object> indexSettings = new HashMap<>();
        indexSettings.put("index.number_of_shards", 1);
        
        if(explicitReplicas != null) {
            if(explicitReplicas.contains("-")) {
                indexSettings.put("index.auto_expand_replicas", explicitReplicas);
            } else {
                indexSettings.put("index.number_of_replicas", Integer.parseInt(explicitReplicas));
            }
        } else {
            indexSettings.put("index.auto_expand_replicas", "0-all");
        }

        boolean indexCreated;
        
        if (CREATE_AS_LEGACY) {
            indexCreated = tc.admin().indices().create(new CreateIndexRequest(index).settings(indexSettings)).actionGet().isAcknowledged();
        } else {
            indexCreated = tc.admin().indices()
                    .create(new CreateIndexRequest(index).settings(indexSettings).mapping("_doc", ConfigurationRepository.SG_INDEX_MAPPING))
                    .actionGet().isAcknowledged();
        }
        
        if (indexCreated) {
            System.out.println("done ("+(explicitReplicas!=null?explicitReplicas:"0-all")+" replicas)");
            return 0;
        } else {
            System.out.println("failed!");
            System.out.println("FAIL: Unable to create the "+index+" index. See elasticsearch logs for more details");
            return (-1);
        }
    }
    
    private static int backup(TransportClient tc, String index, File backupDir, boolean legacy) {
        backupDir.mkdirs();
        
        boolean success = retrieveFile(tc, backupDir.getAbsolutePath()+"/sg_config.yml", index, "config", legacy);
        success = retrieveFile(tc, backupDir.getAbsolutePath()+"/sg_roles.yml", index, "roles", legacy) && success;
        
        success = retrieveFile(tc, backupDir.getAbsolutePath()+"/sg_roles_mapping.yml", index, "rolesmapping", legacy) && success;
        success = retrieveFile(tc, backupDir.getAbsolutePath()+"/sg_internal_users.yml", index, "internalusers", legacy) && success;
        success = retrieveFile(tc, backupDir.getAbsolutePath()+"/sg_action_groups.yml", index, "actiongroups", legacy) && success;

        if(!legacy) {
            success = retrieveFile(tc, backupDir.getAbsolutePath()+"/sg_tenants.yml", index, "tenants", legacy) && success;
            success = retrieveFile(tc, backupDir.getAbsolutePath()+"/sg_blocks.yml", index, "blocks", legacy) && success;
            success = retrieveFile(tc, backupDir.getAbsolutePath()+"/sg_frontend_authc.yml", index, "frontend_authc", legacy) && success;
        }
        
        return success?0:-1;
    }
    
    private static int upload(TransportClient tc, String index, String cd, boolean legacy, NodesInfoResponse nodesInfo, boolean resolveEnvVars) {
        
        boolean success = uploadFile(tc, cd+"sg_config.yml", index, "config", legacy, resolveEnvVars);
        success = uploadFile(tc, cd+"sg_roles.yml", index, "roles", legacy, resolveEnvVars) && success;
        success = uploadFile(tc, cd+"sg_roles_mapping.yml", index, "rolesmapping", legacy, resolveEnvVars) && success;
        
        success = uploadFile(tc, cd+"sg_internal_users.yml", index, "internalusers", legacy, resolveEnvVars) && success;
        success = uploadFile(tc, cd+"sg_action_groups.yml", index, "actiongroups", legacy, resolveEnvVars) && success;

        
        if(!legacy) {
            success = uploadFile(tc, cd+"sg_tenants.yml", index, "tenants", legacy, resolveEnvVars) && success;

            if (new File(cd + "sg_blocks.yml").exists()) {
                success = uploadFile(tc, cd + "sg_blocks.yml", index, "blocks", legacy, resolveEnvVars) && success;
            }

            if (new File(cd + "sg_frontend_authc.yml").exists()) {
                success = uploadFile(tc, cd + "sg_frontend_authc.yml", index, "frontend_authc", legacy, resolveEnvVars) && success;
            }
        }
        
        if(!success) {
            System.out.println("ERR: cannot upload configuration, see errors above");
            return -1;
        }
        
        ConfigUpdateResponse cur = tc.execute(ConfigUpdateAction.INSTANCE, new ConfigUpdateRequest(getTypes(legacy))).actionGet();

        success = checkConfigUpdateResponse(cur, nodesInfo, getTypes(legacy).length) && success;

        System.out.println("Done with "+(success?"success":"failures"));
        return (success?0:-1);
    }
    
    private static String readTypeFromFile(File file) throws IOException {
        if(!file.exists() || !file.isFile()) {
            System.out.println("ERR: No such file "+file.getAbsolutePath());
            return null;
        }
        final JsonNode jsonNode = DefaultObjectMapper.YAML_MAPPER.readTree(file);
        return new SgJsonNode(jsonNode).get("_sg_meta").get("type").asString();
    }

    private static String[] getTypes(boolean legacy) {
        if(legacy) {
            return new String[]{"config","roles","rolesmapping","internalusers","actiongroups"};
        }
        return CType.lcStringValues().toArray(new String[0]);
    }
}
