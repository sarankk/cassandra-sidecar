/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.sidecar.acl;

import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.extension.ExtendWith;

import com.datastax.driver.core.Session;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.config.SslConfiguration;
import org.apache.cassandra.sidecar.config.yaml.KeyStoreConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SslConfigurationImpl;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;

import static org.apache.cassandra.sidecar.testing.IntegrationTestModule.ADMIN_IDENTITY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Test for role based access control in Sidecar
 */
@ExtendWith(VertxExtension.class)
public class RoleBasedAuthorizationIntegrationTest extends IntegrationTestBase
{
    private static final int MIN_VERSION_WITH_MTLS = 5;

    @CassandraIntegrationTest(buildCluster = false)
    void testForAdmin(VertxTestContext context, ConfigurableCassandraTestContext cassandraContext) throws Exception
    {
        // starts cluster for 5.0 and above version test
        startClusterWithMtlsAndAuthorizer(cassandraContext);

        createKeyspace("sample_keyspace");
        sidecarTestContext.setSslConfiguration(sslConfigWithKeystoreTruststore());
        Thread.sleep(2000);
        String testRoute = String.format("/api/v1/keyspaces/%s/schema", "sample_keyspace");
        verifyAccess(context, testRoute, clientKeystorePath);
    }

    @CassandraIntegrationTest(buildCluster = false)
    void testForSuperUser(VertxTestContext context, ConfigurableCassandraTestContext cassandraContext) throws Exception
    {
        // starts cluster for 5.0 and above version test
        startClusterWithMtlsAndAuthorizer(cassandraContext);

        createRole("test_role", true);
        createKeyspace("sample_keyspace");
        insertIdentityRole("spiffe://cassandra/sidecar/test_user", "test_role");
        sidecarTestContext.setSslConfiguration(sslConfigWithKeystoreTruststore());
        Thread.sleep(2000);
        String testRoute = String.format("/api/v1/keyspaces/%s/schema", "sample_keyspace");
        Path clientKeystorePath = clientKeystorePath("spiffe://cassandra/sidecar/test_user");
        verifyAccess(context, testRoute, clientKeystorePath);
    }

    @CassandraIntegrationTest(buildCluster = false)
    void testForNonAdmin(VertxTestContext context, ConfigurableCassandraTestContext cassandraContext) throws Exception
    {
        // starts cluster for 5.0 and above version test
        startClusterWithMtlsAndAuthorizer(cassandraContext);

        createRole("test_role", false);
        createKeyspace("sample_keyspace");
        insertIdentityRole("spiffe://cassandra/sidecar/test_user", "test_role");
        grantPermission("sample_keyspace", "test_role");
        sidecarTestContext.setSslConfiguration(sslConfigWithKeystoreTruststore());
        Thread.sleep(2000);
        String testRoute = String.format("/api/v1/keyspaces/%s/schema", "sample_keyspace");
        Path clientKeystorePath = clientKeystorePath("spiffe://cassandra/sidecar/test_user");
        verifyAccess(context, testRoute, clientKeystorePath);
    }

    @CassandraIntegrationTest(buildCluster = false)
    void testGrantingForTable(VertxTestContext context, ConfigurableCassandraTestContext cassandraContext) throws Exception
    {
        // starts cluster for 5.0 and above version test
        startClusterWithMtlsAndAuthorizer(cassandraContext);

        createRole("test_role", false);
        createKeyspace("sample_keyspace");
        insertIdentityRole("spiffe://cassandra/sidecar/test_user", "test_role");
        grantPermission("sample_keyspace", "test_role");
        sidecarTestContext.setSslConfiguration(sslConfigWithKeystoreTruststore());
        Thread.sleep(2000);
        String testRoute = String.format("/api/v1/keyspaces/%s/tables/%s/create-restore-jobs", "sample_keyspace");
        Path clientKeystorePath = clientKeystorePath("spiffe://cassandra/sidecar/test_user");
        verifyAccess(context, testRoute, clientKeystorePath);
    }

    @CassandraIntegrationTest(buildCluster = false)
    void testEndpointWithOrAuthorization(VertxTestContext context, ConfigurableCassandraTestContext cassandraContext) throws Exception
    {
        // starts cluster for 5.0 and above version test
        startClusterWithMtlsAndAuthorizer(cassandraContext);

        createRole("test_role", false);
        createKeyspace("sample_keyspace");
        insertIdentityRole("spiffe://cassandra/sidecar/test_user", "test_role");
        sidecarTestContext.setSslConfiguration(sslConfigWithKeystoreTruststore());
        Thread.sleep(2000);
        String testRoute = String.format("/api/v1/keyspaces/%s/schema", "sample_keyspace");
        Path clientKeystorePath = clientKeystorePath("spiffe://cassandra/sidecar/test_user");

        // permission for test_role on sample_keyspace not granted, falls back to sidecar permission VIEW:* added
        verifyAccess(context, testRoute, clientKeystorePath);
    }

    @CassandraIntegrationTest(buildCluster = false)
    void testAllowingWildcardAction(VertxTestContext context, ConfigurableCassandraTestContext cassandraContext) throws Exception
    {
        // starts cluster for 5.0 and above version test
        startClusterWithMtlsAndAuthorizer(cassandraContext);

        createRole("test_role", false);
        insertIdentityRole("spiffe://cassandra/sidecar/test_user", "test_role");
        sidecarTestContext.setSslConfiguration(sslConfigWithKeystoreTruststore());
        Thread.sleep(2000);

        String testRoute = "/api/v1/time-skew";
        Path clientKeystorePath = clientKeystorePath("spiffe://cassandra/sidecar/test_user");

        // Uses sidecar permission VIEW:* added
        verifyAccess(context, testRoute, clientKeystorePath);
    }

    @CassandraIntegrationTest(buildCluster = false)
    void testResourceWideActions(VertxTestContext context, ConfigurableCassandraTestContext cassandraContext) throws Exception
    {
        // starts cluster for 5.0 and above version test
        startClusterWithMtlsAndAuthorizer(cassandraContext);

        createRole("test_role", false);
        createKeyspace("test_keyspace");
        insertIdentityRole("spiffe://cassandra/sidecar/test_user", "test_role");
        sidecarTestContext.setSslConfiguration(sslConfigWithKeystoreTruststore());
        Thread.sleep(2000);

        String testRoute = String.format("/api/v1/keyspaces/%s/schema", "test_keyspace");
        Path clientKeystorePath = clientKeystorePath("spiffe://cassandra/sidecar/test_user");

        // Uses sidecar permission *:* added across resource data/test_keyspace
        verifyAccess(context, testRoute, clientKeystorePath);
    }

    private void startClusterWithMtlsAndAuthorizer(ConfigurableCassandraTestContext  cassandraContext)
    {
        // mTLS authentication was added in Cassandra starting 5.0 version
        assumeThat(cassandraContext.version.major)
        .withFailMessage("mTLS authentication is not supported in 4.0 Cassandra version")
        .isGreaterThanOrEqualTo(MIN_VERSION_WITH_MTLS);

        cassandraContext.configureAndStartCluster(builder -> {
            builder.appendConfig(config -> config.set("authenticator.class_name", "org.apache.cassandra.auth.MutualTlsWithPasswordFallbackAuthenticator")
                                                 .set("authenticator.parameters",
                                                      Collections.singletonMap("validator_class_name", "org.apache.cassandra.auth.SpiffeCertificateValidator"))
                                                 .set("role_manager", "CassandraRoleManager")
                                                 .set("authorizer", "CassandraAuthorizer")
                                                 .set("client_encryption_options.enabled", "true")
                                                 .set("client_encryption_options.optional", "true")
                                                 .set("client_encryption_options.require_client_auth", "true")
                                                 .set("client_encryption_options.require_endpoint_verification", "false")
                                                 .set("client_encryption_options.keystore", serverKeystorePath.toAbsolutePath().toString())
                                                 .set("client_encryption_options.keystore_password", serverKeystorePassword)
                                                 .set("client_encryption_options.truststore", truststorePath.toAbsolutePath().toString())
                                                 .set("client_encryption_options.truststore_password", truststorePassword));
        });
        waitForSchemaReady(30, TimeUnit.SECONDS);

        // required for authentication of sidecar requests to Cassandra
        insertIdentityRole(ADMIN_IDENTITY, "cassandra");
    }

    private void createRole(String role, boolean superUser)
    {
        Session session = maybeGetSession();
        session.execute("CREATE ROLE " + role + " WITH PASSWORD = 'password' AND SUPERUSER = " + superUser + " AND LOGIN = true;");
    }

    private void insertIdentityRole(String identity, String role)
    {
        Session session = maybeGetSession();
        session.execute("INSERT INTO system_auth.identity_to_role (identity, role) VALUES (\'" + identity + "\',\'" + role + "\');");
    }

    private void createKeyspace(String keyspace)
    {
        Session session = maybeGetSession();
        session.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspace + " WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':'3'}");
    }

    private void grantPermission(String keyspace, String role)
    {
        Session session = maybeGetSession();
        session.execute("GRANT ALL PERMISSIONS ON KEYSPACE " + keyspace + " TO " + role);
    }

    private SslConfiguration sslConfigWithKeystoreTruststore() throws Exception
    {
        return SslConfigurationImpl.builder()
                                   .enabled(true)
                                   .keystore(new KeyStoreConfigurationImpl(clientKeystorePath.toAbsolutePath().toString(), clientKeystorePassword, "PKCS12"))
                                   .truststore(new KeyStoreConfigurationImpl(truststorePath.toAbsolutePath().toString(), truststorePassword, "PKCS12"))
                                   .build();
    }

    private void verifyAccess(VertxTestContext context, String testRoute, Path clientKeystorePath)
    {
        WebClient client = createClient(clientKeystorePath, truststorePath);
        client.get(server.actualPort(), "127.0.0.1", testRoute)
              .send(context.succeeding(response -> {
                  context.verify(() -> {
                      assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                  });
                  context.completeNow();
              }));
    }
}
