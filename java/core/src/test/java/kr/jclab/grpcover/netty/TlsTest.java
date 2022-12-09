/*
 * Copyright 2015 The gRPC Authors
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
 */

package kr.jclab.grpcover.netty;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.*;
import io.grpc.internal.testing.TestUtils;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.protobuf.SimpleRequest;
import io.grpc.testing.protobuf.SimpleResponse;
import io.grpc.testing.protobuf.SimpleServiceGrpc;
import io.netty.handler.ssl.*;
import kr.jclab.grpcover.netty.test.GrpcClientChannelInitializer;
import kr.jclab.grpcover.netty.test.TestHelper;
import kr.jclab.grpcover.netty.test.TestServer;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


/**
 * Integration tests for Netty's TLS support.
 */
@RunWith(JUnit4.class)
public class TlsTest {
  private final TestHelper testHelper = new TestHelper();
  @After
  public void cleanup() {
    testHelper.shutdown();
  }

  private ScheduledExecutorService executor;
  private Server server;
  private ManagedChannel channel;
  private SslContextBuilder clientContextBuilder;

  @Before
  public void setUp() throws NoSuchAlgorithmException {
    executor = Executors.newSingleThreadScheduledExecutor();
    clientContextBuilder = SslContextBuilder.forClient();
  }

  @After
  public void tearDown() {
    if (server != null) {
      server.shutdown();
    }
    if (channel != null) {
      channel.shutdown();
    }
    MoreExecutors.shutdownAndAwaitTermination(executor, 5, TimeUnit.SECONDS);
  }


  /**
   * Tests that a client and a server configured using GrpcSslContexts can successfully
   * communicate with each other.
   */
  @Test
  public void basicClientServerIntegrationTest() throws Exception {
    // Create & start a server.
    File serverCertFile = TestUtils.loadCert("server1.pem");
    File serverPrivateKeyFile = TestUtils.loadCert("server1.key");
    X509Certificate[] serverTrustedCaCerts = {
      TestUtils.loadX509Cert("ca.pem")
    };
    server = serverBuilder(0, serverCertFile, serverPrivateKeyFile, serverTrustedCaCerts)
        .addService(new SimpleServiceImpl())
        .build()
        .start();

    // Create a client.
    File clientCertChainFile = TestUtils.loadCert("client.pem");
    File clientPrivateKeyFile = TestUtils.loadCert("client.key");
    X509Certificate[] clientTrustedCaCerts = {
      TestUtils.loadX509Cert("ca.pem")
    };
    channel = clientChannel(server.getPort(), clientContextBuilder
        .keyManager(clientCertChainFile, clientPrivateKeyFile)
        .trustManager(clientTrustedCaCerts)
        .build());
    SimpleServiceGrpc.SimpleServiceBlockingStub client = SimpleServiceGrpc.newBlockingStub(channel);

    // Send an actual request, via the full GRPC & network stack, and check that a proper
    // response comes back.
    client.unaryRpc(SimpleRequest.getDefaultInstance());
  }

  /**
   * Tests that a server configured to require client authentication refuses to accept connections
   * from a client that has an untrusted certificate.
   */
  @Test
  @Ignore
  public void serverRejectsUntrustedClientCert() throws Exception {
    // Create & start a server. It requires client authentication and trusts only the test CA.
    File serverCertFile = TestUtils.loadCert("server1.pem");
    File serverPrivateKeyFile = TestUtils.loadCert("server1.key");
    X509Certificate[] serverTrustedCaCerts = {
      TestUtils.loadX509Cert("ca.pem")
    };
    server = serverBuilder(0, serverCertFile, serverPrivateKeyFile, serverTrustedCaCerts)
        .addService(new SimpleServiceImpl())
        .build()
        .start();

    // Create a client. Its credentials come from a CA that the server does not trust. The client
    // trusts both test CAs, so we can be sure that the handshake failure is due to the server
    // rejecting the client's cert, not the client rejecting the server's cert.
    File clientCertChainFile = TestUtils.loadCert("badclient.pem");
    File clientPrivateKeyFile = TestUtils.loadCert("badclient.key");
    X509Certificate[] clientTrustedCaCerts = {
      TestUtils.loadX509Cert("ca.pem")
    };
    channel = clientChannel(server.getPort(), clientContextBuilder
        .keyManager(clientCertChainFile, clientPrivateKeyFile)
        .trustManager(clientTrustedCaCerts)
        .build());
    SimpleServiceGrpc.SimpleServiceBlockingStub client = SimpleServiceGrpc.newBlockingStub(channel);

    // Check that the TLS handshake fails.
    try {
      client.unaryRpc(SimpleRequest.getDefaultInstance());
      fail("TLS handshake should have failed, but didn't; received RPC response");
    } catch (StatusRuntimeException e) {
      // GRPC reports this situation by throwing a StatusRuntimeException that wraps either a
      // javax.net.ssl.SSLHandshakeException or a java.nio.channels.ClosedChannelException.
      // Thus, reliably detecting the underlying cause is not feasible.
      assertEquals(
          Throwables.getStackTraceAsString(e),
          Status.Code.UNAVAILABLE, e.getStatus().getCode());
    }
    // We really want to see TRANSIENT_FAILURE here, but if the test runs slowly the 1s backoff
    // may be exceeded by the time the failure happens (since it counts from the start of the
    // attempt). Even so, CONNECTING is a strong indicator that the handshake failed; otherwise we'd
    // expect READY or IDLE.
    assertThat(channel.getState(false))
        .isAnyOf(ConnectivityState.TRANSIENT_FAILURE, ConnectivityState.CONNECTING);
  }


  /**
   * Tests that a server configured to require client authentication actually does require client
   * authentication.
   */
  @Test
  @Ignore
  public void noClientAuthFailure() throws Exception {
    // Create & start a server.
    File serverCertFile = TestUtils.loadCert("server1.pem");
    File serverPrivateKeyFile = TestUtils.loadCert("server1.key");
    X509Certificate[] serverTrustedCaCerts = {
      TestUtils.loadX509Cert("ca.pem")
    };
    server = serverBuilder(0, serverCertFile, serverPrivateKeyFile, serverTrustedCaCerts)
        .addService(new SimpleServiceImpl())
        .build()
        .start();

    // Create a client. It has no credentials.
    X509Certificate[] clientTrustedCaCerts = {
      TestUtils.loadX509Cert("ca.pem")
    };
    channel = clientChannel(server.getPort(), clientContextBuilder
        .trustManager(clientTrustedCaCerts)
        .build());
    SimpleServiceGrpc.SimpleServiceBlockingStub client = SimpleServiceGrpc.newBlockingStub(channel);

    // Check that the TLS handshake fails.
    try {
      client.unaryRpc(SimpleRequest.getDefaultInstance());
      fail("TLS handshake should have failed, but didn't; received RPC response");
    } catch (StatusRuntimeException e) {
      // GRPC reports this situation by throwing a StatusRuntimeException that wraps either a
      // javax.net.ssl.SSLHandshakeException or a java.nio.channels.ClosedChannelException.
      // Thus, reliably detecting the underlying cause is not feasible.
      assertEquals(
          Throwables.getStackTraceAsString(e),
          Status.Code.UNAVAILABLE, e.getStatus().getCode());
    }
    // We really want to see TRANSIENT_FAILURE here, but if the test runs slowly the 1s backoff
    // may be exceeded by the time the failure happens (since it counts from the start of the
    // attempt). Even so, CONNECTING is a strong indicator that the handshake failed; otherwise we'd
    assertThat(channel.getState(false))
        .isAnyOf(ConnectivityState.TRANSIENT_FAILURE, ConnectivityState.CONNECTING);
  }


  /**
   * Tests that a client configured using GrpcSslContexts refuses to talk to a server that has an
   * an untrusted certificate.
   */
  @Test
  public void clientRejectsUntrustedServerCert() throws Exception {
    // Create & start a server.
    File serverCertFile = TestUtils.loadCert("badserver.pem");
    File serverPrivateKeyFile = TestUtils.loadCert("badserver.key");
    X509Certificate[] serverTrustedCaCerts = {
      TestUtils.loadX509Cert("ca.pem")
    };
    server = serverBuilder(0, serverCertFile, serverPrivateKeyFile, serverTrustedCaCerts)
        .addService(new SimpleServiceImpl())
        .build()
        .start();

    // Create a client.
    File clientCertChainFile = TestUtils.loadCert("client.pem");
    File clientPrivateKeyFile = TestUtils.loadCert("client.key");
    X509Certificate[] clientTrustedCaCerts = {
      TestUtils.loadX509Cert("ca.pem")
    };
    channel = clientChannel(server.getPort(), clientContextBuilder
        .keyManager(clientCertChainFile, clientPrivateKeyFile)
        .trustManager(clientTrustedCaCerts)
        .build());
    SimpleServiceGrpc.SimpleServiceBlockingStub client = SimpleServiceGrpc.newBlockingStub(channel);

    // Check that the TLS handshake fails.
    try {
      client.unaryRpc(SimpleRequest.getDefaultInstance());
      fail("TLS handshake should have failed, but didn't; received RPC response");
    } catch (StatusRuntimeException e) {
      // GRPC reports this situation by throwing a StatusRuntimeException that wraps either a
      // javax.net.ssl.SSLHandshakeException or a java.nio.channels.ClosedChannelException.
      // Thus, reliably detecting the underlying cause is not feasible.
      // TODO(carl-mastrangelo): eventually replace this with a hamcrest matcher.
      assertEquals(
          Throwables.getStackTraceAsString(e),
          Status.Code.UNAVAILABLE, e.getStatus().getCode());
    }
    // We really want to see TRANSIENT_FAILURE here, but if the test runs slowly the 1s backoff
    // may be exceeded by the time the failure happens (since it counts from the start of the
    // attempt). Even so, CONNECTING is a strong indicator that the handshake failed; otherwise we'd
    assertThat(channel.getState(false))
        .isAnyOf(ConnectivityState.TRANSIENT_FAILURE, ConnectivityState.CONNECTING);
  }


  private ServerBuilder<?> serverBuilder(int port, File serverCertChainFile,
      File serverPrivateKeyFile, X509Certificate[] serverTrustedCaCerts) throws IOException {
    SslContextBuilder sslContextBuilder
        = SslContextBuilder.forServer(serverCertChainFile, serverPrivateKeyFile);
//    if (sslProvider == SslProvider.JDK) {
//      GrpcSslContexts.configure(sslContextBuilder, jdkProvider);
//    } else {
//      GrpcSslContexts.configure(sslContextBuilder, sslProvider);
//    }
    sslContextBuilder.trustManager(serverTrustedCaCerts)
        .clientAuth(ClientAuth.REQUIRE);

    TestServer testServer = TestServer.newServer(testHelper, 0, sslContextBuilder.build());
    return NettyServerBuilder
            .forInitializer(testHelper.worker, testServer.serverChannel, testServer.initializer);
  }


  private ManagedChannel clientChannel(int port, SslContext sslContext) throws IOException {
    return NettyChannelBuilder
            .forAddress(testHelper.clientBootstrapFactory, new InetSocketAddress("127.0.0.1", port))
            .overrideAuthority(TestUtils.TEST_SERVER_HOST)
            .channelAttribute(GrpcClientChannelInitializer.SSL_CONTEXT_ATTR, sslContext)
            .build();
  }

  private static class SimpleServiceImpl extends SimpleServiceGrpc.SimpleServiceImplBase {
    @Override
    public void unaryRpc(SimpleRequest req, StreamObserver<SimpleResponse> respOb) {
      respOb.onNext(SimpleResponse.getDefaultInstance());
      respOb.onCompleted();
    }
  }
}
