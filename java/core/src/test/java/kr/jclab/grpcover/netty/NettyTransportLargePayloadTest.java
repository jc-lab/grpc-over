/*
 * Copyright 2016 The gRPC Authors
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

import io.grpc.*;
import io.grpc.internal.*;
import io.grpc.stub.StreamObserver;
import kr.jclab.grpcover.testing.protobuf.SimpleRequest;
import kr.jclab.grpcover.testing.protobuf.SimpleResponse;
import kr.jclab.grpcover.testing.protobuf.SimpleServiceGrpc;
import kr.jclab.grpcover.netty.test.TestHelper;
import kr.jclab.grpcover.netty.test.TestServer;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Base64;
import java.util.Random;

/** Unit tests for Netty transport. */
@RunWith(JUnit4.class)
public class NettyTransportLargePayloadTest {
  private final TestHelper testHelper = new TestHelper();
  @After
  public void cleanup() {
    testHelper.shutdown();
  }

  private final FakeClock fakeClock = new FakeClock();
  // Avoid LocalChannel for testing because LocalChannel can fail with
  // io.netty.channel.ChannelException instead of java.net.ConnectException which breaks
  // serverNotListening test.
  private final ClientTransportFactory clientFactory = NettyChannelBuilder
          .forTarget(testHelper.clientBootstrapFactory, "http://127.0.0.1")
      // Although specified here, address is ignored because we never call build.
      .flowControlWindow(AbstractTransportTest.TEST_FLOW_CONTROL_WINDOW)
//      .negotiationType(NegotiationType.PLAINTEXT)
      .buildTransportFactory();

  private final Random random = new Random();

  @After
  public void releaseClientFactory() {
    clientFactory.close();
  }

  protected Server newServer() throws IOException {
    TestServer testServer = TestServer.newServer(testHelper, 0);
    return NettyServerBuilder
            .forInitializer(testHelper.worker, testServer.serverChannel, testServer.initializer)
            .flowControlWindow(AbstractTransportTest.TEST_FLOW_CONTROL_WINDOW)
            .addService(new SimpleServiceImpl())
            .build()
            .start();
  }

  protected ManagedChannel clientChannel(Server server) {
    return NettyChannelBuilder
            .forAddress(testHelper.clientBootstrapFactory, server.getListenSockets().get(0))
            .overrideAuthority(testAuthority(server))
            .build();
  }

  @Test
  public void largePayloadTest() throws Exception {
    Server server = newServer();
    ManagedChannel managedChannel = clientChannel(server);

    SimpleServiceGrpc.SimpleServiceBlockingStub client = SimpleServiceGrpc.newBlockingStub(managedChannel);
    SimpleResponse response = client.unaryRpc(
            SimpleRequest.newBuilder()
                    .setRequestMessage(makeText(1048576))
                    .build()
    );
    System.out.println("response: " + response.getResponseMessage().length());
  }

  protected String makeText(int length) {
    int binarySize = (length / 4 + 1) * 3;
    byte[] binaryData = new byte[binarySize];
    random.nextBytes(binaryData);
    return Base64.getEncoder().encodeToString(binaryData).substring(0, length);
  }

  protected String testAuthority(Server server) {
    return "localhost:" + ((InetSocketAddress)server.getListenSockets().get(0)).getPort();
  }

  private static class SimpleServiceImpl extends SimpleServiceGrpc.SimpleServiceImplBase {
    @Override
    public void unaryRpc(SimpleRequest req, StreamObserver<SimpleResponse> respOb) {
      System.out.println("unaryRpc : " + req.getRequestMessage().length());
      respOb.onNext(SimpleResponse.newBuilder().setResponseMessage("OK").build());
      respOb.onCompleted();
    }
  }

  protected final ChannelLogger transportLogger() {
    return new ChannelLogger() {
      @Override
      public void log(ChannelLogLevel level, String message) {}

      @Override
      public void log(ChannelLogLevel level, String messageFormat, Object... args) {}
    };
  }
}
