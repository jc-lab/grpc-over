/*
 * Copyright 2019 The gRPC Authors
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

import io.grpc.ChannelLogger;
import kr.jclab.grpcover.netty.ProtocolNegotiators.ClientTlsHandler;
import kr.jclab.grpcover.netty.ProtocolNegotiators.GrpcNegotiationHandler;
import kr.jclab.grpcover.netty.ProtocolNegotiators.WaitUntilActiveHandler;
import io.netty.channel.ChannelHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.util.AsciiString;

/**
 * Internal accessor for {@link ProtocolNegotiators}.
 */
public final class InternalProtocolNegotiators {

  private InternalProtocolNegotiators() {}

  /**
   * Returns a {@link ProtocolNegotiator} that ensures the pipeline is set up so that TLS will
   * be negotiated, the {@code handler} is added and writes to the {@link io.netty.channel.Channel}
   * may happen immediately, even before the TLS Handshake is complete.
   */
  public static InternalProtocolNegotiator.ProtocolNegotiator tls(SslContext sslContext) {
    final kr.jclab.grpcover.netty.ProtocolNegotiator negotiator = ProtocolNegotiators.tls(sslContext);
    final class TlsNegotiator implements InternalProtocolNegotiator.ProtocolNegotiator {

      @Override
      public AsciiString scheme() {
        return negotiator.scheme();
      }

      @Override
      public ChannelHandler newHandler(GrpcHttp2ConnectionHandler grpcHandler) {
        return negotiator.newHandler(grpcHandler);
      }

      @Override
      public void close() {
        negotiator.close();
      }
    }
    
    return new TlsNegotiator();
  }

  /**
   * Returns a {@link ProtocolNegotiator} that ensures the pipeline is set up so that TLS will be
   * negotiated, the server TLS {@code handler} is added and writes to the {@link
   * io.netty.channel.Channel} may happen immediately, even before the TLS Handshake is complete.
   */
  public static InternalProtocolNegotiator.ProtocolNegotiator serverTls(SslContext sslContext) {
    final kr.jclab.grpcover.netty.ProtocolNegotiator negotiator = ProtocolNegotiators.serverTls(sslContext);
    final class ServerTlsNegotiator implements InternalProtocolNegotiator.ProtocolNegotiator {

      @Override
      public AsciiString scheme() {
        return negotiator.scheme();
      }

      @Override
      public ChannelHandler newHandler(GrpcHttp2ConnectionHandler grpcHandler) {
        return negotiator.newHandler(grpcHandler);
      }

      @Override
      public void close() {
        negotiator.close();
      }
    }

    return new ServerTlsNegotiator();
  }

  /** Returns a {@link ProtocolNegotiator} for plaintext client channel. */
  public static InternalProtocolNegotiator.ProtocolNegotiator plaintext() {
    final kr.jclab.grpcover.netty.ProtocolNegotiator negotiator = ProtocolNegotiators.plaintext();
    final class PlaintextNegotiator implements InternalProtocolNegotiator.ProtocolNegotiator {

      @Override
      public AsciiString scheme() {
        return negotiator.scheme();
      }

      @Override
      public ChannelHandler newHandler(GrpcHttp2ConnectionHandler grpcHandler) {
        return negotiator.newHandler(grpcHandler);
      }

      @Override
      public void close() {
        negotiator.close();
      }
    }

    return new PlaintextNegotiator();
  }

  /** Returns a {@link ProtocolNegotiator} for plaintext server channel. */
  public static InternalProtocolNegotiator.ProtocolNegotiator serverPlaintext() {
    final kr.jclab.grpcover.netty.ProtocolNegotiator negotiator = ProtocolNegotiators.serverPlaintext();
    final class ServerPlaintextNegotiator implements InternalProtocolNegotiator.ProtocolNegotiator {

      @Override
      public AsciiString scheme() {
        return negotiator.scheme();
      }

      @Override
      public ChannelHandler newHandler(GrpcHttp2ConnectionHandler grpcHandler) {
        return negotiator.newHandler(grpcHandler);
      }

      @Override
      public void close() {
        negotiator.close();
      }
    }

    return new ServerPlaintextNegotiator();
  }

  /**
   * Internal version of {@link WaitUntilActiveHandler}.
   */
  public static ChannelHandler waitUntilActiveHandler(ChannelHandler next,
      ChannelLogger negotiationLogger) {
    return new WaitUntilActiveHandler(next, negotiationLogger);
  }

  /**
   * Internal version of {@link GrpcNegotiationHandler}.
   */
  public static ChannelHandler grpcNegotiationHandler(GrpcHttp2ConnectionHandler next) {
    return new GrpcNegotiationHandler(next);
  }

  public static ChannelHandler clientTlsHandler(
      ChannelHandler next, SslContext sslContext, String authority,
      ChannelLogger negotiationLogger) {
    return new ClientTlsHandler(next, sslContext, authority, null, negotiationLogger);
  }

  public static class ProtocolNegotiationHandler
      extends ProtocolNegotiators.ProtocolNegotiationHandler {

    protected ProtocolNegotiationHandler(ChannelHandler next, String negotiatorName,
        ChannelLogger negotiationLogger) {
      super(next, negotiatorName, negotiationLogger);
    }

    protected ProtocolNegotiationHandler(ChannelHandler next, ChannelLogger negotiationLogger) {
      super(next, negotiationLogger);
    }
  }
}
