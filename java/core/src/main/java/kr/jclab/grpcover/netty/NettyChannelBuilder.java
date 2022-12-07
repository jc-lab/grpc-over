/*
 * Copyright 2014 The gRPC Authors
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

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.InlineMe;
import io.grpc.*;
import io.grpc.internal.*;
import io.grpc.internal.ManagedChannelImplBuilder.ChannelBuilderDefaultPortProvider;
import io.grpc.internal.ManagedChannelImplBuilder.ClientTransportFactoryBuilder;
import io.netty.channel.*;
import io.netty.util.AttributeKey;
import kr.jclab.grpcover.portable.NettyClientBootstrapFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.*;
import static io.grpc.internal.GrpcUtil.DEFAULT_KEEPALIVE_TIMEOUT_NANOS;
import static io.grpc.internal.GrpcUtil.KEEPALIVE_TIME_NANOS_DISABLED;

/**
 * A builder to help simplify construction of channels using the Netty transport.
 */
@ExperimentalApi("https://github.com/grpc/grpc-java/issues/1784")
@CheckReturnValue
public final class NettyChannelBuilder extends
    AbstractManagedChannelImplBuilder<NettyChannelBuilder> {
  public static AttributeKey<ChannelHandler> GRPC_CHANNEL_HANDLER = AttributeKey.valueOf("GRPC_CHANNEL_HANDLER");


  // 1MiB.
  public static final int DEFAULT_FLOW_CONTROL_WINDOW = 1024 * 1024;
  private static final boolean DEFAULT_AUTO_FLOW_CONTROL;

  private static final long AS_LARGE_AS_INFINITE = TimeUnit.DAYS.toNanos(1000L);


  static {
    String autoFlowControl = System.getenv("GRPC_EXPERIMENTAL_AUTOFLOWCONTROL");
    if (autoFlowControl == null) {
      autoFlowControl = "true";
    }
    DEFAULT_AUTO_FLOW_CONTROL = Boolean.parseBoolean(autoFlowControl);
  }

  private final ManagedChannelImplBuilder managedChannelImplBuilder;
  private TransportTracer.Factory transportTracerFactory = TransportTracer.getDefaultFactory();
  private NettyClientBootstrapFactory clientBootstrapFactory;
  private boolean autoFlowControl = DEFAULT_AUTO_FLOW_CONTROL;
  private int flowControlWindow = DEFAULT_FLOW_CONTROL_WINDOW;
  private int maxHeaderListSize = GrpcUtil.DEFAULT_MAX_HEADER_LIST_SIZE;
  private long keepAliveTimeNanos = KEEPALIVE_TIME_NANOS_DISABLED;
  private long keepAliveTimeoutNanos = DEFAULT_KEEPALIVE_TIMEOUT_NANOS;
  private boolean keepAliveWithoutCalls;

  public static NettyChannelBuilder forTarget(
          NettyClientBootstrapFactory clientBootstrapFactory,
          String target,
          ChannelCredentials channelCreds,
          CallCredentials callCreds
  ) {
    return new NettyChannelBuilder(clientBootstrapFactory, target, channelCreds, callCreds);
  }

  public static NettyChannelBuilder forTarget(
          NettyClientBootstrapFactory clientBootstrapFactory,
          String target
  ) {
    return new NettyChannelBuilder(clientBootstrapFactory, target, null, null);
  }

  public static NettyChannelBuilder forAddress(
          NettyClientBootstrapFactory clientBootstrapFactory,
          SocketAddress directServerAddress,
          ChannelCredentials channelCreds,
          CallCredentials callCreds
  ) {
    return new NettyChannelBuilder(clientBootstrapFactory, directServerAddress, channelCreds, callCreds);
  }

  public static NettyChannelBuilder forAddress(
          NettyClientBootstrapFactory clientBootstrapFactory,
          SocketAddress directServerAddress
  ) {
    return new NettyChannelBuilder(clientBootstrapFactory, directServerAddress, null, null);
  }

  private final class NettyChannelTransportFactoryBuilder implements ClientTransportFactoryBuilder {
    @Override
    public ClientTransportFactory buildClientTransportFactory() {
      return buildTransportFactory();
    }
  }

  private final class NettyChannelDefaultPortProvider implements ChannelBuilderDefaultPortProvider {
    @Override
    public int getDefaultPort() {
      return 0;
    }
  }

  NettyChannelBuilder(
          NettyClientBootstrapFactory clientBootstrapFactory,
          String target, ChannelCredentials channelCreds, CallCredentials callCreds) {
    managedChannelImplBuilder = new ManagedChannelImplBuilder(target,
            channelCreds, callCreds,
            new NettyChannelTransportFactoryBuilder(),
            new NettyChannelDefaultPortProvider());
    this.clientBootstrapFactory = checkNotNull(clientBootstrapFactory, "clientBootstrapFactory");
  }

  NettyChannelBuilder(
          NettyClientBootstrapFactory clientBootstrapFactory,
          SocketAddress address, ChannelCredentials channelCreds, CallCredentials callCreds) {
    managedChannelImplBuilder = new ManagedChannelImplBuilder(address,
            getAuthorityFromAddress(address),
            channelCreds, callCreds,
            new NettyChannelTransportFactoryBuilder(),
            new NettyChannelDefaultPortProvider());
    this.clientBootstrapFactory = checkNotNull(clientBootstrapFactory, "clientBootstrapFactory");
  }

  @Internal
  @Override
  protected ManagedChannelBuilder<?> delegate() {
    return managedChannelImplBuilder;
  }

  private static String getAuthorityFromAddress(SocketAddress address) {
    if (address instanceof InetSocketAddress) {
      InetSocketAddress inetAddress = (InetSocketAddress) address;
      return GrpcUtil.authorityFromHostAndPort(inetAddress.getHostString(), inetAddress.getPort());
    } else {
      return address.toString();
    }
  }

  /**
   * Sets the initial flow control window in bytes. Setting initial flow control window enables auto
   * flow control tuning using bandwidth-delay product algorithm. To disable auto flow control
   * tuning, use {@link #flowControlWindow(int)}. By default, auto flow control is enabled with
   * initial flow control window size of {@link #DEFAULT_FLOW_CONTROL_WINDOW}.
   */
  @CanIgnoreReturnValue
  public NettyChannelBuilder initialFlowControlWindow(int initialFlowControlWindow) {
    checkArgument(initialFlowControlWindow > 0, "initialFlowControlWindow must be positive");
    this.flowControlWindow = initialFlowControlWindow;
    this.autoFlowControl = true;
    return this;
  }

  /**
   * Sets the flow control window in bytes. Setting flowControlWindow disables auto flow control
   * tuning; use {@link #initialFlowControlWindow(int)} to enable auto flow control tuning. If not
   * called, the default value is {@link #DEFAULT_FLOW_CONTROL_WINDOW}) with auto flow control
   * tuning.
   */
  @CanIgnoreReturnValue
  public NettyChannelBuilder flowControlWindow(int flowControlWindow) {
    checkArgument(flowControlWindow > 0, "flowControlWindow must be positive");
    this.flowControlWindow = flowControlWindow;
    this.autoFlowControl = false;
    return this;
  }

  /**
   * Sets the maximum size of header list allowed to be received. This is cumulative size of the
   * headers with some overhead, as defined for
   * <a href="http://httpwg.org/specs/rfc7540.html#rfc.section.6.5.2">
   * HTTP/2's SETTINGS_MAX_HEADER_LIST_SIZE</a>. The default is 8 KiB.
   *
   * @deprecated Use {@link #maxInboundMetadataSize} instead
   */
  @CanIgnoreReturnValue
  @Deprecated
  @InlineMe(replacement = "this.maxInboundMetadataSize(maxHeaderListSize)")
  public NettyChannelBuilder maxHeaderListSize(int maxHeaderListSize) {
    return maxInboundMetadataSize(maxHeaderListSize);
  }

  /**
   * Sets the maximum size of metadata allowed to be received. This is cumulative size of the
   * entries with some overhead, as defined for
   * <a href="http://httpwg.org/specs/rfc7540.html#rfc.section.6.5.2">
   * HTTP/2's SETTINGS_MAX_HEADER_LIST_SIZE</a>. The default is 8 KiB.
   *
   * @param bytes the maximum size of received metadata
   * @return this
   * @throws IllegalArgumentException if bytes is non-positive
   * @since 1.17.0
   */
  @CanIgnoreReturnValue
  @Override
  public NettyChannelBuilder maxInboundMetadataSize(int bytes) {
    checkArgument(bytes > 0, "maxInboundMetadataSize must be > 0");
    this.maxHeaderListSize = bytes;
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @since 1.3.0
   */
  @CanIgnoreReturnValue
  @Override
  public NettyChannelBuilder keepAliveTime(long keepAliveTime, TimeUnit timeUnit) {
    checkArgument(keepAliveTime > 0L, "keepalive time must be positive");
    keepAliveTimeNanos = timeUnit.toNanos(keepAliveTime);
    keepAliveTimeNanos = KeepAliveManager.clampKeepAliveTimeInNanos(keepAliveTimeNanos);
    if (keepAliveTimeNanos >= AS_LARGE_AS_INFINITE) {
      // Bump keepalive time to infinite. This disables keepalive.
      keepAliveTimeNanos = KEEPALIVE_TIME_NANOS_DISABLED;
    }
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @since 1.3.0
   */
  @CanIgnoreReturnValue
  @Override
  public NettyChannelBuilder keepAliveTimeout(long keepAliveTimeout, TimeUnit timeUnit) {
    checkArgument(keepAliveTimeout > 0L, "keepalive timeout must be positive");
    keepAliveTimeoutNanos = timeUnit.toNanos(keepAliveTimeout);
    keepAliveTimeoutNanos = KeepAliveManager.clampKeepAliveTimeoutInNanos(keepAliveTimeoutNanos);
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @since 1.3.0
   */
  @CanIgnoreReturnValue
  @Override
  public NettyChannelBuilder keepAliveWithoutCalls(boolean enable) {
    keepAliveWithoutCalls = enable;
    return this;
  }

  /**
   * Sets the maximum message size allowed for a single gRPC frame. If an inbound messages larger
   * than this limit is received it will not be processed and the RPC will fail with
   * RESOURCE_EXHAUSTED.
   */
  @CanIgnoreReturnValue
  @Override
  public NettyChannelBuilder maxInboundMessageSize(int max) {
    checkArgument(max >= 0, "negative max");
    maxInboundMessageSize = max;
    return this;
  }

  ClientTransportFactory buildTransportFactory() {
    return new NettyTransportFactory(
        clientBootstrapFactory,
        autoFlowControl, flowControlWindow, maxInboundMessageSize,
        maxHeaderListSize, keepAliveTimeNanos, keepAliveTimeoutNanos, keepAliveWithoutCalls,
        transportTracerFactory);
  }

  @CanIgnoreReturnValue
  NettyChannelBuilder disableCheckAuthority() {
    this.managedChannelImplBuilder.disableCheckAuthority();
    return this;
  }

  @CanIgnoreReturnValue
  NettyChannelBuilder enableCheckAuthority() {
    this.managedChannelImplBuilder.enableCheckAuthority();
    return this;
  }

  void setTracingEnabled(boolean value) {
    this.managedChannelImplBuilder.setTracingEnabled(value);
  }

  void setStatsEnabled(boolean value) {
    this.managedChannelImplBuilder.setStatsEnabled(value);
  }

  void setStatsRecordStartedRpcs(boolean value) {
    this.managedChannelImplBuilder.setStatsRecordStartedRpcs(value);
  }

  void setStatsRecordFinishedRpcs(boolean value) {
    this.managedChannelImplBuilder.setStatsRecordFinishedRpcs(value);
  }

  void setStatsRecordRealTimeMetrics(boolean value) {
    this.managedChannelImplBuilder.setStatsRecordRealTimeMetrics(value);
  }

  void setStatsRecordRetryMetrics(boolean value) {
    this.managedChannelImplBuilder.setStatsRecordRetryMetrics(value);
  }

  @CanIgnoreReturnValue
  @VisibleForTesting
  NettyChannelBuilder setTransportTracerFactory(TransportTracer.Factory transportTracerFactory) {
    this.transportTracerFactory = transportTracerFactory;
    return this;
  }

  /**
   * Creates Netty transports. Exposed for internal use, as it should be private.
   */
  private static final class NettyTransportFactory implements ClientTransportFactory {
    private final NettyClientBootstrapFactory clientBootstrapFactory;
    private final boolean autoFlowControl;
    private final int flowControlWindow;
    private final int maxMessageSize;
    private final int maxHeaderListSize;
    private final long keepAliveTimeNanos;
    private final AtomicBackoff keepAliveBackoff;
    private final long keepAliveTimeoutNanos;
    private final boolean keepAliveWithoutCalls;
    private final TransportTracer.Factory transportTracerFactory;

    private boolean closed;

    NettyTransportFactory(
        NettyClientBootstrapFactory clientBootstrapFactory,
        boolean autoFlowControl, int flowControlWindow, int maxMessageSize, int maxHeaderListSize,
        long keepAliveTimeNanos, long keepAliveTimeoutNanos, boolean keepAliveWithoutCalls,
        TransportTracer.Factory transportTracerFactory) {
      this.clientBootstrapFactory = checkNotNull(clientBootstrapFactory, "clientBootstrapFactory");
      this.autoFlowControl = autoFlowControl;
      this.flowControlWindow = flowControlWindow;
      this.maxMessageSize = maxMessageSize;
      this.maxHeaderListSize = maxHeaderListSize;
      this.keepAliveTimeNanos = keepAliveTimeNanos;
      this.keepAliveBackoff = new AtomicBackoff("keepalive time nanos", keepAliveTimeNanos);
      this.keepAliveTimeoutNanos = keepAliveTimeoutNanos;
      this.keepAliveWithoutCalls = keepAliveWithoutCalls;
      this.transportTracerFactory = transportTracerFactory;
    }

    @Override
    public ConnectionClientTransport newClientTransport(
        SocketAddress serverAddress, ClientTransportOptions options, ChannelLogger channelLogger) {
      checkState(!closed, "The transport factory is closed.");

      final AtomicBackoff.State keepAliveTimeNanosState = keepAliveBackoff.getState();
      Runnable tooManyPingsRunnable = new Runnable() {
        @Override
        public void run() {
          keepAliveTimeNanosState.backoff();
        }
      };

      // TODO(carl-mastrangelo): Pass channelLogger in.
      NettyClientTransport transport = new NettyClientTransport(
          serverAddress,
          clientBootstrapFactory,
          //  autoFlowControl, flowControlWindow,
          maxMessageSize, keepAliveTimeNanosState.get(), keepAliveTimeoutNanos,
          keepAliveWithoutCalls, options.getAuthority(),
          tooManyPingsRunnable, transportTracerFactory.create(), options.getEagAttributes(),
          channelLogger);
      return transport;
    }

    @Override
    public ScheduledExecutorService getScheduledExecutorService() {
      return clientBootstrapFactory.getGroup();
    }

    @Override
    public SwapChannelCredentialsResult swapChannelCredentials(ChannelCredentials channelCreds) {
      checkNotNull(channelCreds, "channelCreds");
      throw new RuntimeException("NOT IMPLEMENTED");
    }

    @Override
    public void close() {
      if (closed) {
        return;
      }
      closed = true;
    }
  }
}
