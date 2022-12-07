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
import io.grpc.internal.InternalServer;
import io.grpc.internal.*;
import io.grpc.internal.ServerImplBuilder.ClientTransportServersBuilder;
import io.netty.channel.*;
import io.netty.channel.Channel;
import kr.jclab.grpcover.portable.GofChannelInitializer;

import javax.annotation.Nullable;
import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.*;
import static io.grpc.internal.GrpcUtil.*;

/**
 * A builder to help simplify the construction of a Netty-based GRPC server.
 */
@ExperimentalApi("https://github.com/grpc/grpc-java/issues/1784")
@CheckReturnValue
public final class NettyServerBuilder extends AbstractServerImplBuilder<NettyServerBuilder> {

  // 1MiB
  public static final int DEFAULT_FLOW_CONTROL_WINDOW = 1024 * 1024;

  static final long MAX_CONNECTION_IDLE_NANOS_DISABLED = Long.MAX_VALUE;
  static final long MAX_CONNECTION_AGE_NANOS_DISABLED = Long.MAX_VALUE;
  static final long MAX_CONNECTION_AGE_GRACE_NANOS_INFINITE = Long.MAX_VALUE;

  private static final long MIN_KEEPALIVE_TIME_NANO = TimeUnit.MILLISECONDS.toNanos(1L);
  private static final long MIN_KEEPALIVE_TIMEOUT_NANO = TimeUnit.MICROSECONDS.toNanos(499L);
  private static final long MIN_MAX_CONNECTION_IDLE_NANO = TimeUnit.SECONDS.toNanos(1L);
  private static final long MIN_MAX_CONNECTION_AGE_NANO = TimeUnit.SECONDS.toNanos(1L);
  private static final long AS_LARGE_AS_INFINITE = TimeUnit.DAYS.toNanos(1000L);

  private final ServerImplBuilder serverImplBuilder;
  private final Channel parentChannel;
  private final GofChannelInitializer channelInitializer;

  private TransportTracer.Factory transportTracerFactory = TransportTracer.getDefaultFactory();
  private EventLoopGroup workerEventLoopGroup = null;
  private boolean forceHeapBuffer;
  private int maxConcurrentCallsPerConnection = Integer.MAX_VALUE;
  private boolean autoFlowControl = true;
  private int flowControlWindow = DEFAULT_FLOW_CONTROL_WINDOW;
  private int maxMessageSize = DEFAULT_MAX_MESSAGE_SIZE;
  private int maxHeaderListSize = GrpcUtil.DEFAULT_MAX_HEADER_LIST_SIZE;
  private long keepAliveTimeInNanos = DEFAULT_SERVER_KEEPALIVE_TIME_NANOS;
  private long keepAliveTimeoutInNanos = DEFAULT_SERVER_KEEPALIVE_TIMEOUT_NANOS;
  private long maxConnectionIdleInNanos = MAX_CONNECTION_IDLE_NANOS_DISABLED;
  private long maxConnectionAgeInNanos = MAX_CONNECTION_AGE_NANOS_DISABLED;
  private long maxConnectionAgeGraceInNanos = MAX_CONNECTION_AGE_GRACE_NANOS_INFINITE;
  private boolean permitKeepAliveWithoutCalls;
  private long permitKeepAliveTimeInNanos = TimeUnit.MINUTES.toNanos(5);
  private Attributes eagAttributes = Attributes.EMPTY;

  /**
   * Creates a server builder that will bind to the given port.
   *
   * @param port the port on which the server is to be bound.
   * @return the server builder.
   */
  public static NettyServerBuilder forPort(int port) {
    throw new RuntimeException("NOT SUPPORT");
  }

  /**
   * Creates a server builder that will bind to the given port.
   *
   * @param port the port on which the server is to be bound.
   * @return the server builder.
   */
  public static NettyServerBuilder forPort(int port, ServerCredentials creds) {
    throw new RuntimeException("NOT SUPPORT");
  }

  /**
   * Creates a server builder configured with the given {@link SocketAddress}.
   *
   * @param address the socket address on which the server is to be bound.
   * @return the server builder
   */
  public static NettyServerBuilder forAddress(SocketAddress address) {
    throw new RuntimeException("NOT SUPPORT");
  }

  /**
   * Creates a server builder configured with the given {@link SocketAddress}.
   *
   * @param address the socket address on which the server is to be bound.
   * @return the server builder
   */
  public static NettyServerBuilder forAddress(SocketAddress address, ServerCredentials creds) {
    throw new RuntimeException("NOT SUPPORT");
  }

  public static NettyServerBuilder forInitializer(EventLoopGroup workerEventLoopGroup, @Nullable Channel parentChannel, GofChannelInitializer initializer) {
    return new NettyServerBuilder(workerEventLoopGroup, parentChannel, initializer);
  }

  private final class NettyClientTransportServersBuilder implements ClientTransportServersBuilder {
    @Override
    public InternalServer buildClientTransportServers(
        List<? extends ServerStreamTracer.Factory> streamTracerFactories) {
      return buildTransportServers(streamTracerFactories);
    }
  }

  private NettyServerBuilder(EventLoopGroup workerEventLoopGroup, @Nullable Channel parentChannel, GofChannelInitializer initializer) {
    serverImplBuilder = new ServerImplBuilder(new NettyClientTransportServersBuilder());
    this.workerEventLoopGroup = workerEventLoopGroup;
    this.parentChannel = parentChannel;
    this.channelInitializer = initializer;
  }

  @Internal
  @Override
  protected ServerBuilder<?> delegate() {
    return serverImplBuilder;
  }

  /**
   * Force using heap buffer when custom allocator is enabled.
   */
  void setForceHeapBuffer(boolean value) {
    forceHeapBuffer = value;
  }

  void setTracingEnabled(boolean value) {
    this.serverImplBuilder.setTracingEnabled(value);
  }

  void setStatsEnabled(boolean value) {
    this.serverImplBuilder.setStatsEnabled(value);
  }

  void setStatsRecordStartedRpcs(boolean value) {
    this.serverImplBuilder.setStatsRecordStartedRpcs(value);
  }

  void setStatsRecordRealTimeMetrics(boolean value) {
    this.serverImplBuilder.setStatsRecordRealTimeMetrics(value);
  }

  /**
   * The maximum number of concurrent calls permitted for each incoming connection. Defaults to no
   * limit.
   */
  @CanIgnoreReturnValue
  public NettyServerBuilder maxConcurrentCallsPerConnection(int maxCalls) {
    checkArgument(maxCalls > 0, "max must be positive: %s", maxCalls);
    this.maxConcurrentCallsPerConnection = maxCalls;
    return this;
  }

  /**
   * Sets the initial flow control window in bytes. Setting initial flow control window enables auto
   * flow control tuning using bandwidth-delay product algorithm. To disable auto flow control
   * tuning, use {@link #flowControlWindow(int)}. By default, auto flow control is enabled with
   * initial flow control window size of {@link #DEFAULT_FLOW_CONTROL_WINDOW}.
   */
  @CanIgnoreReturnValue
  public NettyServerBuilder initialFlowControlWindow(int initialFlowControlWindow) {
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
  public NettyServerBuilder flowControlWindow(int flowControlWindow) {
    checkArgument(flowControlWindow > 0, "flowControlWindow must be positive: %s",
        flowControlWindow);
    this.flowControlWindow = flowControlWindow;
    this.autoFlowControl = false;
    return this;
  }

  /**
   * Sets the maximum message size allowed to be received on the server. If not called,
   * defaults to 4 MiB. The default provides protection to services who haven't considered the
   * possibility of receiving large messages while trying to be large enough to not be hit in normal
   * usage.
   *
   * @deprecated Call {@link #maxInboundMessageSize} instead. This method will be removed in a
   *     future release.
   */
  @CanIgnoreReturnValue
  @Deprecated
  @InlineMe(replacement = "this.maxInboundMessageSize(maxMessageSize)")
  public NettyServerBuilder maxMessageSize(int maxMessageSize) {
    return maxInboundMessageSize(maxMessageSize);
  }

  /** {@inheritDoc} */
  @CanIgnoreReturnValue
  @Override
  public NettyServerBuilder maxInboundMessageSize(int bytes) {
    checkArgument(bytes >= 0, "bytes must be non-negative: %s", bytes);
    this.maxMessageSize = bytes;
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
  public NettyServerBuilder maxHeaderListSize(int maxHeaderListSize) {
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
  public NettyServerBuilder maxInboundMetadataSize(int bytes) {
    checkArgument(bytes > 0, "maxInboundMetadataSize must be positive: %s", bytes);
    this.maxHeaderListSize = bytes;
    return this;
  }

  /**
   * Sets a custom keepalive time, the delay time for sending next keepalive ping. An unreasonably
   * small value might be increased, and {@code Long.MAX_VALUE} nano seconds or an unreasonably
   * large value will disable keepalive.
   *
   * @since 1.3.0
   */
  @CanIgnoreReturnValue
  @Override
  public NettyServerBuilder keepAliveTime(long keepAliveTime, TimeUnit timeUnit) {
    checkArgument(keepAliveTime > 0L, "keepalive time must be positive: %s", keepAliveTime);
    keepAliveTimeInNanos = timeUnit.toNanos(keepAliveTime);
    keepAliveTimeInNanos = KeepAliveManager.clampKeepAliveTimeInNanos(keepAliveTimeInNanos);
    if (keepAliveTimeInNanos >= AS_LARGE_AS_INFINITE) {
      // Bump keepalive time to infinite. This disables keep alive.
      keepAliveTimeInNanos = SERVER_KEEPALIVE_TIME_NANOS_DISABLED;
    }
    if (keepAliveTimeInNanos < MIN_KEEPALIVE_TIME_NANO) {
      // Bump keepalive time.
      keepAliveTimeInNanos = MIN_KEEPALIVE_TIME_NANO;
    }
    return this;
  }

  /**
   * Sets a custom keepalive timeout, the timeout for keepalive ping requests. An unreasonably small
   * value might be increased.
   *
   * @since 1.3.0
   */
  @CanIgnoreReturnValue
  @Override
  public NettyServerBuilder keepAliveTimeout(long keepAliveTimeout, TimeUnit timeUnit) {
    checkArgument(keepAliveTimeout > 0L, "keepalive timeout must be positive: %s",
        keepAliveTimeout);
    keepAliveTimeoutInNanos = timeUnit.toNanos(keepAliveTimeout);
    keepAliveTimeoutInNanos =
        KeepAliveManager.clampKeepAliveTimeoutInNanos(keepAliveTimeoutInNanos);
    if (keepAliveTimeoutInNanos < MIN_KEEPALIVE_TIMEOUT_NANO) {
      // Bump keepalive timeout.
      keepAliveTimeoutInNanos = MIN_KEEPALIVE_TIMEOUT_NANO;
    }
    return this;
  }

  /**
   * Sets a custom max connection idle time, connection being idle for longer than which will be
   * gracefully terminated. Idleness duration is defined since the most recent time the number of
   * outstanding RPCs became zero or the connection establishment. An unreasonably small value might
   * be increased. {@code Long.MAX_VALUE} nano seconds or an unreasonably large value will disable
   * max connection idle.
   *
   * @since 1.4.0
   */
  @CanIgnoreReturnValue
  @Override
  public NettyServerBuilder maxConnectionIdle(long maxConnectionIdle, TimeUnit timeUnit) {
    checkArgument(maxConnectionIdle > 0L, "max connection idle must be positive: %s",
        maxConnectionIdle);
    maxConnectionIdleInNanos = timeUnit.toNanos(maxConnectionIdle);
    if (maxConnectionIdleInNanos >= AS_LARGE_AS_INFINITE) {
      maxConnectionIdleInNanos = MAX_CONNECTION_IDLE_NANOS_DISABLED;
    }
    if (maxConnectionIdleInNanos < MIN_MAX_CONNECTION_IDLE_NANO) {
      maxConnectionIdleInNanos = MIN_MAX_CONNECTION_IDLE_NANO;
    }
    return this;
  }

  /**
   * Sets a custom max connection age, connection lasting longer than which will be gracefully
   * terminated. An unreasonably small value might be increased.  A random jitter of +/-10% will be
   * added to it. {@code Long.MAX_VALUE} nano seconds or an unreasonably large value will disable
   * max connection age.
   *
   * @since 1.3.0
   */
  @CanIgnoreReturnValue
  @Override
  public NettyServerBuilder maxConnectionAge(long maxConnectionAge, TimeUnit timeUnit) {
    checkArgument(maxConnectionAge > 0L, "max connection age must be positive: %s",
        maxConnectionAge);
    maxConnectionAgeInNanos = timeUnit.toNanos(maxConnectionAge);
    if (maxConnectionAgeInNanos >= AS_LARGE_AS_INFINITE) {
      maxConnectionAgeInNanos = MAX_CONNECTION_AGE_NANOS_DISABLED;
    }
    if (maxConnectionAgeInNanos < MIN_MAX_CONNECTION_AGE_NANO) {
      maxConnectionAgeInNanos = MIN_MAX_CONNECTION_AGE_NANO;
    }
    return this;
  }

  /**
   * Sets a custom grace time for the graceful connection termination. Once the max connection age
   * is reached, RPCs have the grace time to complete. RPCs that do not complete in time will be
   * cancelled, allowing the connection to terminate. {@code Long.MAX_VALUE} nano seconds or an
   * unreasonably large value are considered infinite.
   *
   * @see #maxConnectionAge(long, TimeUnit)
   * @since 1.3.0
   */
  @CanIgnoreReturnValue
  @Override
  public NettyServerBuilder maxConnectionAgeGrace(long maxConnectionAgeGrace, TimeUnit timeUnit) {
    checkArgument(maxConnectionAgeGrace >= 0L, "max connection age grace must be non-negative: %s",
        maxConnectionAgeGrace);
    maxConnectionAgeGraceInNanos = timeUnit.toNanos(maxConnectionAgeGrace);
    if (maxConnectionAgeGraceInNanos >= AS_LARGE_AS_INFINITE) {
      maxConnectionAgeGraceInNanos = MAX_CONNECTION_AGE_GRACE_NANOS_INFINITE;
    }
    return this;
  }

  /**
   * Specify the most aggressive keep-alive time clients are permitted to configure. The server will
   * try to detect clients exceeding this rate and when detected will forcefully close the
   * connection. The default is 5 minutes.
   *
   * <p>Even though a default is defined that allows some keep-alives, clients must not use
   * keep-alive without approval from the service owner. Otherwise, they may experience failures in
   * the future if the service becomes more restrictive. When unthrottled, keep-alives can cause a
   * significant amount of traffic and CPU usage, so clients and servers should be conservative in
   * what they use and accept.
   *
   * @see #permitKeepAliveWithoutCalls(boolean)
   * @since 1.3.0
   */
  @CanIgnoreReturnValue
  @Override
  public NettyServerBuilder permitKeepAliveTime(long keepAliveTime, TimeUnit timeUnit) {
    checkArgument(keepAliveTime >= 0, "permit keepalive time must be non-negative: %s",
        keepAliveTime);
    permitKeepAliveTimeInNanos = timeUnit.toNanos(keepAliveTime);
    return this;
  }

  /**
   * Sets whether to allow clients to send keep-alive HTTP/2 PINGs even if there are no outstanding
   * RPCs on the connection. Defaults to {@code false}.
   *
   * @see #permitKeepAliveTime(long, TimeUnit)
   * @since 1.3.0
   */
  @CanIgnoreReturnValue
  @Override
  public NettyServerBuilder permitKeepAliveWithoutCalls(boolean permit) {
    permitKeepAliveWithoutCalls = permit;
    return this;
  }

  /** Sets the EAG attributes available to protocol negotiators. Not for general use. */
  void eagAttributes(Attributes eagAttributes) {
    this.eagAttributes = checkNotNull(eagAttributes, "eagAttributes");
  }

  NettyServer buildTransportServers(
      List<? extends ServerStreamTracer.Factory> streamTracerFactories) {
    assertEventLoop();

    return new NettyServer(
        parentChannel,
        channelInitializer,
        workerEventLoopGroup, forceHeapBuffer,
        streamTracerFactories, transportTracerFactory, maxConcurrentCallsPerConnection,
        autoFlowControl, flowControlWindow, maxMessageSize, maxHeaderListSize,
        keepAliveTimeInNanos, keepAliveTimeoutInNanos,
        maxConnectionIdleInNanos, maxConnectionAgeInNanos,
        maxConnectionAgeGraceInNanos, permitKeepAliveWithoutCalls, permitKeepAliveTimeInNanos,
        eagAttributes, this.serverImplBuilder.getChannelz());
  }

  @VisibleForTesting
  void assertEventLoop() {
    checkNotNull(workerEventLoopGroup);
  }

  @CanIgnoreReturnValue
  NettyServerBuilder setTransportTracerFactory(TransportTracer.Factory transportTracerFactory) {
    this.transportTracerFactory = transportTracerFactory;
    return this;
  }
}
