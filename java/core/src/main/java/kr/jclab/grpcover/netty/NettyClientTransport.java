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
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.grpc.*;
import io.grpc.InternalChannelz.SocketStats;
import io.grpc.internal.*;
import io.grpc.internal.KeepAliveManager.ClientKeepAlivePinger;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.*;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import kr.jclab.grpcover.portable.NettyClientBootstrapFactory;

import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.Map;
import java.util.concurrent.Executor;

import static io.grpc.internal.GrpcUtil.KEEPALIVE_TIME_NANOS_DISABLED;
import static io.netty.channel.ChannelOption.ALLOCATOR;
import static io.netty.channel.ChannelOption.SO_KEEPALIVE;

/**
 * A Netty-based {@link ConnectionClientTransport} implementation.
 */
class NettyClientTransport implements ConnectionClientTransport {

  private final InternalLogId logId;
  private final SocketAddress remoteAddress;
  private final String target;
  private final NettyClientBootstrapFactory clientBootstrapFactory;
  private final String authority;
  private final int maxMessageSize;
  private KeepAliveManager keepAliveManager;
  private final long keepAliveTimeNanos;
  private final long keepAliveTimeoutNanos;
  private final boolean keepAliveWithoutCalls;
  private final Runnable tooManyPingsRunnable;
  private NettyClientHandler handler;
  // We should not send on the channel until negotiation completes. This is a hard requirement
  // by SslHandler but is appropriate for HTTP/1.1 Upgrade as well.
  private Channel channel;
  /** If {@link #start} has been called, non-{@code null} if channel is {@code null}. */
  private Status statusExplainingWhyTheChannelIsNull;
  /** Since not thread-safe, may only be used from event loop. */
  private ClientTransportLifecycleManager lifecycleManager;
  /** Since not thread-safe, may only be used from event loop. */
  private final TransportTracer transportTracer;
  private final Attributes eagAttributes;
  private final ChannelLogger channelLogger;

  NettyClientTransport(
      SocketAddress address,
      String target,
      NettyClientBootstrapFactory clientBootstrapFactory,
      int maxMessageSize,
      long keepAliveTimeNanos, long keepAliveTimeoutNanos,
      boolean keepAliveWithoutCalls, String authority,
      Runnable tooManyPingsRunnable, TransportTracer transportTracer, Attributes eagAttributes,
      ChannelLogger channelLogger) {
    this.remoteAddress = Preconditions.checkNotNull(address, "address");
    this.target = target;
    this.clientBootstrapFactory = Preconditions.checkNotNull(clientBootstrapFactory, "clientBootstrapFactory");
    this.maxMessageSize = maxMessageSize;
    this.keepAliveTimeNanos = keepAliveTimeNanos;
    this.keepAliveTimeoutNanos = keepAliveTimeoutNanos;
    this.keepAliveWithoutCalls = keepAliveWithoutCalls;
    this.authority = authority;
    this.tooManyPingsRunnable =
        Preconditions.checkNotNull(tooManyPingsRunnable, "tooManyPingsRunnable");
    this.transportTracer = Preconditions.checkNotNull(transportTracer, "transportTracer");
    this.eagAttributes = Preconditions.checkNotNull(eagAttributes, "eagAttributes");
    this.logId = InternalLogId.allocate(getClass(), remoteAddress.toString());
    this.channelLogger = Preconditions.checkNotNull(channelLogger, "channelLogger");
  }

  @Override
  public void ping(final PingCallback callback, final Executor executor) {
    if (channel == null) {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          callback.onFailure(statusExplainingWhyTheChannelIsNull.asException());
        }
      });
      return;
    }
    // The promise and listener always succeed in NettyClientHandler. So this listener handles the
    // error case, when the channel is closed and the NettyClientHandler no longer in the pipeline.
    ChannelFutureListener failureListener = new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture future) throws Exception {
        if (!future.isSuccess()) {
          Status s = statusFromFailedFuture(future);
          Http2Ping.notifyFailed(callback, executor, s.asException());
        }
      }
    };
    // Write the command requesting the ping
    handler.getWriteQueue().enqueue(new SendPingCommand(callback, executor), true)
        .addListener(failureListener);
  }

  @Override
  public ClientStream newStream(
      MethodDescriptor<?, ?> method, Metadata headers, CallOptions callOptions,
      ClientStreamTracer[] tracers) {
    Preconditions.checkNotNull(method, "method");
    Preconditions.checkNotNull(headers, "headers");
    if (channel == null) {
      return new FailingClientStream(statusExplainingWhyTheChannelIsNull, tracers);
    }
    StatsTraceContext statsTraceCtx =
        StatsTraceContext.newClientContext(tracers, getAttributes(), headers);
    return new NettyClientStream(
        new NettyClientStream.TransportState(
            handler,
            channel.eventLoop(),
            maxMessageSize,
            statsTraceCtx,
            transportTracer,
            method.getFullMethodName()) {
          @Override
          protected Status statusFromFailedFuture(ChannelFuture f) {
            return NettyClientTransport.this.statusFromFailedFuture(f);
          }
        },
        method,
        headers,
        channel,
        authority,
        statsTraceCtx,
        transportTracer,
        callOptions);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Runnable start(Listener transportListener) {
    lifecycleManager = new ClientTransportLifecycleManager(
        Preconditions.checkNotNull(transportListener, "listener"));
    EventLoop eventLoop = clientBootstrapFactory.getGroup().next();
    if (keepAliveTimeNanos != KEEPALIVE_TIME_NANOS_DISABLED) {
      keepAliveManager = new KeepAliveManager(
          new ClientKeepAlivePinger(this), eventLoop, keepAliveTimeNanos, keepAliveTimeoutNanos,
          keepAliveWithoutCalls);
    }

    handler = NettyClientHandler.builder()
            .lifecycleManager(lifecycleManager)
            .keepAliveManager(keepAliveManager)
//            .autoFlowControl(autoFlowControl)
//            .flowControlWindow(flowControlWindow)
//            .maxHeaderListSize(maxHeaderListSize)
            .stopwatchFactory(GrpcUtil.STOPWATCH_SUPPLIER)
            .tooManyPingsRunnable(tooManyPingsRunnable)
            .transportTracer(transportTracer)
            .eagAttributes(eagAttributes)
            .authority(authority)
            .negotiationLogger(channelLogger)
            .build();


    ProtocolNegotiators.GrpcNegotiationHandler negotiationHandler = new ProtocolNegotiators.GrpcNegotiationHandler(handler);
    ProtocolNegotiators.WaitUntilActiveHandler waitUntilActiveHandler = new ProtocolNegotiators.WaitUntilActiveHandler(negotiationHandler, handler.getNegotiationLogger());
    ChannelHandler bufferingHandler = new WriteBufferingAndExceptionHandler(clientBootstrapFactory.channelInitializer());

    Bootstrap b = clientBootstrapFactory.bootstrap();
    b.option(ALLOCATOR, Utils.getByteBufAllocator(false));
    b.attr(NettyChannelBuilder.GRPC_CHANNEL_HANDLER, waitUntilActiveHandler);
    if (target != null) {
      b.attr(NettyChannelBuilder.GRPC_TARGET, target);
    }

    // For non-socket based channel, the option will be ignored.
    b.option(SO_KEEPALIVE, true);

    b.handler(bufferingHandler);

    /*
     * We don't use a ChannelInitializer in the client bootstrap because its "initChannel" method
     * is executed in the event loop and we need this handler to be in the pipeline immediately so
     * that it may begin buffering writes.
     */
    ChannelFuture regFuture = b.register();
    if (regFuture.isDone() && !regFuture.isSuccess()) {
      channel = null;
      // Initialization has failed badly. All new streams should be made to fail.
      Throwable t = regFuture.cause();
      if (t == null) {
        t = new IllegalStateException("Channel is null, but future doesn't have a cause");
      }
      statusExplainingWhyTheChannelIsNull = Utils.statusFromThrowable(t);
      // Use a Runnable since lifecycleManager calls transportListener
      return new Runnable() {
        @Override
        public void run() {
          // NOTICE: we not are calling lifecycleManager from the event loop. But there isn't really
          // an event loop in this case, so nothing should be accessing the lifecycleManager. We
          // could use GlobalEventExecutor (which is what regFuture would use for notifying
          // listeners in this case), but avoiding on-demand thread creation in an error case seems
          // a good idea and is probably clearer threading.
          lifecycleManager.notifyTerminated(statusExplainingWhyTheChannelIsNull);
        }
      };
    }
    channel = regFuture.channel();
    // Start the write queue as soon as the channel is constructed
    handler.startWriteQueue(channel);
    // This write will have no effect, yet it will only complete once the negotiationHandler
    // flushes any pending writes. We need it to be staged *before* the `connect` so that
    // the channel can't have been closed yet, removing all handlers. This write will sit in the
    // AbstractBufferingHandler's buffer, and will either be flushed on a successful connection,
    // or failed if the connection fails.
    channel.writeAndFlush(NettyClientHandler.NOOP_MESSAGE).addListener(new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture future) throws Exception {
        if (!future.isSuccess()) {
          // Need to notify of this failure, because NettyClientHandler may not have been added to
          // the pipeline before the error occurred.
          lifecycleManager.notifyTerminated(Utils.statusFromThrowable(future.cause()));
        }
      }
    });
    channel.connect(remoteAddress);

    if (keepAliveManager != null) {
      keepAliveManager.onTransportStarted();
    }

    return null;
  }

  @Override
  public void shutdown(Status reason) {
    // start() could have failed
    if (channel == null) {
      return;
    }
    // Notifying of termination is automatically done when the channel closes.
    if (channel.isOpen()) {
      handler.getWriteQueue().enqueue(new GracefulCloseCommand(reason), true);
    }
  }

  @Override
  public void shutdownNow(final Status reason) {
    // Notifying of termination is automatically done when the channel closes.
    if (channel != null && channel.isOpen()) {
      handler.getWriteQueue().enqueue(new Runnable() {
        @Override
        public void run() {
          lifecycleManager.notifyShutdown(reason);
          channel.write(new ForcefulCloseCommand(reason));
        }
      }, true);
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("logId", logId.getId())
        .add("remoteAddress", remoteAddress)
        .add("channel", channel)
        .toString();
  }

  @Override
  public InternalLogId getLogId() {
    return logId;
  }

  @Override
  public Attributes getAttributes() {
    return handler.getAttributes();
  }

  @Override
  public ListenableFuture<SocketStats> getStats() {
    final SettableFuture<SocketStats> result = SettableFuture.create();
    if (channel.eventLoop().inEventLoop()) {
      // This is necessary, otherwise we will block forever if we get the future from inside
      // the event loop.
      result.set(getStatsHelper(channel));
      return result;
    }
    channel.eventLoop().submit(
        new Runnable() {
          @Override
          public void run() {
            result.set(getStatsHelper(channel));
          }
        })
        .addListener(
            new GenericFutureListener<Future<Object>>() {
              @Override
              public void operationComplete(Future<Object> future) throws Exception {
                if (!future.isSuccess()) {
                  result.setException(future.cause());
                }
              }
            });
    return result;
  }

  private SocketStats getStatsHelper(Channel ch) {
    assert ch.eventLoop().inEventLoop();
    return new SocketStats(
        transportTracer.getStats(),
        channel.localAddress(),
        channel.remoteAddress(),
        Utils.getSocketOptions(ch),
        handler == null ? null : handler.getSecurityInfo());
  }

  @VisibleForTesting
  Channel channel() {
    return channel;
  }

  @VisibleForTesting
  KeepAliveManager keepAliveManager() {
    return keepAliveManager;
  }

  /**
   * Convert ChannelFuture.cause() to a Status, taking into account that all handlers are removed
   * from the pipeline when the channel is closed. Since handlers are removed, you may get an
   * unhelpful exception like ClosedChannelException.
   *
   * <p>This method must only be called on the event loop.
   */
  private Status statusFromFailedFuture(ChannelFuture f) {
    Throwable t = f.cause();
    if (t instanceof ClosedChannelException
//        // Exception thrown by the StreamBufferingEncoder if the channel is closed while there
//        // are still streams buffered. This exception is not helpful. Replace it by the real
//        // cause of the shutdown (if available).
//        || t instanceof Http2ChannelClosedException
    ) {
      Status shutdownStatus = lifecycleManager.getShutdownStatus();
      if (shutdownStatus == null) {
        return Status.UNKNOWN.withDescription("Channel closed but for unknown reason")
            .withCause(new ClosedChannelException().initCause(t));
      }
      return shutdownStatus;
    }
    return Utils.statusFromThrowable(t);
  }
}
