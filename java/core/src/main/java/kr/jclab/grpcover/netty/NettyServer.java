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

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.grpc.*;
import io.grpc.InternalChannelz.SocketStats;
import io.grpc.internal.InternalServer;
import io.grpc.internal.*;
import io.netty.channel.Channel;
import io.netty.channel.*;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import kr.jclab.grpcover.portable.GofChannelInitializer;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Netty-based server implementation.
 */
class NettyServer implements InternalServer, InternalWithLogId {
  private static final Logger log = Logger.getLogger(InternalServer.class.getName());

  private final InternalLogId logId;
  @Nullable
  private final Channel parentChannel;
  private final GofChannelInitializer channelInitializer;
  private final int maxStreamsPerConnection;
  private final boolean forceHeapBuffer;
  private EventLoopGroup bossGroup;
  private EventLoopGroup workerGroup;
  private ServerListener listener;
  private final boolean autoFlowControl;
  private final int flowControlWindow;
  private final int maxMessageSize;
  private final int maxHeaderListSize;
  private final long keepAliveTimeInNanos;
  private final long keepAliveTimeoutInNanos;
  private final long maxConnectionIdleInNanos;
  private final long maxConnectionAgeInNanos;
  private final long maxConnectionAgeGraceInNanos;
  private final boolean permitKeepAliveWithoutCalls;
  private final long permitKeepAliveTimeInNanos;
  private final Attributes eagAttributes;
  private final List<? extends ServerStreamTracer.Factory> streamTracerFactories;
  private final TransportTracer.Factory transportTracerFactory;
  private final InternalChannelz channelz;
  private volatile List<InternalInstrumented<SocketStats>> listenSocketStatsList =
      Collections.emptyList();
  private volatile boolean terminated;

  NettyServer(
      Channel parentChannel,
      GofChannelInitializer channelInitializer,
      EventLoopGroup workerGroup,
      boolean forceHeapBuffer,
      List<? extends ServerStreamTracer.Factory> streamTracerFactories,
      TransportTracer.Factory transportTracerFactory,
      int maxStreamsPerConnection, boolean autoFlowControl, int flowControlWindow,
      int maxMessageSize, int maxHeaderListSize,
      long keepAliveTimeInNanos, long keepAliveTimeoutInNanos,
      long maxConnectionIdleInNanos,
      long maxConnectionAgeInNanos, long maxConnectionAgeGraceInNanos,
      boolean permitKeepAliveWithoutCalls, long permitKeepAliveTimeInNanos,
      Attributes eagAttributes, InternalChannelz channelz) {
    this.parentChannel = parentChannel;
    this.channelInitializer = checkNotNull(channelInitializer, "channelInitializer");
    this.workerGroup = checkNotNull(workerGroup, "workerGroup");
    this.forceHeapBuffer = forceHeapBuffer;
    this.streamTracerFactories = checkNotNull(streamTracerFactories, "streamTracerFactories");
    this.transportTracerFactory = transportTracerFactory;
    this.maxStreamsPerConnection = maxStreamsPerConnection;
    this.autoFlowControl = autoFlowControl;
    this.flowControlWindow = flowControlWindow;
    this.maxMessageSize = maxMessageSize;
    this.maxHeaderListSize = maxHeaderListSize;
    this.keepAliveTimeInNanos = keepAliveTimeInNanos;
    this.keepAliveTimeoutInNanos = keepAliveTimeoutInNanos;
    this.maxConnectionIdleInNanos = maxConnectionIdleInNanos;
    this.maxConnectionAgeInNanos = maxConnectionAgeInNanos;
    this.maxConnectionAgeGraceInNanos = maxConnectionAgeGraceInNanos;
    this.permitKeepAliveWithoutCalls = permitKeepAliveWithoutCalls;
    this.permitKeepAliveTimeInNanos = permitKeepAliveTimeInNanos;
    this.eagAttributes = checkNotNull(eagAttributes, "eagAttributes");
    this.channelz = Preconditions.checkNotNull(channelz);
    this.logId = InternalLogId.allocate(getClass(), "No address");
  }

  @Override
  public SocketAddress getListenSocketAddress() {
    if (parentChannel != null) {
      return parentChannel.localAddress();
    }
    return null;
  }

  @Override
  public List<SocketAddress> getListenSocketAddresses() {
    if (parentChannel != null) {
      return Collections.singletonList(parentChannel.localAddress());
    }
    return Collections.emptyList();
  }

  @Override
  public InternalInstrumented<SocketStats> getListenSocketStats() {
    List<InternalInstrumented<SocketStats>> savedListenSocketStatsList = listenSocketStatsList;
    return savedListenSocketStatsList.isEmpty() ? null : savedListenSocketStatsList.get(0);
  }

  @Override
  public List<InternalInstrumented<SocketStats>> getListenSocketStatsList() {
    return listenSocketStatsList;
  }

  @Override
  public void start(ServerListener serverListener) throws IOException {
    listener = checkNotNull(serverListener, "serverListener");

    channelInitializer.attachGofServerChannelSetupHandler(ch -> {
      ChannelPromise channelDone = ch.newPromise();

      long maxConnectionAgeInNanos = NettyServer.this.maxConnectionAgeInNanos;
      if (maxConnectionAgeInNanos != NettyServerBuilder.MAX_CONNECTION_AGE_NANOS_DISABLED) {
        // apply a random jitter of +/-10% to max connection age
        maxConnectionAgeInNanos =
                (long) ((.9D + Math.random() * .2D) * maxConnectionAgeInNanos);
      }

      NettyServerTransport transport =
              new NettyServerTransport(
                      ch,
                      channelDone,
                      streamTracerFactories,
                      transportTracerFactory.create(),
                      maxStreamsPerConnection,
                      autoFlowControl,
                      flowControlWindow,
                      maxMessageSize,
                      maxHeaderListSize,
                      keepAliveTimeInNanos,
                      keepAliveTimeoutInNanos,
                      maxConnectionIdleInNanos,
                      maxConnectionAgeInNanos,
                      maxConnectionAgeGraceInNanos,
                      permitKeepAliveWithoutCalls,
                      permitKeepAliveTimeInNanos,
                      eagAttributes);
      ServerTransportListener transportListener;
      // This is to order callbacks on the listener, not to guard access to channel.
      synchronized (NettyServer.this) {
        if (terminated) {
          // Server already terminated.
          ch.close();
          return;
        }
        // `channel` shutdown can race with `ch` initialization, so this is only safe to increment
        // inside the lock.
        transportListener = listener.transportCreated(transport);
      }

      transport.start(transportListener);
    });

    final List<InternalInstrumented<SocketStats>> socketStats = new ArrayList<>();
    if (parentChannel != null) {
      final InternalInstrumented<SocketStats> listenSocketStats =
              new ListenSocket(parentChannel);
      channelz.addListenSocket(listenSocketStats);
      socketStats.add(listenSocketStats);
      parentChannel.closeFuture().addListener(new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
          channelz.removeListenSocket(listenSocketStats);
        }
      });
    }
    listenSocketStatsList = Collections.unmodifiableList(socketStats);
  }

  @Override
  public void shutdown() {
    if (terminated) {
      return;
    }
    if (parentChannel != null) {
      ChannelFuture channelFuture = parentChannel.close()
              .addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                  if (!future.isSuccess()) {
                    log.log(Level.WARNING, "Error closing server channel group", future.cause());
                  }
                  listenSocketStatsList = Collections.emptyList();
                  synchronized (NettyServer.this) {
                    listener.serverShutdown();
                    terminated = true;
                  }
                }
              });
      try {
        channelFuture.await();
      } catch (InterruptedException e) {
        log.log(Level.FINE, "Interrupted while shutting down", e);
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public InternalLogId getLogId() {
    return logId;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("logId", logId.getId())
        .add("addresses", getListenSocketAddress())
        .toString();
  }

  /**
   * A class that can answer channelz queries about the server listen sockets.
   */
  private static final class ListenSocket implements InternalInstrumented<SocketStats> {
    private final InternalLogId id;
    private final Channel ch;

    ListenSocket(Channel ch) {
      this.ch = ch;
      this.id = InternalLogId.allocate(getClass(), String.valueOf(ch.localAddress()));
    }

    @Override
    public ListenableFuture<SocketStats> getStats() {
      final SettableFuture<SocketStats> ret = SettableFuture.create();
      if (ch.eventLoop().inEventLoop()) {
        // This is necessary, otherwise we will block forever if we get the future from inside
        // the event loop.
        ret.set(new SocketStats(
            /*data=*/ null,
            ch.localAddress(),
            /*remote=*/ null,
            Utils.getSocketOptions(ch),
            /*security=*/ null));
        return ret;
      }
      ch.eventLoop()
          .submit(
              new Runnable() {
                @Override
                public void run() {
                  ret.set(new SocketStats(
                      /*data=*/ null,
                      ch.localAddress(),
                      /*remote=*/ null,
                      Utils.getSocketOptions(ch),
                      /*security=*/ null));
                }
              })
          .addListener(
              new GenericFutureListener<Future<Object>>() {
                @Override
                public void operationComplete(Future<Object> future) throws Exception {
                  if (!future.isSuccess()) {
                    ret.setException(future.cause());
                  }
                }
              });
      return ret;
    }

    @Override
    public InternalLogId getLogId() {
      return id;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("logId", id.getId())
          .add("channel", ch)
          .toString();
    }
  }
}
