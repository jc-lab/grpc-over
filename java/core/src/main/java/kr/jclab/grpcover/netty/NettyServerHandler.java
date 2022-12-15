package kr.jclab.grpcover.netty;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import io.grpc.*;
import io.grpc.internal.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.util.ReferenceCountUtil;
import io.perfmark.PerfMark;
import io.perfmark.Tag;
import kr.jclab.grpcover.core.protocol.v1.GofProto;
import kr.jclab.grpcover.gofprotocol.*;
import kr.jclab.grpcover.gofprotocol.FrameWriter;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.CheckForNull;
import javax.annotation.Nullable;
import java.text.MessageFormat;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.grpc.internal.GrpcUtil.SERVER_KEEPALIVE_TIME_NANOS_DISABLED;
import static kr.jclab.grpcover.netty.NettyServerBuilder.*;

/**
 * Server-side Netty handler for GRPC processing. All event handlers are executed entirely within
 * the context of the Netty Channel thread.
 */
@Slf4j
class NettyServerHandler extends AbstractNettyHandler {
  private static final Logger logger = Logger.getLogger(NettyServerHandler.class.getName());
  private static final long KEEPALIVE_PING = 0xDEADL;
  @VisibleForTesting
  static final long GRACEFUL_SHUTDOWN_PING = 0x97ACEF001L;
  private static final long GRACEFUL_SHUTDOWN_PING_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(10);
  /** Temporary workaround for #8674. Fine to delete after v1.45 release, and maybe earlier. */
  private static final boolean DISABLE_CONNECTION_HEADER_CHECK = Boolean.parseBoolean(
      System.getProperty("io.grpc.netty.disableConnectionHeaderCheck", "false"));

  private final GofConnection.PropertyKey<NettyServerStream.TransportState> streamKey;
  private final ServerTransportListener transportListener;
  private final int maxMessageSize;
  private final long keepAliveTimeInNanos;
  private final long keepAliveTimeoutInNanos;
  private final long maxConnectionAgeInNanos;
  private final long maxConnectionAgeGraceInNanos;
  private final List<? extends ServerStreamTracer.Factory> streamTracerFactories;
  private final TransportTracer transportTracer;
  private final KeepAliveEnforcer keepAliveEnforcer;
  /** Incomplete attributes produced by negotiator. */
  private Attributes negotiationAttributes;
  private InternalChannelz.Security securityInfo;
  /** Completed attributes produced by transportReady. */
  private Attributes attributes;
  private Throwable connectionError;
  private boolean teWarningLogged;
  private WriteQueue serverWriteQueue;
  private String lastKnownAuthority;
  @CheckForNull
  private KeepAliveManager keepAliveManager;
  @CheckForNull
  private MaxConnectionIdleManager maxConnectionIdleManager;
  @CheckForNull
  private ScheduledFuture<?> maxConnectionAgeMonitor;
  @CheckForNull
  private GracefulShutdown gracefulShutdown;

  private final DefaultGofDecoder decoder;

  @Override
  protected DefaultGofDecoder decoder() {
    return this.decoder;
  }

  @lombok.Builder(builderClassName = "Builder")
  NettyServerHandler(
      ChannelPromise channelUnused,
      GofConnection connection,
      ServerTransportListener transportListener,
      List<? extends ServerStreamTracer.Factory> streamTracerFactories,
      TransportTracer transportTracer,
      FrameWriter frameWriter,
      int maxStreams,
      int maxMessageSize,
      long keepAliveTimeInNanos,
      long keepAliveTimeoutInNanos,
      long maxConnectionIdleInNanos,
      long maxConnectionAgeInNanos,
      long maxConnectionAgeGraceInNanos,
      boolean permitKeepAliveWithoutCalls,
      long permitKeepAliveTimeInNanos
  ) {
    super(connection, maxStreams, new ServerChannelLogger());

    KeepAliveEnforcer keepAliveEnforcer = new KeepAliveEnforcer(
            permitKeepAliveWithoutCalls, permitKeepAliveTimeInNanos, TimeUnit.NANOSECONDS);

    final MaxConnectionIdleManager maxConnectionIdleManager;
    if (maxConnectionIdleInNanos == MAX_CONNECTION_IDLE_NANOS_DISABLED) {
      maxConnectionIdleManager = null;
    } else {
      maxConnectionIdleManager = new MaxConnectionIdleManager(maxConnectionIdleInNanos);
    }

    this.frameWriter = new WriteMonitoringFrameWriter(frameWriter, keepAliveEnforcer);
    decoder = new DefaultGofDecoder(connection(), frameWriter);
    decoder.setFrameHandler(frameHandler);
    decoder.setLifecycleManager(this);

    connection().addListener(new GofConnection.Listener() {
      @Override
      public void onStreamActive(GofStream stream) {
        if (connection().numActiveStreams() == 1) {
          keepAliveEnforcer.onTransportActive();
          if (maxConnectionIdleManager != null) {
            maxConnectionIdleManager.onTransportActive();
          }
        }
      }

      @Override
      public void onStreamClosed(GofStream stream) {
        if (connection().numActiveStreams() == 0) {
          keepAliveEnforcer.onTransportIdle();
          if (maxConnectionIdleManager != null) {
            maxConnectionIdleManager.onTransportIdle();
          }
        }
      }
    });

    checkArgument(maxMessageSize >= 0, "maxMessageSize must be non-negative: %s", maxMessageSize);
    this.maxMessageSize = maxMessageSize;
    this.keepAliveTimeInNanos = keepAliveTimeInNanos;
    this.keepAliveTimeoutInNanos = keepAliveTimeoutInNanos;
    this.maxConnectionIdleManager = maxConnectionIdleManager;
    this.maxConnectionAgeInNanos = maxConnectionAgeInNanos;
    this.maxConnectionAgeGraceInNanos = maxConnectionAgeGraceInNanos;
    this.keepAliveEnforcer = checkNotNull(keepAliveEnforcer, "keepAliveEnforcer");

    streamKey = connection().newKey();
    this.transportListener = checkNotNull(transportListener, "transportListener");
    this.streamTracerFactories = checkNotNull(streamTracerFactories, "streamTracerFactories");
    this.transportTracer = checkNotNull(transportTracer, "transportTracer");
  }

  public static class Builder {
    public NettyServerHandler build() {
      if (connection == null) {
        connection = new DefaultGofConnection(true);
      }
      if (frameWriter == null) {
        frameWriter = new DefaultGofFrameWriter(connection);
      }
      return new NettyServerHandler(
              channelUnused,
              connection,
              transportListener,
              streamTracerFactories,
              transportTracer,
              frameWriter,
              maxStreams,
              maxMessageSize,
              keepAliveTimeInNanos,
              keepAliveTimeoutInNanos,
              maxConnectionIdleInNanos,
              maxConnectionAgeInNanos,
              maxConnectionAgeGraceInNanos,
              permitKeepAliveWithoutCalls,
              permitKeepAliveTimeInNanos
      );
    }
  }

  @Nullable
  Throwable connectionError() {
    return connectionError;
  }

  @Override
  public void handlerAdded(final ChannelHandlerContext ctx) throws Exception {
    serverWriteQueue = new WriteQueue(ctx.channel());

    // init max connection age monitor
    if (maxConnectionAgeInNanos != MAX_CONNECTION_AGE_NANOS_DISABLED) {
      maxConnectionAgeMonitor = ctx.executor().schedule(
          new LogExceptionRunnable(new Runnable() {
            @Override
            public void run() {
              if (gracefulShutdown == null) {
                gracefulShutdown = new GracefulShutdown("max_age", maxConnectionAgeGraceInNanos);
                gracefulShutdown.start(ctx);
                ctx.flush();
              }
            }
          }),
          maxConnectionAgeInNanos,
          TimeUnit.NANOSECONDS);
    }

    if (maxConnectionIdleManager != null) {
      maxConnectionIdleManager.start(new Runnable() {
        @Override
        public void run() {
          if (gracefulShutdown == null) {
            gracefulShutdown = new GracefulShutdown("max_idle", null);
            gracefulShutdown.start(ctx);
            ctx.flush();
          }
        }
      }, ctx.executor());
    }

    if (keepAliveTimeInNanos != SERVER_KEEPALIVE_TIME_NANOS_DISABLED) {
      keepAliveManager = new KeepAliveManager(new KeepAlivePinger(ctx), ctx.executor(),
          keepAliveTimeInNanos, keepAliveTimeoutInNanos, true /* keepAliveDuringTransportIdle */);
      keepAliveManager.onTransportStarted();
    }

    super.handlerAdded(ctx);
  }

  private void onHeadersRead(ChannelHandlerContext ctx, GofStream gofStream, GofProto.Header header)
      throws GofException {
    try {
      String method = header.getMethod();

      Metadata metadata = Utils.convertHeaders(header);
      StatsTraceContext statsTraceCtx =
          StatsTraceContext.newServerContext(streamTracerFactories, method, metadata);

      NettyServerStream.TransportState state = new NettyServerStream.TransportState(
          this,
          ctx.channel().eventLoop(),
          gofStream,
          maxMessageSize,
          statsTraceCtx,
          transportTracer,
          method);

      PerfMark.startTask("NettyServerHandler.onHeadersRead", state.tag());
      try {
        String authority = getOrUpdateAuthority(header.getAuthority());
        NettyServerStream stream = new NettyServerStream(
            ctx.channel(),
            state,
            attributes,
            authority,
            statsTraceCtx,
            transportTracer);
        transportListener.streamCreated(stream, method, metadata);
        state.onStreamAllocated();
        gofStream.setProperty(streamKey, state);
      } finally {
        PerfMark.stopTask("NettyServerHandler.onHeadersRead", state.tag());
      }
    } catch (Exception e) {
      logger.log(Level.WARNING, "Exception in onHeadersRead()", e);
      // Throw an exception that will get handled by onStreamError.
      throw newStreamException(gofStream.id(), e);
    }
  }

  private String getOrUpdateAuthority(String authority) {
    if (authority == null) {
      return null;
    } else if (!authority.equals(lastKnownAuthority)) {
      lastKnownAuthority = authority;
    }

    // AsciiString.toString() is internally cached, so subsequent calls will not
    // result in recomputing the String representation of lastKnownAuthority.
    return lastKnownAuthority;
  }

  private void onDataRead(GofStream gofStream, ByteBuf data, boolean endOfStream)
      throws GofException {
    try {
      NettyServerStream.TransportState stream = serverStream(gofStream);
      stream.inboundDataReceived(data, endOfStream);
    } catch (Throwable e) {
      logger.log(Level.WARNING, "Exception in onDataRead()", e);
      // Throw an exception that will get handled by onStreamError.
      throw newStreamException(gofStream.id(), e);
    }
  }

  private void onRstStreamRead(GofStream gofStream, long errorCode) throws GofException {
    try {
      NettyServerStream.TransportState stream = serverStream(gofStream);
      if (stream != null) {
        PerfMark.startTask("NettyServerHandler.onRstStreamRead", stream.tag());
        try {
          stream.transportReportStatus(
              Status.CANCELLED.withDescription("RST_STREAM received for code " + errorCode));
        } finally {
          PerfMark.stopTask("NettyServerHandler.onRstStreamRead", stream.tag());
        }
      }
    } catch (Throwable e) {
      logger.log(Level.WARNING, "Exception in onRstStreamRead()", e);
      // Throw an exception that will get handled by onStreamError.
      throw newStreamException(gofStream.id(), e);
    }
  }

  @Override
  protected void onConnectionError(ChannelHandlerContext ctx, boolean outbound, Throwable cause,
      GofException http2Ex) {
    logger.log(Level.FINE, "Connection Error", cause);
    connectionError = cause;
    super.onConnectionError(ctx, outbound, cause, http2Ex);
  }

  @Override
  protected void onStreamError(ChannelHandlerContext ctx, boolean outbound, Throwable cause, GofException.StreamException gofException) {
    NettyServerStream.TransportState serverStream = serverStream(
        connection().stream(GofException.streamId(gofException)));
    Level level = Level.WARNING;
    if (serverStream == null && gofException.error() == GrpcUtil.Http2Error.STREAM_CLOSED) {
      level = Level.FINE;
    }
    logger.log(level, "Stream Error", cause);
    Tag tag = serverStream != null ? serverStream.tag() : PerfMark.createTag();
    PerfMark.startTask("NettyServerHandler.onStreamError", tag);
    try {
      if (serverStream != null) {
        serverStream.transportReportStatus(Utils.statusFromThrowable(cause));
      }
      // TODO(ejona): Abort the stream by sending headers to help the client with debugging.
      // Delegate to the base class to send a RST_STREAM.
      super.onStreamError(ctx, outbound, cause, gofException);
    } finally {
      PerfMark.stopTask("NettyServerHandler.onStreamError", tag);
    }
  }

  InternalChannelz.Security getSecurityInfo() {
    return securityInfo;
  }

  @VisibleForTesting
  KeepAliveManager getKeepAliveManagerForTest() {
    return keepAliveManager;
  }

  @VisibleForTesting
  void setKeepAliveManagerForTest(KeepAliveManager keepAliveManager) {
    this.keepAliveManager = keepAliveManager;
  }

  /**
   * Handler for the Channel shutting down.
   */
  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    try {
      if (keepAliveManager != null) {
        keepAliveManager.onTransportTermination();
      }
      if (maxConnectionIdleManager != null) {
        maxConnectionIdleManager.onTransportTermination();
      }
      if (maxConnectionAgeMonitor != null) {
        maxConnectionAgeMonitor.cancel(false);
      }
      final Status status =
          Status.UNAVAILABLE.withDescription("connection terminated for unknown reason");
      // Any streams that are still active must be closed
      connection().forEachActiveStream(new GofStreamVisitor() {
        @Override
        public boolean visit(GofStream stream) throws GofException {
          NettyServerStream.TransportState serverStream = serverStream(stream);
          if (serverStream != null) {
            serverStream.transportReportStatus(status);
          }
          return true;
        }
      });
    } finally {
      super.channelInactive(ctx);
    }
  }

  WriteQueue getWriteQueue() {
    return serverWriteQueue;
  }

  /**
   * Handler for commands sent from the stream.
   */
  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
      throws Exception {
    if (msg instanceof SendGrpcFrameCommand) {
      sendGrpcFrame(ctx, (SendGrpcFrameCommand) msg, promise);
    } else if (msg instanceof SendResponseHeadersCommand) {
      sendResponseHeaders(ctx, (SendResponseHeadersCommand) msg, promise);
    } else if (msg instanceof CancelServerStreamCommand) {
      cancelStream(ctx, (CancelServerStreamCommand) msg, promise);
    } else if (msg instanceof GracefulServerCloseCommand) {
      gracefulClose(ctx, (GracefulServerCloseCommand) msg, promise);
    } else if (msg instanceof ForcefulCloseCommand) {
      forcefulClose(ctx, (ForcefulCloseCommand) msg, promise);
    } else {
      AssertionError e =
          new AssertionError("Write called for unexpected type: " + msg.getClass().getName());
      ReferenceCountUtil.release(msg);
      promise.setFailure(e);
      throw e;
    }
  }

  @Override
  public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
    gracefulClose(ctx, new GracefulServerCloseCommand("app_requested"), promise);
    ctx.flush();
  }

  private void closeStreamWhenDone(ChannelPromise promise, int streamId) throws GofException {
    final NettyServerStream.TransportState stream = serverStream(requireGofStream(streamId));
    promise.addListener(new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture future) {
        stream.complete();
      }
    });
  }

  /**
   * Sends the given gRPC frame to the client.
   */
  private void sendGrpcFrame(ChannelHandlerContext ctx, SendGrpcFrameCommand cmd,
      ChannelPromise promise) throws GofException {
    PerfMark.startTask("NettyServerHandler.sendGrpcFrame", cmd.stream().tag());
    PerfMark.linkIn(cmd.getLink());
    try {
      if (cmd.endStream()) {
        closeStreamWhenDone(promise, cmd.stream().id());
      }
      // Call the base class to write the HTTP/2 DATA frame.
      frameWriter().writeData(ctx, cmd.stream().id(), cmd.content(), cmd.endStream(), promise);
    } finally {
      PerfMark.stopTask("NettyServerHandler.sendGrpcFrame", cmd.stream().tag());
    }
  }

  /**
   * Sends the response headers to the client.
   */
  private void sendResponseHeaders(ChannelHandlerContext ctx, SendResponseHeadersCommand cmd,
      ChannelPromise promise) throws GofException {
    PerfMark.startTask("NettyServerHandler.sendResponseHeaders", cmd.stream().tag());
    PerfMark.linkIn(cmd.getLink());
    try {
      // TODO(carl-mastrangelo): remove this check once https://github.com/netty/netty/issues/6296
      // is fixed.
      int streamId = cmd.stream().id();
      GofStream stream = connection().stream(streamId);
      if (stream == null) {
         resetStream(ctx, streamId, GrpcUtil.Http2Error.CANCEL.code(), promise);
        return ;
      }
      if (cmd.endOfStream()) {
        closeStreamWhenDone(promise, streamId);
      }
      frameWriter().writeHeaders(ctx, streamId, cmd.headers(), cmd.endOfStream(), promise);
    } finally {
      PerfMark.stopTask("NettyServerHandler.sendResponseHeaders", cmd.stream().tag());
    }
  }

  private void cancelStream(ChannelHandlerContext ctx, CancelServerStreamCommand cmd,
      ChannelPromise promise) {
    PerfMark.startTask("NettyServerHandler.cancelStream", cmd.stream().tag());
    PerfMark.linkIn(cmd.getLink());
    try {
      // Notify the listener if we haven't already.
      cmd.stream().transportReportStatus(cmd.reason());
      // Terminate the stream.
      super.resetStream(ctx, cmd.stream().id(), GrpcUtil.Http2Error.CANCEL.code(), promise);
    } finally {
      PerfMark.stopTask("NettyServerHandler.cancelStream", cmd.stream().tag());
    }
  }

  private void gracefulClose(final ChannelHandlerContext ctx, final GracefulServerCloseCommand msg,
      ChannelPromise promise) throws Exception {
    // Ideally we'd adjust a pre-existing graceful shutdown's grace period to at least what is
    // requested here. But that's an edge case and seems bug-prone.
    if (gracefulShutdown == null) {
      Long graceTimeInNanos = null;
      if (msg.getGraceTimeUnit() != null) {
        graceTimeInNanos = msg.getGraceTimeUnit().toNanos(msg.getGraceTime());
      }
      gracefulShutdown = new GracefulShutdown(msg.getGoAwayDebugString(), graceTimeInNanos);
      gracefulShutdown.start(ctx);
    }
    promise.setSuccess();
  }

  private void forcefulClose(final ChannelHandlerContext ctx, final ForcefulCloseCommand msg,
      ChannelPromise promise) throws Exception {
    super.close(ctx, promise);
    connection().forEachActiveStream(new GofStreamVisitor() {
      @Override
      public boolean visit(GofStream stream) throws GofException {
        NettyServerStream.TransportState serverStream = serverStream(stream);
        if (serverStream != null) {
          PerfMark.startTask("NettyServerHandler.forcefulClose", serverStream.tag());
          PerfMark.linkIn(msg.getLink());
          try {
            serverStream.transportReportStatus(msg.getStatus());
            resetStream(ctx, stream.id(), GrpcUtil.Http2Error.CANCEL.code(), ctx.newPromise());
          } finally {
            PerfMark.stopTask("NettyServerHandler.forcefulClose", serverStream.tag());
          }
        }
        stream.close();
        return true;
      }
    });
  }

  private GofStream requireGofStream(int streamId) {
    GofStream stream = connection().stream(streamId);
    if (stream == null) {
      // This should never happen.
      throw new AssertionError("Stream does not exist: " + streamId);
    }
    return stream;
  }

  /**
   * Returns the server stream associated to the given HTTP/2 stream object.
   */
  private NettyServerStream.TransportState serverStream(GofStream stream) {
    return stream == null ? null : (NettyServerStream.TransportState) stream.getProperty(streamKey);
  }

  private GofException newStreamException(int streamId, Throwable cause) {
    return GofException.streamError(
        streamId, GrpcUtil.Http2Error.INTERNAL_ERROR, cause, Strings.nullToEmpty(cause.getMessage()));
  }

  private boolean started = false;

  private void checkNegotiationAndTransportReady() {
    if (!started && negotiationAttributes != null) {
      started = true;
      attributes = transportListener.transportReady(negotiationAttributes);
    }
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    checkNegotiationAndTransportReady();
    super.channelActive(ctx);
  }

  @Override
  public void handleProtocolNegotiationCompleted(Attributes attrs, InternalChannelz.Security securityInfo) {
    this.negotiationAttributes = attrs;
    this.securityInfo = securityInfo;
    NettyClientHandler.writeBufferingAndRemove(ctx().channel());

    checkNegotiationAndTransportReady();
  }

  private final FrameWriter frameWriter;
  @Override
  protected FrameWriter frameWriter() {
    return frameWriter;
  }

  private final FrameHandler frameHandler = new FrameHandler() {
    @Override
    public void onHeadersRead(ChannelHandlerContext ctx, GofStream stream, GofProto.Header header, boolean endStream) throws GofException {
      if (keepAliveManager != null) {
        keepAliveManager.onDataReceived();
      }
      NettyServerHandler.this.onHeadersRead(ctx, stream, header);
      if (endStream) {
        NettyServerHandler.this.onDataRead(stream, Unpooled.EMPTY_BUFFER, endStream);
      }
    }

    @Override
    public void onDataRead(ChannelHandlerContext ctx, GofStream stream, ByteBuf data, boolean endOfStream) throws GofException {
      if (keepAliveManager != null) {
        keepAliveManager.onDataReceived();
      }
      NettyServerHandler.this.onDataRead(stream, data, endOfStream);
    }

    @Override
    public void onGoAwayRead(ChannelHandlerContext ctx, int streamId, long errorCode, ByteBuf debugData) throws GofException {}

    @Override
    public void onRstStreamRead(ChannelHandlerContext ctx, GofStream stream, long errorCode) throws GofException {
        if (keepAliveManager != null) {
          keepAliveManager.onDataReceived();
        }
        NettyServerHandler.this.onRstStreamRead(stream, errorCode);
    }

    @Override
    public void onPingRead(ChannelHandlerContext ctx, long data) throws GofException {
      if (keepAliveManager != null) {
        keepAliveManager.onDataReceived();
      }

      if (!keepAliveEnforcer.pingAcceptable()) {
        ByteBuf debugData = ByteBufUtil.writeAscii(ctx.alloc(), "too_many_pings");
        goAway(ctx, connection().remote().lastStreamCreated(), GrpcUtil.Http2Error.ENHANCE_YOUR_CALM.code(),
                debugData, ctx.newPromise());
        Status status = Status.RESOURCE_EXHAUSTED.withDescription("Too many pings from client");
        try {
          forcefulClose(ctx, new ForcefulCloseCommand(status), ctx.newPromise());
        } catch (Exception ex) {
          onError(ctx, /* outbound= */ true, ex);
        }
      }
    }

    @Override
    public void onPongRead(ChannelHandlerContext ctx, long data) throws GofException {
      if (keepAliveManager != null) {
        keepAliveManager.onDataReceived();
      }
      if (data == GRACEFUL_SHUTDOWN_PING) {
        if (gracefulShutdown == null) {
          // this should never happen
          logger.warning("Received GRACEFUL_SHUTDOWN_PING Ack but gracefulShutdown is null");
        } else {
          gracefulShutdown.secondGoAwayAndClose(ctx);
        }
      } else if (data != KEEPALIVE_PING) {
        logger.warning("Received unexpected ping ack. No ping outstanding");
      }
    }
  };

  private final class KeepAlivePinger implements KeepAliveManager.KeepAlivePinger {
    final ChannelHandlerContext ctx;

    KeepAlivePinger(ChannelHandlerContext ctx) {
      this.ctx = ctx;
    }

    @Override
    public void ping() {
      ChannelFuture pingFuture = frameWriter().writePing(
          ctx, false /* isAck */, KEEPALIVE_PING, ctx.newPromise());
      ctx.flush();
      pingFuture.addListener(new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
          if (future.isSuccess()) {
            transportTracer.reportKeepAliveSent();
          }
        }
      });
    }

    @Override
    public void onPingTimeout() {
      try {
        forcefulClose(
            ctx,
            new ForcefulCloseCommand(Status.UNAVAILABLE
                .withDescription("Keepalive failed. The connection is likely gone")),
            ctx.newPromise());
      } catch (Exception ex) {
        try {
          exceptionCaught(ctx, ex);
        } catch (Exception ex2) {
          logger.log(Level.WARNING, "Exception while propagating exception", ex2);
          logger.log(Level.WARNING, "Original failure", ex);
        }
      }
    }
  }

  private final class GracefulShutdown {
    String goAwayMessage;

    /**
     * The grace time between starting graceful shutdown and closing the netty channel,
     * {@code null} is unspecified.
     */
    @CheckForNull
    Long graceTimeInNanos;

    /**
     * True if ping is Acked or ping is timeout.
     */
    boolean pingAckedOrTimeout;

    Future<?> pingFuture;

    GracefulShutdown(String goAwayMessage,
        @Nullable Long graceTimeInNanos) {
      this.goAwayMessage = goAwayMessage;
      this.graceTimeInNanos = graceTimeInNanos;
    }

    /**
     * Sends out first GOAWAY and ping, and schedules second GOAWAY and close.
     */
    void start(final ChannelHandlerContext ctx) {
      goAway(
          ctx,
          Integer.MAX_VALUE,
          GrpcUtil.Http2Error.NO_ERROR.code(),
          ByteBufUtil.writeAscii(ctx.alloc(), goAwayMessage),
          ctx.newPromise());

      pingFuture = ctx.executor().schedule(
          new Runnable() {
            @Override
            public void run() {
              secondGoAwayAndClose(ctx);
            }
          },
          GRACEFUL_SHUTDOWN_PING_TIMEOUT_NANOS,
          TimeUnit.NANOSECONDS);

      frameWriter().writePing(ctx, false /* isAck */, GRACEFUL_SHUTDOWN_PING, ctx.newPromise());
    }

    void secondGoAwayAndClose(ChannelHandlerContext ctx) {
      if (pingAckedOrTimeout) {
        return;
      }
      pingAckedOrTimeout = true;

      checkNotNull(pingFuture, "pingFuture");
      pingFuture.cancel(false);

      // send the second GOAWAY with last stream id
      goAway(
          ctx,
          connection().remote().lastStreamCreated(),
          GrpcUtil.Http2Error.NO_ERROR.code(),
          ByteBufUtil.writeAscii(ctx.alloc(), goAwayMessage),
          ctx.newPromise());

      // gracefully shutdown with specified grace time
      long savedGracefulShutdownTimeMillis = gracefulShutdownTimeoutMillis();
      long overriddenGraceTime = graceTimeOverrideMillis(savedGracefulShutdownTimeMillis);
      try {
        gracefulShutdownTimeoutMillis(overriddenGraceTime);
        NettyServerHandler.super.close(ctx, ctx.newPromise());
      } catch (Exception e) {
        onError(ctx, /* outbound= */ true, e);
      } finally {
        gracefulShutdownTimeoutMillis(savedGracefulShutdownTimeMillis);
      }
    }

    private long graceTimeOverrideMillis(long originalMillis) {
      if (graceTimeInNanos == null) {
        return originalMillis;
      }
      if (graceTimeInNanos == MAX_CONNECTION_AGE_GRACE_NANOS_INFINITE) {
        // netty treats -1 as "no timeout"
        return -1L;
      }
      return TimeUnit.NANOSECONDS.toMillis(graceTimeInNanos);
    }
  }

  // Use a frame writer so that we know when frames are through flow control and actually being
  // written.
  private static class WriteMonitoringFrameWriter extends FrameWriterDecorator {
    private final KeepAliveEnforcer keepAliveEnforcer;

    public WriteMonitoringFrameWriter(FrameWriter delegate, KeepAliveEnforcer keepAliveEnforcer) {
      super(delegate);
      this.keepAliveEnforcer = keepAliveEnforcer;
    }

    @Override
    public ChannelFuture writeHeaders(ChannelHandlerContext ctx, int streamId, GofProto.Header headers, boolean endStream, ChannelPromise promise) {
      keepAliveEnforcer.resetCounters();
      return super.writeHeaders(ctx, streamId, headers, endStream, promise);
    }

    @Override
    public ChannelFuture writeData(ChannelHandlerContext ctx, int streamId, ByteBuf data, boolean endStream, ChannelPromise promise) {
      keepAliveEnforcer.resetCounters();
      return super.writeData(ctx, streamId, data, endStream, promise);
    }
  }

  private static class ServerChannelLogger extends ChannelLogger {
    private static final Logger log = Logger.getLogger(ChannelLogger.class.getName());

    @Override
    public void log(ChannelLogLevel level, String message) {
      log.log(toJavaLogLevel(level), message);
    }

    @Override
    public void log(ChannelLogLevel level, String messageFormat, Object... args) {
      log(level, MessageFormat.format(messageFormat, args));
    }
  }

  private static Level toJavaLogLevel(ChannelLogger.ChannelLogLevel level) {
    switch (level) {
      case ERROR:
        return Level.FINE;
      case WARNING:
        return Level.FINER;
      default:
        return Level.FINEST;
    }
  }
}
