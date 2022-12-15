package kr.jclab.grpcover.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.util.concurrent.Future;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import kr.jclab.grpcover.core.protocol.v1.GofProto;
import kr.jclab.grpcover.gofprotocol.*;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static io.grpc.internal.GrpcUtil.Http2Error.INTERNAL_ERROR;
import static io.grpc.internal.GrpcUtil.Http2Error.NO_ERROR;
import static io.netty.buffer.Unpooled.EMPTY_BUFFER;
import static io.netty.util.CharsetUtil.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public abstract class GofConnectionHandler extends ChannelDuplexHandler implements GofLifecycleManager, GofConnectionHandlerCallback {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(GofConnectionHandler.class);

    private ChannelHandlerContext ctx;

    private ChannelFutureListener closeListener;
    private long gracefulShutdownTimeoutMillis;

    protected abstract GofConnection connection();
    protected abstract FrameWriter frameWriter();
    protected final ChannelHandlerContext ctx() {
        return ctx;
    }

    /**
     * Get the amount of time (in milliseconds) this endpoint will wait for all streams to be closed before closing
     * the connection during the graceful shutdown process. Returns -1 if this connection is configured to wait
     * indefinitely for all streams to close.
     */
    public long gracefulShutdownTimeoutMillis() {
        return gracefulShutdownTimeoutMillis;
    }

    /**
     * Set the amount of time (in milliseconds) this endpoint will wait for all streams to be closed before closing
     * the connection during the graceful shutdown process.
     * @param gracefulShutdownTimeoutMillis the amount of time (in milliseconds) this endpoint will wait for all
     * streams to be closed before closing the connection during the graceful shutdown process.
     */
    public void gracefulShutdownTimeoutMillis(long gracefulShutdownTimeoutMillis) {
        if (gracefulShutdownTimeoutMillis < -1) {
            throw new IllegalArgumentException("gracefulShutdownTimeoutMillis: " + gracefulShutdownTimeoutMillis +
                    " (expected: -1 for indefinite or >= 0)");
        }
        this.gracefulShutdownTimeoutMillis = gracefulShutdownTimeoutMillis;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;
        super.handlerAdded(ctx);
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
//    if (decoupleCloseAndGoAway) {
//      ctx.close(promise);
//      return;
//    }
        promise = promise.unvoid();
        // Avoid NotYetConnectedException
        if (!ctx.channel().isActive()) {
            ctx.close(promise);
            return;
        }

        // If the user has already sent a GO_AWAY frame they may be attempting to do a graceful shutdown which requires
        // sending multiple GO_AWAY frames. We should only send a GO_AWAY here if one has not already been sent. If
        // a GO_AWAY has been sent we send a empty buffer just so we can wait to close until all other data has been
        // flushed to the OS.
        // https://github.com/netty/netty/issues/5307
        ChannelFuture f = connection().goAwaySent() ? ctx.write(EMPTY_BUFFER) : goAway(ctx, null, ctx.newPromise());
        ctx.flush();
        doGracefulShutdown(ctx, f, promise);
    }

    private ChannelFutureListener newClosingChannelFutureListener(
            ChannelHandlerContext ctx, ChannelPromise promise) {
        long gracefulShutdownTimeoutMillis = this.gracefulShutdownTimeoutMillis;
        return gracefulShutdownTimeoutMillis < 0 ?
                new ClosingChannelFutureListener(ctx, promise) :
                new ClosingChannelFutureListener(ctx, promise, gracefulShutdownTimeoutMillis, MILLISECONDS);
    }

    private void doGracefulShutdown(ChannelHandlerContext ctx, ChannelFuture future, final ChannelPromise promise) {
        final ChannelFutureListener listener = newClosingChannelFutureListener(ctx, promise);
        if (isGracefulShutdownComplete()) {
            // If there are no active streams, close immediately after the GO_AWAY write completes or the timeout
            // elapsed.
            future.addListener(listener);
        } else {
            // If there are active streams we should wait until they are all closed before closing the connection.

            // The ClosingChannelFutureListener will cascade promise completion. We need to always notify the
            // new ClosingChannelFutureListener when the graceful close completes if the promise is not null.
            if (closeListener == null) {
                closeListener = listener;
            } else if (promise != null) {
                final ChannelFutureListener oldCloseListener = closeListener;
                closeListener = new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        try {
                            oldCloseListener.operationComplete(future);
                        } finally {
                            listener.operationComplete(future);
                        }
                    }
                };
            }
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        // Trigger flush after read on the assumption that flush is cheap if there is nothing to write and that
        // for flow-control the read may release window that causes data to be written that can now be flushed.
        try {
            // First call channelReadComplete0(...) as this may produce more data that we want to flush
            channelReadComplete0(ctx);
        } finally {
            flush(ctx);
        }
    }

    final void channelReadComplete0(ChannelHandlerContext ctx) {
        ctx.fireChannelReadComplete();
    }

    /**
     * Sends a {@code RST_STREAM} frame even if we don't know about the stream. This error condition is most likely
     * triggered by the first frame of a stream being invalid. That is, there was an error reading the frame before
     * we could create a new stream.
     */
    private ChannelFuture resetUnknownStream(final ChannelHandlerContext ctx, int streamId, long errorCode,
                                             ChannelPromise promise) {
        ChannelFuture future = frameWriter().writeRstStream(ctx, streamId, errorCode, promise);
        if (future.isDone()) {
            closeConnectionOnError(ctx, future);
        } else {
            future.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    closeConnectionOnError(ctx, future);
                }
            });
        }
        return future;
    }

    @Override
    public ChannelFuture resetStream(final ChannelHandlerContext ctx, int streamId, long errorCode,
                                     ChannelPromise promise) {
        final GofStream stream = connection().stream(streamId);
        if (stream == null) {
            return resetUnknownStream(ctx, streamId, errorCode, promise.unvoid());
        }

        return resetStream(ctx, stream, errorCode, promise);
    }

    private ChannelFuture resetStream(final ChannelHandlerContext ctx, final GofStream stream,
                                      long errorCode, ChannelPromise promise) {
        promise = promise.unvoid();
        if (stream.isResetSent()) {
            // Don't write a RST_STREAM frame if we have already written one.
            return promise.setSuccess();
        }
        // Synchronously set the resetSent flag to prevent any subsequent calls
        // from resulting in multiple reset frames being sent.
        //
        // This needs to be done before we notify the promise as the promise may have a listener attached that
        // call resetStream(...) again.
        stream.resetSent();

        final ChannelFuture future;
        // If the remote peer is not aware of the steam, then we are not allowed to send a RST_STREAM
        // https://tools.ietf.org/html/rfc7540#section-6.4.
        if (stream.state() == GofStream.State.IDLE ||
                connection().local().created(stream) && !stream.isHeadersSent()) {
            future = promise.setSuccess();
        } else {
            future = frameWriter().writeRstStream(ctx, stream.id(), errorCode, promise);
        }
        if (future.isDone()) {
            processRstStreamWriteResult(ctx, stream, future);
        } else {
            future.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    processRstStreamWriteResult(ctx, stream, future);
                }
            });
        }

        return future;
    }

    /**
     * Close the remote endpoint with a {@code GO_AWAY} frame. Does <strong>not</strong> flush
     * immediately, this is the responsibility of the caller.
     */
    private ChannelFuture goAway(ChannelHandlerContext ctx, GofException cause, ChannelPromise promise) {
        long errorCode = cause != null ? cause.error().code() : NO_ERROR.code();
        int lastKnownStream;
        if (cause != null && cause.shutdownHint() == GofException.ShutdownHint.HARD_SHUTDOWN) {
            // The hard shutdown could have been triggered during header processing, before updating
            // lastStreamCreated(). Specifically, any connection errors encountered by Http2FrameReader or HPACK
            // decoding will fail to update the last known stream. So we must be pessimistic.
            // https://github.com/netty/netty/issues/10670
            lastKnownStream = Integer.MAX_VALUE;
        } else {
            lastKnownStream = connection().remote().lastStreamCreated();
        }
        return goAway(ctx, lastKnownStream, errorCode, GofException.toByteBuf(ctx, cause), promise);
    }

    private void processRstStreamWriteResult(ChannelHandlerContext ctx, GofStream stream, ChannelFuture future) {
        if (future.isSuccess()) {
            closeStream(stream, future);
        } else {
            // The connection will be closed and so no need to change the resetSent flag to false.
            onConnectionError(ctx, true, future.cause(), null);
        }
    }

    private void closeConnectionOnError(ChannelHandlerContext ctx, ChannelFuture future) {
        if (!future.isSuccess()) {
            onConnectionError(ctx, true, future.cause(), null);
        }
    }

    public ChannelFuture goAway(final ChannelHandlerContext ctx, final int lastStreamId, final long errorCode,
                                final ByteBuf debugData, ChannelPromise promise) {
        promise = promise.unvoid();
        final GofConnection connection = connection();
        try {
            if (!connection.goAwaySent(lastStreamId, errorCode, debugData)) {
                debugData.release();
                promise.trySuccess();
                return promise;
            }
        } catch (Throwable cause) {
            debugData.release();
            promise.tryFailure(cause);
            return promise;
        }

        // Need to retain before we write the buffer because if we do it after the refCnt could already be 0 and
        // result in an IllegalRefCountException.
        debugData.retain();
        ChannelFuture future = frameWriter().writeGoAway(ctx, lastStreamId, errorCode, debugData, promise);

        if (future.isDone()) {
            processGoAwayWriteResult(ctx, lastStreamId, errorCode, debugData, future);
        } else {
            future.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    processGoAwayWriteResult(ctx, lastStreamId, errorCode, debugData, future);
                }
            });
        }

        return future;
    }

    /**
     * Closes the connection if the graceful shutdown process has completed.
     * @param future Represents the status that will be passed to the {@link #closeListener}.
     */
    private void checkCloseConnection(ChannelFuture future) {
        // If this connection is closing and the graceful shutdown has completed, close the connection
        // once this operation completes.
        if (closeListener != null && isGracefulShutdownComplete()) {
            ChannelFutureListener closeListener = this.closeListener;
            // This method could be called multiple times
            // and we don't want to notify the closeListener multiple times.
            this.closeListener = null;
            try {
                closeListener.operationComplete(future);
            } catch (Exception e) {
                throw new IllegalStateException("Close listener threw an unexpected exception", e);
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        GofException embedded = GofUtils.getEmbeddedGofException(cause);
        if (embedded == null) {
            // There was no embedded embeddedception, assume it's a connection error. Subclasses are
            // responsible for storing the appropriate status and shutting down the connection.
            onError(ctx, /* outbound= */ false, cause);
        } else {
            super.exceptionCaught(ctx, cause);
        }
    }

    /**
     * Closes the local side of the given stream. If this causes the stream to be closed, adds a
     * hook to close the channel after the given future completes.
     *
     * @param stream the stream to be half closed.
     * @param future If closing, the future after which to close the channel.
     */
    @Override
    public void closeStreamLocal(GofStream stream, ChannelFuture future) {
        switch (stream.state()) {
//            case HALF_CLOSED_LOCAL:
            case OPEN:
                stream.closeLocalSide();
                break;
            default:
                closeStream(stream, future);
                break;
        }
    }

    /**
     * Closes the remote side of the given stream. If this causes the stream to be closed, adds a
     * hook to close the channel after the given future completes.
     *
     * @param stream the stream to be half closed.
     * @param future If closing, the future after which to close the channel.
     */
    @Override
    public void closeStreamRemote(GofStream stream, ChannelFuture future) {
        switch (stream.state()) {
            case OPEN:
                stream.closeRemoteSide();
                break;
            default:
                closeStream(stream, future);
                break;
        }
    }

    @Override
    public void closeStream(final GofStream stream, ChannelFuture future) {
        stream.close();

        if (future.isDone()) {
            checkCloseConnection(future);
        } else {
            future.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    checkCloseConnection(future);
                }
            });
        }
    }

    /**
     * Central handler for all exceptions caught during GOF processing.
     */
    public void onError(ChannelHandlerContext ctx, boolean outbound, Throwable cause) {
        GofException embedded = GofUtils.getEmbeddedGofException(cause);
        if (GofException.isStreamError(embedded)) {
            onStreamError(ctx, outbound, cause, (GofException.StreamException) embedded);
        } else {
            onConnectionError(ctx, outbound, cause, embedded);
        }
        ctx.flush();
    }

    /**
     * Called by the graceful shutdown logic to determine when it is safe to close the connection. Returns {@code true}
     * if the graceful shutdown has completed and the connection can be safely closed. This implementation just
     * guarantees that there are no active streams. Subclasses may override to provide additional checks.
     */
    protected boolean isGracefulShutdownComplete() {
        return connection().numActiveStreams() == 0;
    }

    protected void onConnectionError(ChannelHandlerContext ctx, boolean outbound, Throwable cause, GofException embedded) {
        if (embedded == null) {
            embedded = new GofException(INTERNAL_ERROR, cause.getMessage(), cause);
        }

        ChannelPromise promise = ctx.newPromise();
        ChannelFuture future = goAway(ctx, embedded, ctx.newPromise());
        if (embedded.shutdownHint() == GofException.ShutdownHint.GRACEFUL_SHUTDOWN) {
            doGracefulShutdown(ctx, future, promise);
        } else {
            future.addListener(newClosingChannelFutureListener(ctx, promise));
        }
    }

    protected void onStreamError(ChannelHandlerContext ctx, boolean outbound, Throwable cause, GofException.StreamException embedded) {
        final int streamId = embedded.streamId();
        GofStream stream = connection().stream(streamId);

        //if this is caused by reading headers that are too large, send a header with status 431
        if (stream == null) {
            if (!outbound || connection().local().mayHaveCreatedStream(streamId)) {
                resetUnknownStream(ctx, streamId, embedded.error().code(), ctx.newPromise());
            }
        } else {
            resetStream(ctx, stream, embedded.error().code(), ctx.newPromise());
        }
    }

    private static void processGoAwayWriteResult(
            final ChannelHandlerContext ctx,
            final int lastStreamId,
            final long errorCode,
            final ByteBuf debugData,
            ChannelFuture future
    ) {
        try {
            if (future.isSuccess()) {
                if (errorCode != NO_ERROR.code()) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("{} Sent GOAWAY: lastStreamId '{}', errorCode '{}', " +
                                        "debugData '{}'. Forcing shutdown of the connection.",
                                ctx.channel(), lastStreamId, errorCode, debugData.toString(UTF_8), future.cause());
                    }
                    ctx.close();
                }
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("{} Sending GOAWAY failed: lastStreamId '{}', errorCode '{}', " +
                                    "debugData '{}'. Forcing shutdown of the connection.",
                            ctx.channel(), lastStreamId, errorCode, debugData.toString(UTF_8), future.cause());
                }
                ctx.close();
            }
        } finally {
            // We're done with the debug data now.
            debugData.release();
        }
    }

    /**
     * Closes the channel when the future completes.
     */
    private static final class ClosingChannelFutureListener implements ChannelFutureListener {
        private final ChannelHandlerContext ctx;
        private final ChannelPromise promise;
        private final Future<?> timeoutTask;
        private boolean closed;

        ClosingChannelFutureListener(ChannelHandlerContext ctx, ChannelPromise promise) {
            this.ctx = ctx;
            this.promise = promise;
            timeoutTask = null;
        }

        ClosingChannelFutureListener(final ChannelHandlerContext ctx, final ChannelPromise promise,
                                     long timeout, TimeUnit unit) {
            this.ctx = ctx;
            this.promise = promise;
            timeoutTask = ctx.executor().schedule(new Runnable() {
                @Override
                public void run() {
                    doClose();
                }
            }, timeout, unit);
        }

        @Override
        public void operationComplete(ChannelFuture sentGoAwayFuture) {
            if (timeoutTask != null) {
                timeoutTask.cancel(false);
            }
            doClose();
        }

        private void doClose() {
            // We need to guard against multiple calls as the timeout may trigger close() first and then it will be
            // triggered again because of operationComplete(...) is called.
            if (closed) {
                // This only happens if we also scheduled a timeout task.
                assert timeoutTask != null;
                return;
            }
            closed = true;
            if (promise == null) {
                ctx.close();
            } else {
                ctx.close(promise);
            }
        }
    }
}
