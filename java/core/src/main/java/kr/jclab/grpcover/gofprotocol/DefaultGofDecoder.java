package kr.jclab.grpcover.gofprotocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import kr.jclab.grpcover.core.protocol.v1.GofProto;
import lombok.Getter;
import lombok.Setter;

import static io.grpc.internal.GrpcUtil.Http2Error.*;
import static io.grpc.internal.GrpcUtil.Http2Error.PROTOCOL_ERROR;
import static kr.jclab.grpcover.gofprotocol.GofException.connectionError;
import static kr.jclab.grpcover.gofprotocol.GofException.streamError;

public class DefaultGofDecoder {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(DefaultGofDecoder.class);

    private final FrameWriter frameWriter;
    private final GofConnection connection;

    @Setter
    FrameHandler frameHandler;
    @Setter
    GofLifecycleManager lifecycleManager;

    public DefaultGofDecoder(GofConnection connection, FrameWriter frameWriter) {
        this.frameWriter = frameWriter;
        this.connection = connection;
    }

    @Getter
    private final FrameListener frameListener = new FrameListener() {
        @Override
        public void onHeadersRead(ChannelHandlerContext ctx, int streamId, GofProto.Header header, boolean endOfStream) throws GofException {
            GofStream stream = connection.stream(streamId);
            if (stream == null && !connection.streamMayHaveExisted(streamId)) {
                stream = connection.remote().createStream(streamId);
            }
//        else if (stream != null) {
//            isTrailers = stream.isHeadersReceived();
//        }

            if (shouldIgnoreHeadersOrDataFrame(ctx, streamId, stream, "HEADERS")) {
                return;
            }

            switch (stream.state()) {
                case OPEN:
                    break;
                case CLOSED:
                    throw streamError(stream.id(), STREAM_CLOSED, "Stream %d in unexpected state: %s",
                            stream.id(), stream.state());
                default:
                    // Connection error.
                    throw connectionError(PROTOCOL_ERROR, "Stream %d in unexpected state: %s", stream.id(),
                            stream.state());
            }

            frameHandler.onHeadersRead(ctx, stream, header, endOfStream);

            if (endOfStream) {
                lifecycleManager.closeStreamRemote(stream, ctx.newSucceededFuture());
            }
        }

        @Override
        public void onDataRead(ChannelHandlerContext ctx, int streamId, ByteBuf data, boolean endOfStream) throws GofException {
            GofStream stream = connection.stream(streamId);

            final boolean shouldIgnore;
            try {
                shouldIgnore = shouldIgnoreHeadersOrDataFrame(ctx, streamId, stream, "DATA");
            } catch (GofException e) {
//            // Ignoring this frame. We still need to count the frame towards the connection flow control
//            // window, but we immediately mark all bytes as consumed.
//            flowController.receiveFlowControlledFrame(stream, data, padding, endOfStream);
//            flowController.consumeBytes(stream, bytesToReturn);
                throw e;
            } catch (Throwable t) {
                throw connectionError(INTERNAL_ERROR, t, "Unhandled error on data stream id %d", streamId);
            }

            if (shouldIgnore) {
                return ;
            }

            GofException error = null;
            switch (stream.state()) {
                case OPEN:
                    break;
                case CLOSED:
                    error = streamError(stream.id(), STREAM_CLOSED, "Stream %d in unexpected state: %s",
                            stream.id(), stream.state());
                    break;
                default:
                    error = streamError(stream.id(), PROTOCOL_ERROR,
                            "Stream %d in unexpected state: %s", stream.id(), stream.state());
                    break;
            }
            if (error != null) {
                throw error;
            }

            frameHandler.onDataRead(ctx, stream, data, endOfStream);
        }

        @Override
        public void onGoAwayRead(ChannelHandlerContext ctx, int lastStreamId, long errorCode, ByteBuf debugData) throws GofException {
            connection.goAwayReceived(lastStreamId, errorCode, debugData);
            frameHandler.onGoAwayRead(ctx, lastStreamId, errorCode, debugData);
        }

        @Override
        public void onRstStreamRead(ChannelHandlerContext ctx, int streamId, long errorCode) throws GofException {
            GofStream stream = connection.stream(streamId);
            if (stream == null) {
                verifyStreamMayHaveExisted(streamId);
                return;
            }

            switch(stream.state()) {
                case IDLE:
                    throw connectionError(PROTOCOL_ERROR, "RST_STREAM received for IDLE stream %d", streamId);
                case CLOSED:
                    return; // RST_STREAM frames must be ignored for closed streams.
                default:
                    break;
            }

            frameHandler.onRstStreamRead(ctx, stream, errorCode);

            lifecycleManager.closeStream(stream, ctx.newSucceededFuture());
        }

        @Override
        public void onPingRead(ChannelHandlerContext ctx, long data) throws GofException {
            frameWriter.writePing(ctx, true, data, ctx.newPromise());
            frameHandler.onPingRead(ctx, data);
        }

        @Override
        public void onPongRead(ChannelHandlerContext ctx, long data) throws GofException {
            frameHandler.onPongRead(ctx, data);
        }
    };

    private boolean shouldIgnoreHeadersOrDataFrame(ChannelHandlerContext ctx, int streamId, GofStream stream, String frameName) throws GofException {
        if (stream == null) {
            if (streamCreatedAfterGoAwaySent(streamId)) {
                logger.info("{} ignoring {} frame for stream {}. Stream sent after GOAWAY sent", ctx.channel(), frameName, streamId);
                return true;
            }

            // Make sure it's not an out-of-order frame, like a rogue DATA frame, for a stream that could
            // never have existed.
            verifyStreamMayHaveExisted(streamId);

            // Its possible that this frame would result in stream ID out of order creation (PROTOCOL ERROR) and its
            // also possible that this frame is received on a CLOSED stream (STREAM_CLOSED after a RST_STREAM is
            // sent). We don't have enough information to know for sure, so we choose the lesser of the two errors.
            throw streamError(streamId, STREAM_CLOSED, "Received frame for an unknown stream %d", streamId);
        }
        if (stream.isResetSent() || streamCreatedAfterGoAwaySent(streamId)) {
            // If we have sent a reset stream it is assumed the stream will be closed after the write completes.
            // If we have not sent a reset, but the stream was created after a GoAway this is not supported by
            // DefaultHttp2Connection and if a custom Http2Connection is used it is assumed the lifetime is managed
            // elsewhere so we don't close the stream or otherwise modify the stream's state.

            if (logger.isInfoEnabled()) {
                logger.info("{} ignoring {} frame for stream {}", ctx.channel(), frameName,
                        stream.isResetSent() ? "RST_STREAM sent." :
                                "Stream created after GOAWAY sent. Last known stream by peer " +
                                        connection.remote().lastStreamKnownByPeer());
            }

            return true;
        }
        return false;
    }

    /**
     * Helper method for determining whether or not to ignore inbound frames. A stream is considered to be created
     * after a {@code GOAWAY} is sent if the following conditions hold:
     * <p/>
     * <ul>
     *     <li>A {@code GOAWAY} must have been sent by the local endpoint</li>
     *     <li>The {@code streamId} must identify a legitimate stream id for the remote endpoint to be creating</li>
     *     <li>{@code streamId} is greater than the Last Known Stream ID which was sent by the local endpoint
     *     in the last {@code GOAWAY} frame</li>
     * </ul>
     * <p/>
     */
    private boolean streamCreatedAfterGoAwaySent(int streamId) {
        GofConnection.Endpoint remote = connection.remote();
        return connection.goAwaySent() && remote.isValidStreamId(streamId) &&
                streamId > remote.lastStreamKnownByPeer();
    }

    private void verifyStreamMayHaveExisted(int streamId) throws GofException {
        if (!connection.streamMayHaveExisted(streamId)) {
            throw connectionError(PROTOCOL_ERROR, "Stream %d does not exist", streamId);
        }
    }
}
