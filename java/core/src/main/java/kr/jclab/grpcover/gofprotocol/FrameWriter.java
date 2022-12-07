package kr.jclab.grpcover.gofprotocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import kr.jclab.grpcover.core.protocol.v1.GofProto;

import java.io.Closeable;

public interface FrameWriter extends Closeable {
    /**
     * Writes a HEADERS frame to the remote endpoint.
     *
     * @param ctx the context to use for writing.
     * @param streamId the stream for which to send the frame.
     * @param headers the headers to be sent.
     * @param endStream indicates if this is the last frame to be sent for the stream.
     * @param promise the promise for the write.
     * @return the future for the write.
     * <a href="https://tools.ietf.org/html/rfc7540#section-10.5.1">Section 10.5.1</a> states the following:
     * <pre>
     * The header block MUST be processed to ensure a consistent connection state, unless the connection is closed.
     * </pre>
     * If this call has modified the HPACK header state you <strong>MUST</strong> throw a connection error.
     * <p>
     * If this call has <strong>NOT</strong> modified the HPACK header state you are free to throw a stream error.
     */
    ChannelFuture writeHeaders(ChannelHandlerContext ctx, int streamId, GofProto.Header headers,
                               boolean endStream, ChannelPromise promise);

    /**
     * Writes a RST_STREAM frame to the remote endpoint.
     *
     * @param ctx the context to use for writing.
     * @param streamId the stream for which to send the frame.
     * @param errorCode the error code indicating the nature of the failure.
     * @param promise the promise for the write.
     * @return the future for the write.
     */
    ChannelFuture writeRstStream(ChannelHandlerContext ctx, int streamId, long errorCode,
                                 ChannelPromise promise);

    /**
     * Writes a PING frame to the remote endpoint.
     *
     * @param ctx the context to use for writing.
     * @param ack indicates whether this is an ack of a PING frame previously received from the
     *            remote endpoint.
     * @param data the payload of the frame.
     * @param promise the promise for the write.
     * @return the future for the write.
     */
    ChannelFuture writePing(ChannelHandlerContext ctx, boolean ack, long data,
                            ChannelPromise promise);

    /**
     * Writes a GO_AWAY frame to the remote endpoint.
     *
     * @param ctx the context to use for writing.
     * @param lastStreamId the last known stream of this endpoint.
     * @param errorCode the error code, if the connection was abnormally terminated.
     * @param debugData application-defined debug data. This will be released by this method.
     * @param promise the promise for the write.
     * @return the future for the write.
     */
    ChannelFuture writeGoAway(ChannelHandlerContext ctx, int lastStreamId, long errorCode,
                              ByteBuf debugData, ChannelPromise promise);

    ChannelFuture writeData(
            ChannelHandlerContext ctx,
            int streamId,
            ByteBuf data,
            boolean endStream,
            ChannelPromise promise
    );

    /**
     * Closes this writer and frees any allocated resources.
     */
    @Override
    void close();
}
