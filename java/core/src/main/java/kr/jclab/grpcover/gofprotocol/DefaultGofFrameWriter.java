package kr.jclab.grpcover.gofprotocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import kr.jclab.grpcover.core.protocol.v1.GofProto;
import kr.jclab.grpcover.netty.NettyGofFrameWriter;

public class DefaultGofFrameWriter implements FrameWriter {
    private final GofConnection gofConnection;
    private final FrameWriter frameWriter = new NettyGofFrameWriter();;

    public DefaultGofFrameWriter(GofConnection gofConnection) {
        this.gofConnection = gofConnection;
    }

    @Override
    public ChannelFuture writeHeaders(ChannelHandlerContext ctx, int streamId, GofProto.Header headers, boolean endStream, ChannelPromise promise) {
        GofStream stream = gofConnection.stream(streamId);
        if (stream != null) {
            stream.headersSent(false);
        }
        return frameWriter.writeHeaders(ctx, streamId, headers, endStream, promise);
    }

    @Override
    public ChannelFuture writeRstStream(ChannelHandlerContext ctx, int streamId, long errorCode, ChannelPromise promise) {
        return frameWriter.writeRstStream(ctx, streamId, errorCode, promise);
    }

    @Override
    public ChannelFuture writePing(ChannelHandlerContext ctx, boolean ack, long data, ChannelPromise promise) {
        return frameWriter.writePing(ctx, ack, data, promise);
    }

    @Override
    public ChannelFuture writeGoAway(ChannelHandlerContext ctx, int lastStreamId, long errorCode, ByteBuf debugData, ChannelPromise promise) {
        return frameWriter.writeGoAway(ctx, lastStreamId, errorCode, debugData, promise);
    }

    @Override
    public ChannelFuture writeData(ChannelHandlerContext ctx, int streamId, ByteBuf data, boolean endStream, ChannelPromise promise) {
        return frameWriter.writeData(ctx, streamId, data, endStream, promise);
    }

    @Override
    public void close() {

    }
}
