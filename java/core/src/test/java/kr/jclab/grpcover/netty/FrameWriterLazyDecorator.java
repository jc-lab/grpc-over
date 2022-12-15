package kr.jclab.grpcover.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import kr.jclab.grpcover.core.protocol.v1.GofProto;
import kr.jclab.grpcover.gofprotocol.FrameWriter;
import lombok.Setter;

public class FrameWriterLazyDecorator implements FrameWriter {
    @Setter
    private FrameWriter delegate;

    @Override
    public ChannelFuture writeHeaders(ChannelHandlerContext ctx, int streamId, GofProto.Header headers, boolean endStream, ChannelPromise promise) {
        return this.delegate.writeHeaders(ctx, streamId, headers, endStream, promise);
    }

    @Override
    public ChannelFuture writeRstStream(ChannelHandlerContext ctx, int streamId, long errorCode, ChannelPromise promise) {
        return this.delegate.writeRstStream(ctx, streamId, errorCode, promise);
    }

    @Override
    public ChannelFuture writePing(ChannelHandlerContext ctx, boolean ack, long data, ChannelPromise promise) {
        return this.delegate.writePing(ctx, ack, data, promise);
    }

    @Override
    public ChannelFuture writeGoAway(ChannelHandlerContext ctx, int lastStreamId, long errorCode, ByteBuf debugData, ChannelPromise promise) {
        return this.delegate.writeGoAway(ctx, lastStreamId, errorCode, debugData, promise);
    }

    @Override
    public ChannelFuture writeData(ChannelHandlerContext ctx, int streamId, ByteBuf data, boolean endStream, ChannelPromise promise) {
        return this.delegate.writeData(ctx, streamId, data, endStream, promise);
    }

    @Override
    public void close() {
        this.delegate.close();
    }
}

