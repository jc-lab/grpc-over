package kr.jclab.grpcover.netty;

import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import kr.jclab.grpcover.core.protocol.v1.GofProto;
import kr.jclab.grpcover.gofprotocol.FrameWriter;

public class NettyGofFrameWriter implements FrameWriter {
    @Override
    public ChannelFuture writeHeaders(ChannelHandlerContext ctx, int streamId, GofProto.Header headers, boolean endStream, ChannelPromise promise) {
        GofProto.Frame.Builder builder = GofProto.Frame.newBuilder()
                .setStreamId(streamId)
                .setType(GofProto.FrameType.HEADER)
                .setHeader(headers);
        if (endStream) {
            builder.addFlag(GofProto.FrameFlags.END_STREAM);
        }
        return ctx.write(builder.build(), promise);
    }

    @Override
    public ChannelFuture writeRstStream(ChannelHandlerContext ctx, int streamId, long errorCode, ChannelPromise promise) {
        GofProto.Frame.Builder builder = GofProto.Frame.newBuilder()
                .setStreamId(streamId)
                .setType(GofProto.FrameType.RST)
                .setErrorCode(errorCode);
        return ctx.write(builder.build(), promise);
    }

    @Override
    public ChannelFuture writePing(ChannelHandlerContext ctx, boolean ack, long data, ChannelPromise promise) {
        GofProto.Frame.Builder builder = GofProto.Frame.newBuilder()
                .setType(ack ? GofProto.FrameType.PONG : GofProto.FrameType.PING)
                .setPingData(data);
        return ctx.write(builder.build(), promise);
    }

    @Override
    public ChannelFuture writeGoAway(ChannelHandlerContext ctx, int lastStreamId, long errorCode, ByteBuf debugData, ChannelPromise promise) {
        GofProto.Frame.Builder builder = GofProto.Frame.newBuilder()
                .setType(GofProto.FrameType.GO_AWAY)
                .setStreamId(lastStreamId)
                .setErrorCode(errorCode);
        if (debugData != null && debugData.readableBytes() > 0) {
            builder.setData(ByteString.copyFrom(debugData.nioBuffer()));
        }
        return ctx.write(builder.build(), promise);
    }

    @Override
    public ChannelFuture writeData(ChannelHandlerContext ctx, int streamId, ByteBuf data, boolean endStream, ChannelPromise promise) {
        GofProto.Frame.Builder builder = GofProto.Frame.newBuilder()
                .setStreamId(streamId)
                .setType(GofProto.FrameType.DATA);
        if (endStream) {
            builder.addFlag(GofProto.FrameFlags.END_STREAM);
        }
        if (data != null && data.readableBytes() > 0) {
            builder.setData(ByteString.copyFrom(data.nioBuffer()));
        }
        return ctx.write(builder.build(), promise);
    }

    @Override
    public void close() {

    }
}
