package kr.jclab.grpcover.gofprotocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import kr.jclab.grpcover.core.protocol.v1.GofProto;

public interface FrameHandler {
    void onHeadersRead(
            ChannelHandlerContext ctx,
            GofStream stream,
            GofProto.Header header,
            boolean endStream
    ) throws GofException;

    void onDataRead(
            ChannelHandlerContext ctx,
            GofStream stream,
            ByteBuf data,
            boolean endOfStream
    ) throws GofException;

    void onGoAwayRead(ChannelHandlerContext ctx, int lastStreamId, long errorCode, ByteBuf debugData) throws GofException;

    void onRstStreamRead(ChannelHandlerContext ctx, GofStream stream, long errorCode) throws GofException;

    void onPingRead(ChannelHandlerContext ctx, long data) throws GofException;

    void onPongRead(ChannelHandlerContext ctx, long data) throws GofException;
}
