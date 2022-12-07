package kr.jclab.grpcover.gofprotocol;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import kr.jclab.grpcover.core.protocol.v1.GofProto;

import java.util.HashSet;

public class FrameParser {
    public static void handle(
            ChannelHandlerContext ctx,
            GofProto.Frame frame,
            FrameListener listener
    ) throws GofException {
        HashSet<GofProto.FrameFlags> flags = new HashSet<>(frame.getFlagList());
        boolean endOfStream = flags.contains(GofProto.FrameFlags.END_STREAM);
        switch (frame.getType()) {
            case HEADER:
                listener.onHeadersRead(ctx, frame.getStreamId(), frame.getHeader(), endOfStream);
                break;
            case DATA:
                listener.onDataRead(ctx, frame.getStreamId(), Unpooled.copiedBuffer(frame.getData().toByteArray()), endOfStream);
                break;
            case PING:
                listener.onPingRead(ctx, frame.getPingData());
                break;
            case PONG:
                listener.onPongRead(ctx, frame.getPingData());
                break;
            case GO_AWAY:
                listener.onGoAwayRead(ctx, frame.getStreamId(), frame.getErrorCode(), Unpooled.copiedBuffer(frame.getData().toByteArray()));
                break;
            case RST:
                listener.onRstStreamRead(ctx, frame.getStreamId(), frame.getErrorCode());
                break;
        }
    }
}
