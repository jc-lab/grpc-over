package kr.jclab.grpcover.websocket.internal;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import kr.jclab.grpcover.core.protocol.v1.GofProto;
import org.jetbrains.annotations.NotNull;

@ChannelHandler.Sharable
public class GofFrameHandler extends ChannelDuplexHandler {
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof GofProto.Frame) {
            ctx.write(
                    new BinaryWebSocketFrame(Unpooled.wrappedBuffer(((GofProto.Frame) msg).toByteArray())),
                    promise
            );
            return ;
        }

        super.write(ctx, msg, promise);
    }

    @Override
    public void channelRead(@NotNull ChannelHandlerContext ctx, @NotNull Object msg) throws Exception {
        if (msg instanceof BinaryWebSocketFrame) {
            BinaryWebSocketFrame frame = (BinaryWebSocketFrame) msg;
            ctx.fireChannelRead(GofProto.Frame.parseFrom(frame.content().nioBuffer()));
        }
    }
}
