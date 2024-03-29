package kr.jclab.grpcover.websocket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketCloseStatus;

public class WebSocketFrameByteBufHandler extends ChannelDuplexHandler {
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof ByteBuf) {
            if (msg == Unpooled.EMPTY_BUFFER) {
                promise.setSuccess();
                return;
            }
            super.write(ctx, new BinaryWebSocketFrame((ByteBuf) msg), promise);
        } else {
            super.write(ctx, msg, promise);
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof BinaryWebSocketFrame) {
            BinaryWebSocketFrame frame = (BinaryWebSocketFrame) msg;
            super.channelRead(ctx, frame.content());
        } else if (msg instanceof CloseWebSocketFrame) {
            CloseWebSocketFrame frame = (CloseWebSocketFrame) msg;
            if (frame.statusCode() != WebSocketCloseStatus.NORMAL_CLOSURE.code()) {
                ctx.fireExceptionCaught(new WebSocketCloseException(frame.statusCode(), frame.reasonText()));
            }
            ctx.close();
        } else {
            super.channelRead(ctx, msg);
        }
    }
}
