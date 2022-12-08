package kr.jclab.grpcover.websocket.internal;

import io.grpc.Attributes;
import io.grpc.SecurityLevel;
import io.grpc.internal.GrpcAttributes;
import io.netty.channel.*;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.CharsetUtil;
import kr.jclab.grpcover.netty.ProtocolNegotiationEvent;

import java.util.logging.Level;
import java.util.logging.Logger;

public class WebSocketClientHandler extends WebSocketNegotiationHandler {
    private static final Logger logger = Logger.getLogger(WebSocketClientHandler.class.getName());
    private final SslHandler sslHandler;
    private final WebSocketClientHandshaker handshaker;
    private ChannelPromise handshakeFuture;
    private final ChannelHandler nextHandler;

    public WebSocketClientHandler(
            SslHandler sslHandler,
            WebSocketClientHandshaker handshaker,
            ChannelHandler nextHandler
    ) {
        super(sslHandler);
        this.sslHandler = sslHandler;
        this.handshaker = handshaker;
        this.nextHandler = nextHandler;
    }

    public ChannelFuture handshakeFuture() {
        return handshakeFuture;
    }

    @Override
    public void handlerAdded(final ChannelHandlerContext ctx) throws Exception {
        handshakeFuture = ctx.newPromise();
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        handshaker.handshake(ctx.channel());
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
        //System.out.println("WebSocket Client disconnected!");
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        final Channel ch = ctx.channel();
        if (!handshaker.isHandshakeComplete()) {
            // web socket client connected
            handshaker.finishHandshake(ch, (FullHttpResponse) msg);
            handshakeFuture.setSuccess();

            fireProtocolNegotiationEvent(ctx);
            return;
        }

        if (msg instanceof FullHttpResponse) {
            final FullHttpResponse response = (FullHttpResponse) msg;
            throw new Exception("Unexpected FullHttpResponse (getStatus=" + response.status() + ", content="
                    + response.content().toString(CharsetUtil.UTF_8) + ')');
        }

        super.channelRead(ctx, msg);
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
        if (!handshakeFuture.isDone()) {
            handshakeFuture.setFailure(cause);
        }
        logger.log(Level.WARNING, "exception", cause);
        ctx.close();
    }
}
