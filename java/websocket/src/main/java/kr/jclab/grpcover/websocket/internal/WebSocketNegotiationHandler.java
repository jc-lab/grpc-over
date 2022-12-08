package kr.jclab.grpcover.websocket.internal;

import io.grpc.Attributes;
import io.grpc.SecurityLevel;
import io.grpc.internal.GrpcAttributes;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.ContinuationWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.ssl.SslHandler;
import kr.jclab.grpcover.netty.ProtocolNegotiationEvent;
import org.jetbrains.annotations.NotNull;

public class WebSocketNegotiationHandler extends ChannelDuplexHandler {
    private final SslHandler sslHandler;

    private ProtocolNegotiationEvent pne = ProtocolNegotiationEvent.DEFAULT;

    public WebSocketNegotiationHandler(SslHandler sslHandler) {
        this.sslHandler = sslHandler;
    }

    @Override
    public final void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof ProtocolNegotiationEvent) {
            pne = (ProtocolNegotiationEvent) evt;
        } else if (evt instanceof WebSocketServerProtocolHandler.HandshakeComplete) {
            fireProtocolNegotiationEvent(ctx);
        } else {
            userEventTriggered0(ctx, evt);
        }
    }

    protected void userEventTriggered0(ChannelHandlerContext ctx, Object evt) throws Exception {
        super.userEventTriggered(ctx, evt);
    }

    protected void fireProtocolNegotiationEvent(ChannelHandlerContext ctx) {
        Attributes.Builder builder = pne.getAttributes().toBuilder();
        builder.set(GrpcAttributes.ATTR_SECURITY_LEVEL, (this.sslHandler != null) ? SecurityLevel.PRIVACY_AND_INTEGRITY : SecurityLevel.NONE);
        ctx.fireUserEventTriggered(pne.withAttributes(builder.build()));
    }
}
