package kr.jclab.grpcover.netty.test;

import io.grpc.*;
import io.grpc.internal.GrpcAttributes;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import kr.jclab.grpcover.netty.ProtocolNegotiationEvent;

import javax.net.ssl.SSLSession;
import java.nio.channels.ClosedChannelException;

public class TestNegotiationHandler extends ChannelDuplexHandler {
    private ProtocolNegotiationEvent pne = ProtocolNegotiationEvent.DEFAULT;
    private final SslContext sslContext;

    public TestNegotiationHandler(SslContext sslContext) {
        this.sslContext = sslContext;
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof ProtocolNegotiationEvent) {
            pne = (ProtocolNegotiationEvent) evt;

            if (sslContext == null) {
                // only once
                fireProtocolNegotiationEvent(ctx);
            }
        } else if (evt instanceof SslHandshakeCompletionEvent) {
            SslHandshakeCompletionEvent handshakeEvent = (SslHandshakeCompletionEvent) evt;
            if (handshakeEvent.isSuccess()) {
                SslHandler handler = ctx.pipeline().get(SslHandler.class);
                propagateTlsComplete(ctx, handler.engine().getSession());
            } else {
                Throwable t = handshakeEvent.cause();
                if (t instanceof ClosedChannelException) {
                    // On channelInactive(), SslHandler creates its own ClosedChannelException and
                    // propagates it before the actual channelInactive(). So we assume here that any
                    // such exception is from channelInactive() and emulate the normal behavior of
                    // WriteBufferingAndExceptionHandler
                    t = Status.UNAVAILABLE
                            .withDescription("Connection closed while performing TLS negotiation")
                            .withCause(t)
                            .asRuntimeException();
                }
                ctx.fireExceptionCaught(t);
            }
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }

    private void fireProtocolNegotiationEvent(ChannelHandlerContext ctx) {
        Attributes attrs = pne.getAttributes().toBuilder()
                .set(GrpcAttributes.ATTR_SECURITY_LEVEL, SecurityLevel.PRIVACY_AND_INTEGRITY)
//                                .set(Grpc.TRANSPORT_ATTR_SSL_SESSION, session)
                .build();
        ctx.fireUserEventTriggered(pne.withAttributes(attrs));
    }

    private void propagateTlsComplete(ChannelHandlerContext ctx, SSLSession session) {
        InternalChannelz.Security security = new InternalChannelz.Security(new InternalChannelz.Tls(session));
        Attributes attrs = pne.getAttributes().toBuilder()
                .set(GrpcAttributes.ATTR_SECURITY_LEVEL, SecurityLevel.PRIVACY_AND_INTEGRITY)
                .set(Grpc.TRANSPORT_ATTR_SSL_SESSION, session)
                .build();
        ctx.fireUserEventTriggered(pne.withAttributes(attrs).withSecurity(security));
    }
}
