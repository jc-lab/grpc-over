package kr.jclab.grpcover.websocket;

import io.grpc.Attributes;
import io.grpc.SecurityLevel;
import io.grpc.internal.GrpcAttributes;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolHandler;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import kr.jclab.grpcover.GrpcNegotiationHandlerBase;

import java.net.URI;

import static io.grpc.internal.GrpcUtil.DEFAULT_MAX_MESSAGE_SIZE;

public class GrpcOverWebsocketClientNegotiationHandler extends GrpcNegotiationHandlerBase {
    private final URI uri;
    private final SslContext sslContext;

    private ContextWriteWebSocketClientHandshaker13 webSocketClientHandshaker;

    public GrpcOverWebsocketClientNegotiationHandler(ChannelHandler grpcHandler, URI uri, SslContext sslContext) {
        super(grpcHandler);
        this.uri = uri;
        this.sslContext = sslContext;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        ChannelPipeline pipeline = ctx.pipeline();

        String scheme = uri.getScheme().toLowerCase();
        SslHandler sslHandler = scheme.startsWith("wss") ? this.sslContext.newHandler(
                ctx.channel().alloc(),
                uri.getHost(),
                uri.getPort()
        ) : null;

        this.webSocketClientHandshaker = new ContextWriteWebSocketClientHandshaker13(
                uri,
                WebSocketVersion.V13,
                null,
                false,
                new DefaultHttpHeaders(),
                DEFAULT_MAX_MESSAGE_SIZE
        );

        super.handlerAdded(ctx);

        if (sslHandler != null) {
            pipeline.addFirst("sslHandler", sslHandler);
            pipeline.addAfter("sslHandler", "httpClientCodec", new HttpClientCodec());
        } else {
            pipeline.addFirst("httpClientCodec", new HttpClientCodec());
        }
        pipeline.addAfter("httpClientCodec", "httpObjectAggregator", new HttpObjectAggregator(DEFAULT_MAX_MESSAGE_SIZE));
        pipeline.addAfter("httpObjectAggregator", "websocketHandler", new WebSocketClientProtocolHandler(this.webSocketClientHandshaker, true));
        pipeline.addAfter("websocketHandler", "webSocketFrameByteBufHandler", new WebSocketFrameByteBufHandler());
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        this.webSocketClientHandshaker.handshakeWithContext(ctx, ctx.newPromise());
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof WebSocketClientProtocolHandler.ClientHandshakeStateEvent) {
            Attributes attrs = pne.getAttributes().toBuilder()
                    .set(GrpcAttributes.ATTR_SECURITY_LEVEL, SecurityLevel.NONE)
                    .build();

            ctx.pipeline().replace(ctx.name(), null, this.grpcHandler);
            this.fireProtocolNegotiationEvent(ctx, pne.withAttributes(attrs));
        }
        super.userEventTriggered(ctx, evt);
    }
}
