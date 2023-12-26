package kr.jclab.grpcover.websocket;

import io.grpc.Attributes;
import io.grpc.SecurityLevel;
import io.grpc.internal.GrpcAttributes;
import io.grpc.netty.GrpcOverChannelBuilderHelper;
import io.grpc.netty.GrpcOverProtocolNegotiationEventAccessor;
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
        int port = uri.getPort();
        if (port < 0) {
            port = GrpcOverChannelBuilderHelper.getDefaultPort(uri.getScheme());
        }
        SslHandler sslHandler = scheme.startsWith("wss") ? this.sslContext.newHandler(
                ctx.channel().alloc(),
                uri.getHost(),
                port
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
        pipeline.addAfter("httpClientCodec", "httpObjectAggregator", new HttpObjectAggregator(8192)); // it is not websocket size
        pipeline.addAfter("httpObjectAggregator", "websocketHandler", new WebSocketClientProtocolHandler(this.webSocketClientHandshaker, false));
        pipeline.addAfter("websocketHandler", "webSocketFrameByteBufHandler", new WebSocketFrameByteBufHandler());
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        this.webSocketClientHandshaker.handshakeWithContext(ctx, ctx.newPromise());
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        System.out.println("channelRead: " + msg.getClass());
        super.channelRead(ctx, msg);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof WebSocketClientProtocolHandler.ClientHandshakeStateEvent) {
            Attributes attrs = GrpcOverProtocolNegotiationEventAccessor.getAttributes(pne).toBuilder()
                    .set(GrpcAttributes.ATTR_SECURITY_LEVEL, SecurityLevel.NONE)
                    .build();

            ctx.pipeline().replace(ctx.name(), null, this.grpcHandler);
            this.fireProtocolNegotiationEvent(ctx, GrpcOverProtocolNegotiationEventAccessor.withAttributes(pne, attrs));
        } else {
            System.out.println("event: " + evt.getClass());
        }
        super.userEventTriggered(ctx, evt);
    }
}
