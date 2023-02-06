package kr.jclab.grpcover.websocket.internal;

import io.netty.channel.*;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import kr.jclab.grpcover.netty.NettyChannelBuilder;

import java.net.URI;

public class GrpcOverWebsocketClientChannelInitializer extends ChannelInitializer<Channel> {
    private final SslContext sslContext;

    public GrpcOverWebsocketClientChannelInitializer(
            SslContext sslContext
    ) {
        this.sslContext = sslContext;
    }

    @Override
    protected void initChannel(Channel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();
        ChannelHandler grpcChannelHandler = ch.attr(NettyChannelBuilder.GRPC_CHANNEL_HANDLER).get();
        String target = ch.attr(NettyChannelBuilder.GRPC_TARGET).get();
        URI uri = URI.create(target);
        String scheme = uri.getScheme().toLowerCase();
        SslHandler sslHandler = scheme.startsWith("wss") ? this.sslContext.newHandler(
                ch.alloc(),
                uri.getHost(),
                uri.getPort()
        ) : null;

        WebSocketClientHandler websocketHandler = new WebSocketClientHandler(
                sslHandler,
                WebSocketClientHandshakerFactory.newHandshaker(
                        uri,
                        WebSocketVersion.V13,
                        null,
                        false,
                        new DefaultHttpHeaders()
                )
        );

        if (sslHandler != null) {
            pipeline.addFirst("sslHandler", sslHandler);
            pipeline.addAfter("sslHandler", "httpClientCodec", new HttpClientCodec());
        } else {
            pipeline.addFirst("httpClientCodec", new HttpClientCodec());
        }
        pipeline.addAfter("httpClientCodec", "httpObjectAggregator", new HttpObjectAggregator(65536));
        pipeline.addAfter("httpObjectAggregator", "websocketHandler", websocketHandler);
        pipeline.addAfter("websocketHandler", "gofFrameHandler", new GofFrameHandler());
        pipeline.addAfter("gofFrameHandler", "grpcChannelHandler", grpcChannelHandler);
    }
}
