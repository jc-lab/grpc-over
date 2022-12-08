package kr.jclab.grpcover.websocket.internal;

import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import kr.jclab.grpcover.netty.ServerChannelSetupHandler;
import kr.jclab.grpcover.portable.GofChannelInitializer;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;

public class GrpcOverWebsocketServerChannelInitializer extends ChannelInitializer<Channel> implements GofChannelInitializer {
    private final CompletableFuture<ServerChannelSetupHandler> serverChannelSetupHandlerPromise = new CompletableFuture<>();
    private final String path;
    private final SslContext sslContext;

    public GrpcOverWebsocketServerChannelInitializer(
            String path,
            SslContext sslContext
    ) {
        this.path = path;
        this.sslContext = sslContext;
    }

    @Override
    public void attachGofServerChannelSetupHandler(
            ServerChannelSetupHandler serverChannelSetupHandler
    ) {
        serverChannelSetupHandlerPromise.complete(serverChannelSetupHandler);
    }

    @Override
    protected void initChannel(@NotNull Channel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();

        SslHandler sslHandler = (sslContext != null) ? sslContext.newHandler(ch.alloc()) : null;
        pipeline.addLast(
                new HttpRequestDecoder(),
                new HttpObjectAggregator(65536),
                new HttpResponseEncoder(),
                new WebSocketServerProtocolHandler(path),
                new WebSocketNegotiationHandler(sslHandler),
                new GofFrameHandler()
        );

        serverChannelSetupHandlerPromise.get().setup(ch);
    }
}
