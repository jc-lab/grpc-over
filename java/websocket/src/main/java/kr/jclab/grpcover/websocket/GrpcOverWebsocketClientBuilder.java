package kr.jclab.grpcover.websocket;

import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;
import kr.jclab.grpcover.netty.NettyChannelBuilder;
import kr.jclab.grpcover.websocket.internal.GrpcOverWebsocketClientBootstrapFactory;

public class GrpcOverWebsocketClientBuilder {
    public static NettyChannelBuilder forTarget(
            EventLoopGroup workerEventLoopGroup,
            String target
    ) {
        GrpcOverWebsocketClientBootstrapFactory bootstrapFactory = new GrpcOverWebsocketClientBootstrapFactory(
                workerEventLoopGroup,
                null
        );
        return NettyChannelBuilder.forTarget(bootstrapFactory, target);
    }

    public static NettyChannelBuilder forSslTarget(
            EventLoopGroup workerEventLoopGroup,
            String target,
            SslContext sslContext
    ) {
        GrpcOverWebsocketClientBootstrapFactory bootstrapFactory = new GrpcOverWebsocketClientBootstrapFactory(
                workerEventLoopGroup,
                sslContext
        );
        return NettyChannelBuilder.forTarget(bootstrapFactory, target);
    }
}
