package kr.jclab.grpcover.websocket.internal;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelHandler;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import kr.jclab.grpcover.portable.NettyClientBootstrapFactory;
import org.jetbrains.annotations.Nullable;

public class GrpcOverWebsocketClientBootstrapFactory implements NettyClientBootstrapFactory {
    private final EventLoopGroup workerEventLoopGroup;
    private final GrpcOverWebsocketClientChannelInitializer initializer;

    public GrpcOverWebsocketClientBootstrapFactory(EventLoopGroup workerEventLoopGroup, @Nullable SslContext sslContext) {
        this.workerEventLoopGroup = workerEventLoopGroup;
        this.initializer = new GrpcOverWebsocketClientChannelInitializer(sslContext);
    }

    @Override
    public EventLoopGroup getGroup() {
        return this.workerEventLoopGroup;
    }

    @Override
    public Bootstrap bootstrap() {
        return new Bootstrap()
                .group(workerEventLoopGroup)
                .channel(NioSocketChannel.class);
    }

    @Override
    public ChannelHandler channelInitializer() {
        return this.initializer;
    }
}
