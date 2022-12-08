package kr.jclab.grpcover.websocket;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslContext;
import kr.jclab.grpcover.netty.NettyServerBuilder;
import kr.jclab.grpcover.websocket.internal.GrpcOverWebsocketServerChannelInitializer;

public class GrpcOverWebsocketServerBuilder {
    public static NettyServerBuilder forPort(EventLoopGroup boss, EventLoopGroup worker, int port, String address, String path, SslContext sslContext) {
        ServerBootstrap b = new ServerBootstrap();
        b.group(boss);
        b.channel(NioServerSocketChannel.class);
        if (address != null) {
            b.localAddress(address, port);
        } else {
            b.localAddress(port);
        }
        return forServerBootstrap(worker, b, path, sslContext);
    }

    public static NettyServerBuilder forServerBootstrap(EventLoopGroup workerEventLoopGroup, ServerBootstrap b, String path, SslContext sslContext) {
        GrpcOverWebsocketServerChannelInitializer initializer = new GrpcOverWebsocketServerChannelInitializer(path, sslContext);
        b.childHandler(initializer);
        Channel channel = b.bind().channel();
        return NettyServerBuilder.forInitializer(workerEventLoopGroup, channel, initializer);
    }
}
