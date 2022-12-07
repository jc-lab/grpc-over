package kr.jclab.grpcover.netty.test;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.nio.NioSocketChannel;
import kr.jclab.grpcover.portable.NettyClientBootstrapFactory;

public class SimpleClientBootstrapFactory implements NettyClientBootstrapFactory {
    private final EventLoopGroup group;
    private final GrpcClientChannelInitializer initializer = new GrpcClientChannelInitializer();

    public SimpleClientBootstrapFactory(EventLoopGroup group) {
        this.group = group;
    }

    @Override
    public EventLoopGroup getGroup() {
        return this.group;
    }

    @Override
    public Bootstrap bootstrap() {
        Bootstrap b = new Bootstrap();
        b.group(group);
        b.channel(NioSocketChannel.class);
        b.handler(initializer);
        return b;
    }

    @Override
    public ChannelHandler channelInitializer() {
        return initializer;
    }
}
