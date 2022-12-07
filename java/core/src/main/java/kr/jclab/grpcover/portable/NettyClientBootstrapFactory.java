package kr.jclab.grpcover.portable;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelHandler;
import io.netty.channel.EventLoopGroup;

public interface NettyClientBootstrapFactory {
    EventLoopGroup getGroup();
    Bootstrap bootstrap();
    ChannelHandler channelInitializer();
}
