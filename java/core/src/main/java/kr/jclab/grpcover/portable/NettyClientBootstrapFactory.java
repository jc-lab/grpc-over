package kr.jclab.grpcover.portable;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.EventLoopGroup;

public interface NettyClientBootstrapFactory {
    EventLoopGroup getGroup();

    /**
     * If returns null, use {@link #bootstrap}
     */
    default Channel createChannel(ChannelHandler handler) {
        return null;
    }
    Bootstrap bootstrap();
    ChannelHandler channelInitializer();
}
