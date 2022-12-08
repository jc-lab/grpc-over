package kr.jclab.grpcover.netty;

import io.netty.channel.Channel;

@FunctionalInterface
public interface ServerChannelSetupHandler {
    void setup(Channel channel);
}
