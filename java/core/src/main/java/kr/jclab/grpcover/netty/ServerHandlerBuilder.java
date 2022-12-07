package kr.jclab.grpcover.netty;

import io.netty.channel.Channel;

public interface ServerHandlerBuilder {
    void build(Channel channel);
}
