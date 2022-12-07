package kr.jclab.grpcover.portable;

import kr.jclab.grpcover.netty.ServerHandlerBuilder;

public interface GofChannelInitializer {
    void attachGofServerHandlerBuilder(ServerHandlerBuilder serverHandlerBuilder);
}
