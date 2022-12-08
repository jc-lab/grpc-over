package kr.jclab.grpcover.portable;

import kr.jclab.grpcover.netty.ServerChannelSetupHandler;

public interface GofChannelInitializer {
    void attachGofServerChannelSetupHandler(ServerChannelSetupHandler serverChannelSetupHandler);
}
