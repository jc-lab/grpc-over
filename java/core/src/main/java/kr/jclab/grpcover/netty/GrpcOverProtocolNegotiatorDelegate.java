package kr.jclab.grpcover.netty;

import io.netty.channel.ChannelHandler;
import io.netty.util.AsciiString;

public interface GrpcOverProtocolNegotiatorDelegate {
    AsciiString scheme();
    ChannelHandler newHandler(ChannelHandler next);
    void close();
}
