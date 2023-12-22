package io.grpc.netty;

import io.netty.channel.ChannelHandler;
import io.netty.util.AsciiString;

public interface GrpcOverProtocolNegotiatorDelegate {
    default AsciiString scheme() {
        return new AsciiString("grpcover");
    }

    ChannelHandler newHandler(ChannelHandler next);

    default void close() {}
}
