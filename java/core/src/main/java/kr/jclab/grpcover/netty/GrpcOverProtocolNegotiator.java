package kr.jclab.grpcover.netty;

import io.netty.channel.ChannelHandler;
import io.netty.util.AsciiString;

public class GrpcOverProtocolNegotiator implements ProtocolNegotiator {
    private final GrpcOverProtocolNegotiatorDelegate delegate;

    public GrpcOverProtocolNegotiator(GrpcOverProtocolNegotiatorDelegate delegate) {
        this.delegate = delegate;
    }

    @Override
    public AsciiString scheme() {
        return this.delegate.scheme();
    }

    @Override
    public ChannelHandler newHandler(GrpcHttp2ConnectionHandler grpcHandler) {
        return this.delegate.newHandler(grpcHandler);
    }

    @Override
    public void close() {
        this.delegate.close();
    }
}
