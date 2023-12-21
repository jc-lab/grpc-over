package io.grpc.netty;

import io.grpc.ChannelLogger;
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
        ChannelHandler grpcNegotiationHandler = new ProtocolNegotiators.GrpcNegotiationHandler(grpcHandler);
        ChannelLogger negotiationLogger = grpcHandler.getNegotiationLogger();
        return new ProtocolNegotiators.WaitUntilActiveHandler(this.delegate.newHandler(grpcNegotiationHandler), negotiationLogger);
    }

    @Override
    public void close() {
        this.delegate.close();
    }

    public void attachToNettyServerBuilder(NettyServerBuilder builder) {
        builder.protocolNegotiator(this);
    }

    public static class ClientFactory implements ProtocolNegotiator.ClientFactory {
        private final GrpcOverProtocolNegotiatorDelegate delegate;

        public ClientFactory(GrpcOverProtocolNegotiatorDelegate delegate) {
            this.delegate = delegate;
        }

        @Override
        public ProtocolNegotiator newNegotiator() {
            return new GrpcOverProtocolNegotiator(this.delegate);
        }

        @Override
        public int getDefaultPort() {
            return 0;
        }
    }
}
