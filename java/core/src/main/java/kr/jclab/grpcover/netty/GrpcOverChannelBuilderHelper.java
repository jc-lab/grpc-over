package kr.jclab.grpcover.netty;

import io.grpc.CallCredentials;
import io.grpc.ChannelCredentials;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;

public class GrpcOverChannelBuilderHelper {
    public static NettyChannelBuilder newNettyChannelBuilder(SocketAddress address, ChannelCredentials channelCreds, CallCredentials callCreds, ProtocolNegotiator.ClientFactory negotiator) {
        return new NettyChannelBuilder(address, channelCreds, callCreds, negotiator);
    }

    public static NettyChannelBuilder newNettyChannelBuilder(String target, ChannelCredentials channelCreds, CallCredentials callCreds, ProtocolNegotiator.ClientFactory negotiator) {
        URI uri = URI.create(target);

        SocketAddress address = new InetSocketAddress(uri.getHost(), uri.getPort());

        return newNettyChannelBuilder(address, channelCreds, callCreds, negotiator);
    }
}
