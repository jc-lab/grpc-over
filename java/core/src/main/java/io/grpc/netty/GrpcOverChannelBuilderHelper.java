package io.grpc.netty;

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
        int port = uri.getPort();
        if (port < 0) {
            port = getDefaultPort(uri.getScheme());
        }

        SocketAddress address = new InetSocketAddress(uri.getHost(), port);

        return newNettyChannelBuilder(address, channelCreds, callCreds, negotiator);
    }

    public static int getDefaultPort(String scheme) {
        scheme = scheme.toLowerCase();
        if (scheme.startsWith("wss") || scheme.startsWith("https")) {
            return 443;
        } else if (scheme.startsWith("ws") || scheme.startsWith("http")) {
            return 80;
        } else {
            return -1;
        }
    }
}
