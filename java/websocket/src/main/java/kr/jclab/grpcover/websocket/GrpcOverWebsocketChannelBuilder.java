package kr.jclab.grpcover.websocket;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.grpc.netty.*;
import io.netty.channel.ChannelHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.util.AsciiString;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;

import static com.google.common.base.Preconditions.checkArgument;

public class GrpcOverWebsocketChannelBuilder extends GrpcOverNettyChannelBuilder {
    private URI uri;
    private SslContext sslContext;
    private GrpcOverProtocolNegotiatorDelegate additionalNegotiator;

    private final GrpcOverProtocolNegotiator.ClientFactory clientFactory = new GrpcOverProtocolNegotiator.ClientFactory(new GrpcOverProtocolNegotiatorDelegate() {
        @Override
        public AsciiString scheme() {
            if (additionalNegotiator != null) {
                return additionalNegotiator.scheme();
            }
            return GrpcOverProtocolNegotiatorDelegate.super.scheme();
        }

        @Override
        public void close() {
            if (additionalNegotiator != null) {
                additionalNegotiator.close();
            }
            GrpcOverProtocolNegotiatorDelegate.super.close();
        }

        @Override
        public ChannelHandler newHandler(ChannelHandler next) {
            if (additionalNegotiator != null) {
                next = additionalNegotiator.newHandler(next);
            }
            return new GrpcOverWebsocketClientNegotiationHandler(next, uri, sslContext);
        }
    });

    private GrpcOverWebsocketChannelBuilder(URI uri, SocketAddress address) {
        super(address);
        super.protocolNegotiatorFactory(clientFactory);
        this.uri = uri;
    }

    public static GrpcOverWebsocketChannelBuilder forTarget(String targetUrl) {
        URI uri = URI.create(targetUrl);
        SocketAddress address = new InetSocketAddress(uri.getHost(), uri.getPort());

        return new GrpcOverWebsocketChannelBuilder(uri, address);
    }

    @CanIgnoreReturnValue
    public GrpcOverWebsocketChannelBuilder sslContext(SslContext sslContext) {
        if (sslContext != null) {
            checkArgument(sslContext.isClient(), "Server SSL context can not be used for client channel");
        }
        this.sslContext = sslContext;
        return this;
    }

    @CanIgnoreReturnValue
    public GrpcOverWebsocketChannelBuilder additionalNegotiator(GrpcOverProtocolNegotiatorDelegate additionalNegotiator) {
        this.additionalNegotiator = additionalNegotiator;
        return this;
    }
}
