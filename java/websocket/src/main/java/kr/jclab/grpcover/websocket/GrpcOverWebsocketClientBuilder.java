package kr.jclab.grpcover.websocket;

import io.netty.channel.ChannelHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.AsciiString;
import kr.jclab.grpcover.netty.GrpcOverChannelBuilderHelper;
import kr.jclab.grpcover.netty.GrpcOverProtocolNegotiator;
import kr.jclab.grpcover.netty.GrpcOverProtocolNegotiatorDelegate;
import kr.jclab.grpcover.netty.NettyChannelBuilder;

import javax.net.ssl.SSLException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;

public class GrpcOverWebsocketClientBuilder {
    public static NettyChannelBuilder forTarget(
            String targetUrl,
            SslContext sslContext
    ) {
        URI uri = URI.create(targetUrl);
        SocketAddress address = new InetSocketAddress(uri.getHost(), uri.getPort());

        return GrpcOverChannelBuilderHelper.newNettyChannelBuilder(
                address,
                null,
                null,
                new GrpcOverProtocolNegotiator.ClientFactory(new GrpcOverProtocolNegotiatorDelegate(

                ) {
                    @Override
                    public AsciiString scheme() {
                        return new AsciiString(uri.getScheme());
                    }

                    @Override
                    public ChannelHandler newHandler(ChannelHandler next) {
                        return new GrpcOverWebsocketClientNegotiationHandler(next, uri, sslContext);
                    }

                    @Override
                    public void close() {
                        // nothing
                    }
                })
        );
    }

    public static NettyChannelBuilder forTarget(
            String targetUrl
    ) throws SSLException {
        return forTarget(targetUrl, SslContextBuilder.forClient().build());
    }

//    public static NettyChannelBuilder forTarget(
//            EventLoopGroup workerEventLoopGroup,
//            String target
//    ) {
//        GrpcOverWebsocketClientBootstrapFactory bootstrapFactory = new GrpcOverWebsocketClientBootstrapFactory(
//                workerEventLoopGroup,
//                null
//        );
//        return NettyChannelBuilder.forTarget(bootstrapFactory, target);
//    }
//
//    public static NettyChannelBuilder forSslTarget(
//            EventLoopGroup workerEventLoopGroup,
//            String target,
//            SslContext sslContext
//    ) {
//        GrpcOverWebsocketClientBootstrapFactory bootstrapFactory = new GrpcOverWebsocketClientBootstrapFactory(
//                workerEventLoopGroup,
//                sslContext
//        );
//        return NettyChannelBuilder.forTarget(bootstrapFactory, target);
//    }
}
