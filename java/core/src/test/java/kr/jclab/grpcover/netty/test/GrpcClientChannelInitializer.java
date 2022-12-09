package kr.jclab.grpcover.netty.test;

import io.netty.channel.*;
import io.netty.channel.Channel;
import io.netty.handler.codec.protobuf.*;
import io.netty.handler.ssl.SslContext;
import io.netty.util.AttributeKey;
import kr.jclab.grpcover.core.protocol.v1.GofProto;
import kr.jclab.grpcover.netty.NettyChannelBuilder;
import org.jetbrains.annotations.NotNull;

public class GrpcClientChannelInitializer extends ChannelInitializer<Channel> {
    public static AttributeKey<SslContext> SSL_CONTEXT_ATTR = AttributeKey.newInstance("SSL_CONTEXT_ATTR");

    @Override
    protected void initChannel(@NotNull Channel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();
        ChannelHandler grpcChannelHandler = ch.attr(NettyChannelBuilder.GRPC_CHANNEL_HANDLER).get();
        SslContext sslContext = ch.attr(GrpcClientChannelInitializer.SSL_CONTEXT_ATTR).get();

        pipeline.addFirst(
                new ProtobufVarint32LengthFieldPrepender(),
                new ProtobufEncoder(),
                new ProtobufVarint32FrameDecoder(),
                new ProtobufDecoder(GofProto.Frame.getDefaultInstance()),
                new TestNegotiationHandler(sslContext),
                grpcChannelHandler);
        if (sslContext != null) {
            pipeline.addFirst(sslContext.newHandler(ch.alloc()));
        }
    }
}
