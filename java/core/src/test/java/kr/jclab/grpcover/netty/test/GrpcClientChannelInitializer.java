package kr.jclab.grpcover.netty.test;

import io.grpc.Attributes;
import io.grpc.Grpc;
import io.grpc.SecurityLevel;
import io.grpc.internal.GrpcAttributes;
import io.netty.channel.*;
import io.netty.handler.codec.protobuf.*;
import kr.jclab.grpcover.core.protocol.v1.GofProto;
import kr.jclab.grpcover.netty.NettyChannelBuilder;
import kr.jclab.grpcover.netty.ProtocolNegotiationEvent;
import org.jetbrains.annotations.NotNull;

public class GrpcClientChannelInitializer extends ChannelInitializer<Channel> {
    @Override
    protected void initChannel(@NotNull Channel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();
        ChannelHandler grpcChannelHandler = ch.attr(NettyChannelBuilder.GRPC_CHANNEL_HANDLER).get();

        pipeline.addFirst(
                new ProtobufVarint32LengthFieldPrepender(),
                new ProtobufEncoder(),
                new ProtobufVarint32FrameDecoder(),
                new ProtobufDecoder(GofProto.Frame.getDefaultInstance()),
                new ChannelDuplexHandler() {
                    private ProtocolNegotiationEvent pne = ProtocolNegotiationEvent.DEFAULT;

                    @Override
                    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
                        if (evt instanceof ProtocolNegotiationEvent) {
                            pne = (ProtocolNegotiationEvent) evt;

                            // only once
                            fireProtocolNegotiationEvent(ctx);
                        } else {
                            super.userEventTriggered(ctx, evt);
                        }
                    }

                    private void fireProtocolNegotiationEvent(ChannelHandlerContext ctx) {
                        Attributes attrs = pne.getAttributes().toBuilder()
                                .set(GrpcAttributes.ATTR_SECURITY_LEVEL, SecurityLevel.PRIVACY_AND_INTEGRITY)
//                                .set(Grpc.TRANSPORT_ATTR_SSL_SESSION, session)
                                .build();
                        ctx.fireUserEventTriggered(pne.withAttributes(attrs));
                    }
                },
                grpcChannelHandler);
    }
}
