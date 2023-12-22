package kr.jclab.grpcover;

import io.grpc.Attributes;
import io.grpc.SecurityLevel;
import io.grpc.internal.GrpcAttributes;
import io.grpc.netty.GrpcOverProtocolNegotiationEventAccessor;
import io.grpc.netty.ProtocolNegotiationEvent;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;

public class ImmediatelyGrpcNegotiationHandler extends GrpcNegotiationHandlerBase {
    private final SecurityLevel securityLevel;

    public ImmediatelyGrpcNegotiationHandler(ChannelHandler grpcHandler, SecurityLevel securityLevel) {
        super(grpcHandler);
        this.securityLevel = securityLevel;
    }

    public ImmediatelyGrpcNegotiationHandler(ChannelHandler grpcHandler) {
        this(grpcHandler, SecurityLevel.NONE);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof ProtocolNegotiationEvent) {
            pne = (ProtocolNegotiationEvent) evt;
            Attributes attrs = GrpcOverProtocolNegotiationEventAccessor.getAttributes(pne).toBuilder()
                    .set(GrpcAttributes.ATTR_SECURITY_LEVEL, securityLevel)
                    .build();
            ctx.pipeline().replace(ctx.name(), null, this.grpcHandler);
            fireProtocolNegotiationEvent(ctx, GrpcOverProtocolNegotiationEventAccessor.withAttributes(pne, attrs));
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }
}
