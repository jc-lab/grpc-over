package kr.jclab.grpcover;

import io.grpc.Attributes;
import io.grpc.SecurityLevel;
import io.grpc.internal.GrpcAttributes;
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

        Attributes attrs = pne.getAttributes().toBuilder()
                .set(GrpcAttributes.ATTR_SECURITY_LEVEL, securityLevel)
                .build();

        ctx.pipeline().replace(ctx.name(), null, this.grpcHandler);
        fireProtocolNegotiationEvent(ctx, pne.withAttributes(attrs));
    }
}
