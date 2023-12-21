package kr.jclab.grpcover;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import kr.jclab.grpcover.netty.ProtocolNegotiationEvent;

public class GrpcNegotiationHandlerBase extends ChannelDuplexHandler {
    protected final ChannelHandler grpcHandler;

    protected ProtocolNegotiationEvent pne = ProtocolNegotiationEvent.DEFAULT;

    public GrpcNegotiationHandlerBase(ChannelHandler grpcHandler) {
        this.grpcHandler = grpcHandler;
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof ProtocolNegotiationEvent) {
            pne = (ProtocolNegotiationEvent) evt;
        }
        super.userEventTriggered(ctx, evt);
    }

    protected void fireProtocolNegotiationEvent(ChannelHandlerContext ctx, ProtocolNegotiationEvent event) {
        ctx.fireUserEventTriggered(event);
    }
}
