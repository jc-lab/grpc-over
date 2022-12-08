package kr.jclab.grpcover.netty;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.errorprone.annotations.ForOverride;
import io.grpc.Attributes;
import io.grpc.ChannelLogger;
import io.grpc.ChannelLogger.ChannelLogLevel;
import io.grpc.Grpc;
import io.grpc.SecurityLevel;
import io.grpc.internal.GrpcAttributes;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public final class ProtocolNegotiators {
    private ProtocolNegotiators() {
    }

    /**
     * Adapts a {@link ProtocolNegotiationEvent} to the {@link GofConnectionHandlerCallback}.
     */
    static final class GrpcNegotiationHandler extends ChannelInboundHandlerAdapter {
        private final GofConnectionHandler next;

        public GrpcNegotiationHandler(GofConnectionHandler next) {
            this.next = checkNotNull(next, "next");
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof ProtocolNegotiationEvent) {
                ProtocolNegotiationEvent protocolNegotiationEvent = (ProtocolNegotiationEvent) evt;
                ctx.pipeline().replace(ctx.name(), null, next);
                next.handleProtocolNegotiationCompleted(
                        protocolNegotiationEvent.getAttributes(), protocolNegotiationEvent.getSecurity());
            } else {
                super.userEventTriggered(ctx, evt);
            }
        }
    }

    /**
     * Waits for the channel to be active, and then installs the next Handler.  Using this allows
     * subsequent handlers to assume the channel is active and ready to send.  Additionally, this a
     * {@link ProtocolNegotiationEvent}, with the connection addresses.
     */
    public static final class WaitUntilActiveHandler extends ProtocolNegotiationHandler {

        boolean protocolNegotiationEventReceived;

        WaitUntilActiveHandler(ChannelHandler next, ChannelLogger negotiationLogger) {
            super(next, negotiationLogger);
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            if (protocolNegotiationEventReceived) {
                replaceOnActive(ctx);
                fireProtocolNegotiationEvent(ctx);
            }
            // Still propagate channelActive to the new handler.
            super.channelActive(ctx);
        }

        @Override
        protected void protocolNegotiationEventTriggered(ChannelHandlerContext ctx) {
            protocolNegotiationEventReceived = true;
            if (ctx.channel().isActive()) {
                replaceOnActive(ctx);
                fireProtocolNegotiationEvent(ctx);
            }
        }

        private void replaceOnActive(ChannelHandlerContext ctx) {
            ProtocolNegotiationEvent existingPne = getProtocolNegotiationEvent();
            Attributes attrs = existingPne.getAttributes().toBuilder()
                    .set(Grpc.TRANSPORT_ATTR_LOCAL_ADDR, ctx.channel().localAddress())
                    .set(Grpc.TRANSPORT_ATTR_REMOTE_ADDR, ctx.channel().remoteAddress())
                    // Later handlers are expected to overwrite this.
                    .set(GrpcAttributes.ATTR_SECURITY_LEVEL, SecurityLevel.NONE)
                    .build();
            replaceProtocolNegotiationEvent(existingPne.withAttributes(attrs));
        }
    }


    /**
     * ProtocolNegotiationHandler is a convenience handler that makes it easy to follow the rules for
     * protocol negotiation.  Handlers should strongly consider extending this handler.
     */
    static class ProtocolNegotiationHandler extends ChannelDuplexHandler {

        private final ChannelHandler next;
        private final String negotiatorName;
        private ProtocolNegotiationEvent pne;
        private final ChannelLogger negotiationLogger;

        protected ProtocolNegotiationHandler(ChannelHandler next, String negotiatorName,
                                             ChannelLogger negotiationLogger) {
            this.next = checkNotNull(next, "next");
            this.negotiatorName = negotiatorName;
            this.negotiationLogger = checkNotNull(negotiationLogger, "negotiationLogger");
        }

        protected ProtocolNegotiationHandler(ChannelHandler next, ChannelLogger negotiationLogger) {
            this.next = checkNotNull(next, "next");
            this.negotiatorName = getClass().getSimpleName().replace("Handler", "");
            this.negotiationLogger = checkNotNull(negotiationLogger, "negotiationLogger");
        }

        @Override
        public final void handlerAdded(ChannelHandlerContext ctx) throws Exception {
            negotiationLogger.log(ChannelLogLevel.DEBUG, "{0} started", negotiatorName);
            handlerAdded0(ctx);
        }

        @ForOverride
        protected void handlerAdded0(ChannelHandlerContext ctx) throws Exception {
            super.handlerAdded(ctx);
        }

        @Override
        public final void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof ProtocolNegotiationEvent) {
                checkState(pne == null, "pre-existing negotiation: %s < %s", pne, evt);
                pne = (ProtocolNegotiationEvent) evt;
                protocolNegotiationEventTriggered(ctx);
            } else {
                userEventTriggered0(ctx, evt);
            }
        }

        protected void userEventTriggered0(ChannelHandlerContext ctx, Object evt) throws Exception {
            super.userEventTriggered(ctx, evt);
        }

        @ForOverride
        protected void protocolNegotiationEventTriggered(ChannelHandlerContext ctx) {
            // no-op
        }

        protected final ProtocolNegotiationEvent getProtocolNegotiationEvent() {
            checkState(pne != null, "previous protocol negotiation event hasn't triggered");
            return pne;
        }

        protected final void replaceProtocolNegotiationEvent(ProtocolNegotiationEvent pne) {
            checkState(this.pne != null, "previous protocol negotiation event hasn't triggered");
            this.pne = checkNotNull(pne);
        }

        protected final void fireProtocolNegotiationEvent(ChannelHandlerContext ctx) {
            checkState(pne != null, "previous protocol negotiation event hasn't triggered");
            negotiationLogger.log(ChannelLogLevel.INFO, "{0} completed", negotiatorName);
            ctx.pipeline().replace(ctx.name(), /* newName= */ null, next);
            ctx.fireUserEventTriggered(pne);
        }
    }
}
