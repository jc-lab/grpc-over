package kr.jclab.grpcover.netty;

import io.grpc.Attributes;
import io.grpc.ChannelLogger;
import io.grpc.InternalChannelz;

public interface GofConnectionHandlerCallback {
    /**
     * Triggered on protocol negotiation completion.
     *
     * <p>It must me called after negotiation is completed but before given handler is added to the
     * channel.
     *
     * @param attrs arbitrary attributes passed after protocol negotiation (eg. SSLSession).
     * @param securityInfo informs channelz about the security protocol.
     */
    void handleProtocolNegotiationCompleted(Attributes attrs, InternalChannelz.Security securityInfo);

    /**
     * Returns the channel logger for the given channel context.
     */
    ChannelLogger getNegotiationLogger();
}
