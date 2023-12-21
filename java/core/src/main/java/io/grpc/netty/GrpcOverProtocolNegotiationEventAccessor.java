package io.grpc.netty;

import io.grpc.Attributes;
import io.grpc.InternalChannelz;

public class GrpcOverProtocolNegotiationEventAccessor {
    public static Attributes getAttributes(ProtocolNegotiationEvent event) {
        return event.getAttributes();
    }

    public static ProtocolNegotiationEvent withSecurity(ProtocolNegotiationEvent event, InternalChannelz.Security security) {
        return event.withSecurity(security);
    }

    public static ProtocolNegotiationEvent withAttributes(ProtocolNegotiationEvent event, Attributes attributes) {
        return event.withAttributes(attributes);
    }
}
