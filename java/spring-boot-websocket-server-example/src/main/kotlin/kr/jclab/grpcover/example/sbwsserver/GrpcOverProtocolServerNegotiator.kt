package kr.jclab.grpcover.example.sbwsserver

import io.grpc.netty.GrpcHttp2ConnectionHandler
import io.grpc.netty.InternalProtocolNegotiator
import io.netty.channel.ChannelHandler
import io.netty.util.AsciiString

class GrpcOverProtocolServerNegotiator : InternalProtocolNegotiator.ProtocolNegotiator {
    override fun scheme(): AsciiString {
        return AsciiString("grpcover")
    }

    override fun newHandler(grpcHandler: GrpcHttp2ConnectionHandler?): ChannelHandler {
        throw RuntimeException("XX")
    }

    override fun close() {
        println("CLOSE")
    }
}