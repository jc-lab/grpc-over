package kr.jclab.grpcover.example.sbwsserver

import io.netty.buffer.Unpooled
import org.springframework.stereotype.Component
import org.springframework.web.socket.*
import org.springframework.web.socket.handler.BinaryWebSocketHandler

@Component
class GrpcWebsocketHandler(
    private val grpcServerChannel: GrpcServerChannel,
) : BinaryWebSocketHandler() {
    override fun afterConnectionEstablished(session: WebSocketSession) {
        val channel = WrappedWebSocketChannel(
            grpcServerChannel,
            session
        )
        session.attributes["grpcChannel"] = channel
        grpcServerChannel.registerChildChannel(channel)
    }

    override fun handleTransportError(session: WebSocketSession, exception: Throwable) {
        val grpcChannel = session.attributes["grpcChannel"] as WrappedWebSocketChannel
        grpcChannel.pipeline().fireExceptionCaught(exception)
    }

    override fun afterConnectionClosed(session: WebSocketSession, status: CloseStatus) {
        val grpcChannel = session.attributes["grpcChannel"] as WrappedWebSocketChannel
        grpcChannel.doClose()
    }

    override fun handleBinaryMessage(session: WebSocketSession, message: BinaryMessage) {
        val grpcChannel = session.attributes["grpcChannel"] as WrappedWebSocketChannel
        grpcChannel.pipeline().fireChannelRead(Unpooled.copiedBuffer(message.payload))
    }
}