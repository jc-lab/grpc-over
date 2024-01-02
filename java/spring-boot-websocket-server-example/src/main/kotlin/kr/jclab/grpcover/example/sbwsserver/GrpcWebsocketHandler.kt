package kr.jclab.grpcover.example.sbwsserver

import io.netty.handler.codec.http2.Http2CodecUtil
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
        session.binaryMessageSizeLimit = Http2CodecUtil.DEFAULT_MAX_FRAME_SIZE
        grpcServerChannel.registerChildChannel(channel)
        channel.registerPromise.get()
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
        val pipeline = grpcChannel.pipeline()

        // DO NOT USE Unpooled
        // When using unpooled, cause the below exception:
        // java.lang.IndexOutOfBoundsException: writerIndex(14) + minWritableBytes(8) exceeds maxCapacity(14): UnpooledDuplicatedByteBuf(ridx: 9, widx: 14, cap: 14/14, unwrapped: UnpooledHeapByteBuf(ridx: 9, widx: 14, cap: 14/14))
        //	at io.netty.handler.codec.ByteToMessageDecoder.channelRead(ByteToMessageDecoder.java:280)

        val buffer = grpcChannel.alloc().heapBuffer(message.payloadLength)
        buffer.writeBytes(message.payload)
        pipeline.fireChannelRead(buffer)

        pipeline.fireChannelReadComplete()
    }
}