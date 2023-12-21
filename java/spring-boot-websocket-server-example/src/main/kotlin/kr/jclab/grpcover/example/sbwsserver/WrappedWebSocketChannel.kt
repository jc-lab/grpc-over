package kr.jclab.grpcover.example.sbwsserver

import io.netty.buffer.ByteBuf
import io.netty.channel.Channel
import io.netty.channel.ChannelOutboundBuffer
import io.netty.channel.DefaultChannelId
import kr.jclab.netty.pseudochannel.AbstractPseudoChannel
import org.slf4j.LoggerFactory
import org.springframework.web.socket.BinaryMessage
import org.springframework.web.socket.WebSocketSession
import java.net.SocketAddress

class WrappedWebSocketChannel(
    parent: Channel,
    private val webSocketSession: WebSocketSession,
) : AbstractPseudoChannel(parent, DefaultChannelId.newInstance()) {
    private val log = LoggerFactory.getLogger(this::class.java)
    val registerFuture = this.newPromise()

    private enum class State {
        OPEN, ACTIVE, INACTIVE, CLOSED
    }

    private var state: State = State.OPEN


    override fun localAddress0(): SocketAddress? {
        return webSocketSession.localAddress
    }

    override fun remoteAddress0(): SocketAddress? {
        return webSocketSession.remoteAddress
    }

    @Throws(Exception::class)
    override fun doRegister() {
        super.doRegister()
        state = State.ACTIVE
        registerFuture.setSuccess()
    }

    protected fun deactivate() {
        if (state == State.ACTIVE) {
            pipeline().fireChannelInactive()
            state = State.INACTIVE
        }
    }

    @Throws(Exception::class)
    override fun doDisconnect() {
        if (webSocketSession.isOpen) {
            webSocketSession.close()
        }
        doClose()
    }

    @Throws(Exception::class)
    public override fun doClose() {
        val eventLoop = eventLoop()
        if (eventLoop.inEventLoop()) {
            doCloseImpl()
        } else {
            eventLoop.execute { doCloseImpl() }
        }
    }

    private fun doCloseImpl() {
        try {
            if (state != State.CLOSED) {
                log.info("doClose")
                if (webSocketSession.isOpen) {
                    webSocketSession.close()
                }
                deactivate()
                pipeline().deregister()
                state = State.CLOSED
            }
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }

    @Throws(Exception::class)
    override fun doBeginRead() {
        // Nothing
    }

    @Throws(Exception::class)
    override fun doWrite(inb: ChannelOutboundBuffer) {
        try {
            while (true) {
                val msg = inb.current() ?: break
                val buf = msg as ByteBuf
                val copiedBuffer = ByteArray(buf.readableBytes())
                buf.readBytes(copiedBuffer)
                webSocketSession.sendMessage(BinaryMessage(copiedBuffer))
                inb.remove()
            }
        } catch (e: Exception) {
            log.warn("doWrite failed", e)
            throw e
        }
    }

    override fun isOpen(): Boolean {
        return state != State.CLOSED
    }

    override fun isActive(): Boolean {
        return state == State.ACTIVE
    }
}