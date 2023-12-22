package kr.jclab.grpcover.example.sbwsserver

import io.grpc.*
import io.grpc.ForwardingServerCall.SimpleForwardingServerCall
import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener
import io.grpc.Metadata.ASCII_STRING_MARSHALLER
import io.grpc.netty.GrpcOverProtocolNegotiator
import io.grpc.netty.GrpcOverProtocolNegotiatorDelegate
import io.grpc.netty.NettyServerBuilder
import io.netty.channel.ChannelHandler
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.util.AsciiString
import kr.jclab.grpcover.ImmediatelyGrpcNegotiationHandler
import kr.jclab.netty.pseudochannel.DefaultPseudoSocketAddress
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.concurrent.atomic.AtomicInteger


@Configuration
class GrpcServerConfig {
    @Bean
    fun bossGroup(): NioEventLoopGroup {
        return NioEventLoopGroup(1)
    }

    @Bean
    fun workerGroup(): NioEventLoopGroup {
        return NioEventLoopGroup(1)
    }

    @Bean
    fun grpcServerChannel(
        bossGroup: NioEventLoopGroup,
        workerGroup: NioEventLoopGroup,
        sampleService: SampleService,
    ): GrpcServerChannel {
        val addr = DefaultPseudoSocketAddress()
        val channel = GrpcServerChannel()
        val negotiator = GrpcOverProtocolNegotiator { grpcHandler -> ImmediatelyGrpcNegotiationHandler(grpcHandler) }

        val server = NettyServerBuilder
            .forAddress(addr)
            .bossEventLoopGroup(bossGroup)
            .workerEventLoopGroup(workerGroup)
            .channelFactory {
                channel
            }
            .addService(sampleService)
            // .intercept(logging())
            .apply { negotiator.attachToNettyServerBuilder(this) }
            .build()
        server.start()
        return channel
    }

    private val logger = LoggerFactory.getLogger(this::class.java)
    private val TRACE_SEQ_KEY: Metadata.Key<String> = Metadata.Key.of("trace-seq", ASCII_STRING_MARSHALLER)
    private fun logging(): ServerInterceptor? {
        val seq = AtomicInteger(0)
        return object : ServerInterceptor {
            override fun <ReqT, RespT> interceptCall(
                call: ServerCall<ReqT, RespT>?, headers: Metadata, next: ServerCallHandler<ReqT, RespT>,
            ): ServerCall.Listener<ReqT> {
                val remoteSeq = headers.get(TRACE_SEQ_KEY)
                val seqNr = seq.incrementAndGet()
                return object : SimpleForwardingServerCallListener<ReqT>(
                    next.startCall(
                        object : SimpleForwardingServerCall<ReqT, RespT>(call) {
                            override fun sendMessage(message: RespT) {
                                super.sendMessage(message)
                                logger.info("{} - sendMessage", seq)
                            }

                            override fun close(status: Status, trailers: Metadata?) {
                                super.close(status, trailers)
                                logger.info(
                                    "{} trace-seq: {} - closed with status code: {}, desc: {}",
                                    seqNr,
                                    remoteSeq,
                                    status.getCode(),
                                    status.getDescription()
                                )
                            }

                            override fun sendHeaders(headers: Metadata?) {
                                super.sendHeaders(headers)
                                logger.info("{} - sendHeader", seqNr)
                            }
                        },
                        headers
                    )
                ) {
                    override fun onHalfClose() {
                        super.onHalfClose()
                        logger.info("{} - onHalfClose", seqNr)
                    }

                    override fun onMessage(message: ReqT) {
                        super.onMessage(message)
                        logger.info("{} - onMessage", seqNr)
                    }

                    override fun onCancel() {
                        super.onCancel()
                        logger.info("{} - onCancel", seqNr)
                    }

                    override fun onComplete() {
                        super.onComplete()
                        logger.info("{} - onComplete", seqNr)
                    }
                }
            }
        }
    }
}