package kr.jclab.grpcover.example.sbwsserver

import io.netty.channel.ChannelHandler
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.util.AsciiString
import kr.jclab.grpcover.netty.NettyServerBuilder
import kr.jclab.grpcover.ImmediatelyGrpcNegotiationHandler
import kr.jclab.grpcover.netty.GrpcOverProtocolNegotiator
import kr.jclab.grpcover.netty.GrpcOverProtocolNegotiatorDelegate
import kr.jclab.netty.pseudochannel.DefaultPseudoSocketAddress
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

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
        val negotiator = GrpcOverProtocolNegotiator(object :
            GrpcOverProtocolNegotiatorDelegate {
            override fun scheme(): AsciiString {
                return AsciiString("ws")
            }

            override fun newHandler(grpcHandler: ChannelHandler): ChannelHandler {
                return ImmediatelyGrpcNegotiationHandler(grpcHandler)
            }

            override fun close() {
                println("close GrpcOverProtocolNegotiatorDelegate")
            }
        })

        val server = NettyServerBuilder
            .forAddress(addr)
            .bossEventLoopGroup(bossGroup)
            .workerEventLoopGroup(workerGroup)
            .channelFactory {
                channel
            }
            .addService(sampleService)
            .apply { negotiator.attachToNettyServerBuilder(this) }
            .build()
        server.start()
        return channel
    }
}