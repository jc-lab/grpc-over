package kr.jclab.grpcover.example.sbwsserver

import io.grpc.netty.GrpcHttp2ConnectionHandler
import io.grpc.netty.GrpcOverProtocolNegotiator
import io.grpc.netty.NettyServerBuilder
import io.netty.channel.ChannelHandler
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.util.AsciiString
import kr.jclab.grpcover.GrpcOverProtocolNegotiatorDelegate
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
        val server = NettyServerBuilder
            .forAddress(addr)
            .bossEventLoopGroup(bossGroup)
            .workerEventLoopGroup(workerGroup)
            .channelFactory {
                channel
            }
            .protocolNegotiator(GrpcOverProtocolNegotiator(object: GrpcOverProtocolNegotiatorDelegate {
                override fun scheme(): AsciiString {
                    return AsciiString("grpcover")
                }

                override fun newHandler(grpcHandler: GrpcHttp2ConnectionHandler): ChannelHandler {
                    TODO("Not yet implemented")
                }

                override fun close() {
                    TODO("Not yet implemented")
                }
            }))
            .addService(sampleService)
            .build()
        server.start()
        return channel
    }
}