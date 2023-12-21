package kr.jclab.grpcover.example.sbwsserver

import kr.jclab.grpcover.sample.SampleProto
import kr.jclab.grpcover.sample.SampleServiceGrpc
import kr.jclab.grpcover.websocket.GrpcOverWebsocketClientBuilder
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment
import org.springframework.boot.web.server.LocalServerPort

@SpringBootTest(
    webEnvironment = WebEnvironment.DEFINED_PORT
)
class ClientTest {
    @LocalServerPort
    var randomPort = 0

    @Test
    fun connectTest() {
        val targetAddress = "ws://localhost:${randomPort}/ws" // dummy

        val managedChannel = GrpcOverWebsocketClientBuilder.forTarget(targetAddress)
            .build()

        val service = SampleServiceGrpc.newBlockingStub(managedChannel)

        val resp = service.helloOnce(SampleProto.HelloRequest.getDefaultInstance())
        println("resp: ")
        println(resp)
        println("==================================================")
    }
}