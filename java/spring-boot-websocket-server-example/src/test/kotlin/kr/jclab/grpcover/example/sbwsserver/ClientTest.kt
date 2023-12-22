package kr.jclab.grpcover.example.sbwsserver

import io.grpc.Status
import io.grpc.StatusRuntimeException
import kr.jclab.grpcover.ImmediatelyGrpcNegotiationHandler
import kr.jclab.grpcover.sample.SampleProto
import kr.jclab.grpcover.sample.SampleServiceGrpc
import kr.jclab.grpcover.websocket.GrpcOverWebsocketChannelBuilder
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.kotlin.*
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment
import org.springframework.boot.test.mock.mockito.SpyBean
import org.springframework.boot.web.server.LocalServerPort
import java.util.concurrent.TimeUnit


@SpringBootTest(
    webEnvironment = WebEnvironment.DEFINED_PORT
)
class ClientTest {
    @LocalServerPort
    var randomPort = 0

    @SpyBean
    lateinit var sampleService: SampleService

    @AfterEach
    fun resetSpy() {
        reset(sampleService)
    }

    fun prepare(): SampleServiceGrpc.SampleServiceBlockingStub {
        val targetAddress = "ws://localhost:${randomPort}/ws" // dummy
        val managedChannel = GrpcOverWebsocketChannelBuilder.forTarget(targetAddress)
            // You can add custom communication layer
            .additionalNegotiator { next -> ImmediatelyGrpcNegotiationHandler(next) }
            .build()
        return SampleServiceGrpc.newBlockingStub(managedChannel)
            .withDeadlineAfter(5, TimeUnit.SECONDS)
    }

    @Test
    fun requestTest() {
        val service = prepare()
        val resp = service.helloOnce(SampleProto.HelloRequest.newBuilder().setMessage("hello").build())
        verify(sampleService, times(1)).helloOnce(any(), any())
        assertThat(resp.message).isEqualTo("REPLY: hello")
    }

    @Test
    fun requestTest_1k() {
        val service = prepare()
        val message = "a".repeat(1024)
        val resp = service.helloOnce(SampleProto.HelloRequest.newBuilder().setMessage(message).build())
        verify(sampleService, times(1)).helloOnce(any(), any())
        assertThat(resp.message).isEqualTo("REPLY: $message")
    }

    @Test
    fun requestTest_2k() {
        val service = prepare()
        val message = "a".repeat(2048)
        val resp = service.helloOnce(SampleProto.HelloRequest.newBuilder().setMessage(message).build())
        verify(sampleService, times(1)).helloOnce(any(), any())
        assertThat(resp.message).isEqualTo("REPLY: $message")
    }

    @Test
    fun requestTest_16k() {
        val service = prepare()
        val message = "a".repeat(16384)
        val resp = service.helloOnce(SampleProto.HelloRequest.newBuilder().setMessage(message).build())
        verify(sampleService, times(1)).helloOnce(any(), any())
        assertThat(resp.message).isEqualTo("REPLY: $message")
    }


    @Test
    fun requestTest_32k() {
        val service = prepare()
        val message = "a".repeat(32768)
        val resp = service.helloOnce(SampleProto.HelloRequest.newBuilder().setMessage(message).build())
        verify(sampleService, times(1)).helloOnce(any(), any())
        assertThat(resp.message).isEqualTo("REPLY: $message")
    }

    @Test
    fun requestTest_64k() {
        val service = prepare()
        val message = "a".repeat(65536)
        val resp = service.helloOnce(SampleProto.HelloRequest.newBuilder().setMessage(message).build())
        verify(sampleService, times(1)).helloOnce(any(), any())
        assertThat(resp.message).isEqualTo("REPLY: $message")
    }

    @Test
    fun requestTest_1M() {
        val service = prepare()
        val message = "a".repeat(1048576)
        val resp = service.helloOnce(SampleProto.HelloRequest.newBuilder().setMessage(message).build())
        verify(sampleService, times(1)).helloOnce(any(), any())
        assertThat(resp.message).isEqualTo("REPLY: $message")
    }

    @Test
    fun requestTest_4M() {
        val service = prepare()
        val message = "a".repeat(1048576 * 4)
        val exception = assertThrows<StatusRuntimeException>("RST_STREAM closed stream. HTTP/2 error code: CANCEL") {
            service.helloOnce(SampleProto.HelloRequest.newBuilder().setMessage(message).build())
        }
        assertThat(exception.status.code)
            .isEqualTo(Status.CANCELLED.code)
        verify(sampleService, never()).helloOnce(any(), any())
    }
}