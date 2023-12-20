package kr.jclab.grpcover.example.sbwsserver

import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment
import org.springframework.boot.web.server.LocalServerPort

@SpringBootTest(
    webEnvironment = WebEnvironment.RANDOM_PORT
)
class ClientTest {
    @LocalServerPort
    var randomPort = 0

    @Test
    fun contextTest() {
        val targetAddress = "ws://localhost:${randomPort}/ws"
    }
}