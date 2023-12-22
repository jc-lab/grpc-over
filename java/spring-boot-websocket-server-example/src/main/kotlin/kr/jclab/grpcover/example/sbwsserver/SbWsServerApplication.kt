package kr.jclab.grpcover.example.sbwsserver

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class SbWsServerApplication

fun main(args: Array<String>) {
    runApplication<SbWsServerApplication>(*args)
}