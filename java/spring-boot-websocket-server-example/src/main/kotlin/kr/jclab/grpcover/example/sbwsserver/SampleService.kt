package kr.jclab.grpcover.example.sbwsserver

import io.grpc.stub.StreamObserver
import kr.jclab.grpcover.sample.SampleProto
import kr.jclab.grpcover.sample.SampleServiceGrpc
import org.springframework.stereotype.Service

@Service
class SampleService : SampleServiceGrpc.SampleServiceImplBase() {
    override fun helloOnce(
        request: SampleProto.HelloRequest,
        responseObserver: StreamObserver<SampleProto.HelloReply>,
    ) {
        responseObserver.onNext(
            SampleProto.HelloReply.newBuilder()
                .setMessage("REPLY: " + request.message)
                .build()
        )
        responseObserver.onCompleted()
    }

    override fun helloMany(
        request: SampleProto.HelloRequest,
        responseObserver: StreamObserver<SampleProto.HelloReply>,
    ) {
        for (i in 0..9) {
            responseObserver.onNext(
                SampleProto.HelloReply.newBuilder()
                    .setMessage("REPLY[" + i + "]: " + request.message)
                    .build()
            )
            try {
                Thread.sleep(100)
            } catch (e: InterruptedException) {
                throw RuntimeException(e)
            }
        }
        responseObserver.onCompleted()
    }
}