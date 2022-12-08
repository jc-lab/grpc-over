import io.grpc.Server;
import io.grpc.stub.StreamObserver;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import kr.jclab.grpcover.sample.SampleProto;
import kr.jclab.grpcover.sample.SampleServiceGrpc;
import kr.jclab.grpcover.websocket.GrpcOverWebsocketServerBuilder;

import java.io.IOException;

public class SampleServer {
    public static void main(String[] args) throws IOException {
        EventLoopGroup boss = new NioEventLoopGroup(1);
        EventLoopGroup worker = new NioEventLoopGroup();
        Server server = GrpcOverWebsocketServerBuilder.forPort(
                boss,
                worker,
                12345,
                null,
                "/ws",
                null
        )
                .addService(new SampleServiceGrpc.SampleServiceImplBase() {
                    @Override
                    public void helloOnce(SampleProto.HelloRequest request, StreamObserver<SampleProto.HelloReply> responseObserver) {
                        responseObserver.onNext(SampleProto.HelloReply.newBuilder()
                                        .setMessage("REPLY: " + request.getMessage())
                                .build()
                        );
                        responseObserver.onCompleted();
                    }

                    @Override
                    public void helloMany(SampleProto.HelloRequest request, StreamObserver<SampleProto.HelloReply> responseObserver) {
                        for (int i=0; i<10; i++) {
                            responseObserver.onNext(SampleProto.HelloReply.newBuilder()
                                    .setMessage("REPLY[" + i + "]: " + request.getMessage())
                                    .build()
                            );
                            try {
                                Thread.sleep(100);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        responseObserver.onCompleted();
                    }
                })
                .build();
        server.start();
    }
}
