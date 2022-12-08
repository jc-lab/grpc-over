import io.grpc.ManagedChannel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import kr.jclab.grpcover.sample.SampleProto;
import kr.jclab.grpcover.sample.SampleServiceGrpc;
import kr.jclab.grpcover.websocket.GrpcOverWebsocketClientBuilder;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class SampleClient {
    public static void main(String[] args) {
        EventLoopGroup worker = new NioEventLoopGroup();
        ManagedChannel managedChannel = GrpcOverWebsocketClientBuilder.forTarget(worker, "ws://127.0.0.1:12345/ws")
                .build();
        SampleServiceGrpc.SampleServiceBlockingStub blockingStub = SampleServiceGrpc.newBlockingStub(managedChannel);

        SampleProto.HelloReply helloReply = blockingStub.helloOnce(SampleProto.HelloRequest.newBuilder().setMessage("HELLO WORLD").build());
        System.out.println("helloReply: " + helloReply);


        Iterator<SampleProto.HelloReply> iterator = blockingStub.helloMany(SampleProto.HelloRequest.newBuilder().setMessage("HELLO WORLD").build());
        while (iterator.hasNext()) {
            System.out.println("helloReply2: " + iterator.next());
        }

        managedChannel.shutdownNow();
        worker.shutdownGracefully(1000, 2000, TimeUnit.MILLISECONDS);
    }
}
