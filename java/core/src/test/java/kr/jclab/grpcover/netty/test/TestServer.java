package kr.jclab.grpcover.netty.test;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import kr.jclab.grpcover.core.protocol.v1.GofProto;
import kr.jclab.grpcover.portable.GofChannelInitializer;
import kr.jclab.grpcover.netty.ServerChannelSetupHandler;
import org.jetbrains.annotations.NotNull;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

public class TestServer {
    public final Initializer initializer;
    public final Channel serverChannel;

    public TestServer(Initializer initializer, Channel serverChannel) {
        this.initializer = initializer;
        this.serverChannel = serverChannel;
    }

    public static TestServer newServer(TestHelper testHelper, int port) {
        try {
            Initializer initializer = new Initializer();
            Channel serverChannel = new ServerBootstrap()
                    .group(testHelper.boss, testHelper.worker)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(initializer)
                    .bind(new InetSocketAddress("127.0.0.1", port))
                    .sync()
                    .channel();
            return new TestServer(initializer, serverChannel);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static class Initializer extends ChannelInitializer<NioSocketChannel> implements GofChannelInitializer {
        private final Logger logger = Logger.getLogger(Initializer.class.getName());
        private final CompletableFuture<ServerChannelSetupHandler> serverHandlerBuilderCompletableFuture = new CompletableFuture<>();

        @Override
        protected void initChannel(@NotNull NioSocketChannel ch) throws Exception {
            ch.pipeline().addLast(
                    new ProtobufVarint32LengthFieldPrepender(),
                    new ProtobufEncoder(),
                    new ProtobufVarint32FrameDecoder(),
                    new ProtobufDecoder(GofProto.Frame.getDefaultInstance())
            );
            serverHandlerBuilderCompletableFuture.get().setup(ch);
        }

        @Override
        public void attachGofServerChannelSetupHandler(ServerChannelSetupHandler serverChannelSetupHandler) {
            serverHandlerBuilderCompletableFuture.complete(serverChannelSetupHandler);
        }
    }
}
