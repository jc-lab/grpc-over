package kr.jclab.grpcover.netty.test;

import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

public class TestHelper {
    public final EventLoopGroup boss = new NioEventLoopGroup();
    public final EventLoopGroup worker = new NioEventLoopGroup();

    public final InetSocketAddress localhost = new InetSocketAddress("127.0.0.1", 0);
    public final SimpleClientBootstrapFactory clientBootstrapFactory = new SimpleClientBootstrapFactory(worker);


    public void shutdown() {
        boss.shutdownGracefully(1000, 3000, TimeUnit.MILLISECONDS);
        worker.shutdownGracefully(1000, 3000, TimeUnit.MILLISECONDS);
    }

    //  public EmbeddedServerChannel serverChannel;
    //
    //    public TestHelper() {
    //        try {
    //            serverChannel = (EmbeddedServerChannel) new ServerBootstrap()
    //                    .group(boss, worker)
    //                    .channel(NioServerSocketChannel.class)
    //                    .childHandler(new EmbeddedServerChannelInitializer())
    //                    .bind(new DefaultPseudoSocketAddress())
    //                    .sync()
    //                    .channel();
    //        } catch (Exception e) {
    //            throw new RuntimeException(e);
    //        }
    //    }
    //
    //    public void shutdown() {
    //        boss.shutdownGracefully();
    //        worker.shutdownGracefully();
    //    }
    //
    //    public EmbeddedPairingChannel connectClient() {
    //        DefaultPseudoSocketAddress localAddress = new DefaultPseudoSocketAddress();
    //
    //        EmbeddedPairingChannel server = new EmbeddedPairingChannel(
    //                serverChannel,
    //                DefaultChannelId.newInstance(),
    //                localAddress,
    //                serverChannel.remoteAddress()
    //        );
    //        EmbeddedPairingChannel client = new EmbeddedPairingChannel(
    //                null,
    //                DefaultChannelId.newInstance(),
    //                serverChannel.localAddress(),
    //                localAddress
    //        );
    //
    //        server.pipeline().addLast(new ChannelInitializer<Channel>() {
    //            @Override
    //            protected void initChannel(Channel ch) throws Exception {
    //                ChannelPipeline pipeline = ch.pipeline();
    //                pipeline.addLast(new ForwardChannelHandler(client));
    //            }
    //        });
    //        client.pipeline().addLast(new ChannelInitializer<Channel>() {
    //            @Override
    //            protected void initChannel(Channel ch) throws Exception {
    //                ChannelPipeline pipeline = ch.pipeline();
    //                pipeline.addLast(new ForwardChannelHandler(server));
    //            }
    //        });
    //
    //        serverChannel.pipeline().fireChannelRead(server);
    //        worker.register(client);
    //
    //        return client;
    //    }
    //
    //    public class ForwardChannelHandler extends ChannelDuplexHandler {
    //        private final EmbeddedPairingChannel channel;
    //
    //        public ForwardChannelHandler(EmbeddedPairingChannel channel) {
    //            this.channel = channel;
    //        }
    //
    //        @Override
    //        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    //            this.channel.write(msg, promise);
    //        }
    //
    //        @Override
    //        public void flush(ChannelHandlerContext ctx) throws Exception {
    //            this.channel.flush();
    //        }
    //    }
}
