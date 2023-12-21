package kr.jclab.grpcover.netty.gof;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http2.*;

public class GofFrameReader implements Http2FrameReader {
    private final Http2HeadersDecoder headersDecoder;

    public GofFrameReader(Http2HeadersDecoder headersDecoder) {
        this.headersDecoder = headersDecoder;
    }

    public GofFrameReader() {
        this(new DefaultHttp2HeadersDecoder());
    }

    @Override
    public void readFrame(ChannelHandlerContext ctx, ByteBuf input, Http2FrameListener listener) throws Http2Exception {

    }

    @Override
    public Configuration configuration() {
        return null;
    }

    @Override
    public void close() {

    }
}
