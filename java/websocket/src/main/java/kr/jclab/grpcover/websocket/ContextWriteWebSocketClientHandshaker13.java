package kr.jclab.grpcover.websocket;

import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker13;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;

import java.net.URI;

public class ContextWriteWebSocketClientHandshaker13 extends WebSocketClientHandshaker13 {
    private final URI uri;
    private final WebSocketVersion version = WebSocketVersion.V13;

    public ContextWriteWebSocketClientHandshaker13(URI webSocketURL, WebSocketVersion version, String subprotocol, boolean allowExtensions, HttpHeaders customHeaders, int maxFramePayloadLength) {
        super(webSocketURL, version, subprotocol, allowExtensions, customHeaders, maxFramePayloadLength);
        this.uri = webSocketURL;
    }

    public ContextWriteWebSocketClientHandshaker13(URI webSocketURL, WebSocketVersion version, String subprotocol, boolean allowExtensions, HttpHeaders customHeaders, int maxFramePayloadLength, boolean performMasking, boolean allowMaskMismatch) {
        super(webSocketURL, version, subprotocol, allowExtensions, customHeaders, maxFramePayloadLength, performMasking, allowMaskMismatch);
        this.uri = webSocketURL;
    }

    public ContextWriteWebSocketClientHandshaker13(URI webSocketURL, WebSocketVersion version, String subprotocol, boolean allowExtensions, HttpHeaders customHeaders, int maxFramePayloadLength, boolean performMasking, boolean allowMaskMismatch, long forceCloseTimeoutMillis) {
        super(webSocketURL, version, subprotocol, allowExtensions, customHeaders, maxFramePayloadLength, performMasking, allowMaskMismatch, forceCloseTimeoutMillis);
        this.uri = webSocketURL;
    }

    public final ChannelFuture handshakeWithContext(ChannelHandlerContext ctx, ChannelPromise promise) {
        ChannelPipeline pipeline = ctx.pipeline();
        HttpResponseDecoder decoder = pipeline.get(HttpResponseDecoder.class);
        if (decoder == null) {
            HttpClientCodec codec = pipeline.get(HttpClientCodec.class);
            if (codec == null) {
                promise.setFailure(new IllegalStateException("ChannelPipeline does not contain " +
                        "an HttpResponseDecoder or HttpClientCodec"));
                return promise;
            }
        }

        if (uri.getHost() == null) {
            if (customHeaders == null || !customHeaders.contains(HttpHeaderNames.HOST)) {
                promise.setFailure(new IllegalArgumentException("Cannot generate the 'host' header value," +
                        " webSocketURI should contain host or passed through customHeaders"));
                return promise;
            }

            if (generateOriginHeader && !customHeaders.contains(HttpHeaderNames.ORIGIN)) {
                final String originName;
                if (version == WebSocketVersion.V07 || version == WebSocketVersion.V08) {
                    originName = HttpHeaderNames.SEC_WEBSOCKET_ORIGIN.toString();
                } else {
                    originName = HttpHeaderNames.ORIGIN.toString();
                }

                promise.setFailure(new IllegalArgumentException("Cannot generate the '" + originName + "' header" +
                        " value, webSocketURI should contain host or disable generateOriginHeader or pass value" +
                        " through customHeaders"));
                return promise;
            }
        }

        FullHttpRequest request = newHandshakeRequest();

        ctx.writeAndFlush(request).addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) {
                if (future.isSuccess()) {
                    ChannelPipeline p = future.channel().pipeline();
                    ChannelHandlerContext ctx = p.context(HttpRequestEncoder.class);
                    if (ctx == null) {
                        ctx = p.context(HttpClientCodec.class);
                    }
                    if (ctx == null) {
                        promise.setFailure(new IllegalStateException("ChannelPipeline does not contain " +
                                "an HttpRequestEncoder or HttpClientCodec"));
                        return;
                    }
                    p.addAfter(ctx.name(), "ws-encoder", newWebSocketEncoder());

                    promise.setSuccess();
                } else {
                    promise.setFailure(future.cause());
                }
            }
        });
        return promise;
    }
}
