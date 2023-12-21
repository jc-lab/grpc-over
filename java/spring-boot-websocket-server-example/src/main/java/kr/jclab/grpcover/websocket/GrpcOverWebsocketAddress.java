package kr.jclab.grpcover.websocket;

import java.net.SocketAddress;

public class GrpcOverWebsocketAddress extends SocketAddress {
    private final String url;

    public GrpcOverWebsocketAddress(String url) {
        super();
        this.url = url;
    }
}
