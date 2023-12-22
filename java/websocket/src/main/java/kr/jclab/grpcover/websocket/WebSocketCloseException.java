package kr.jclab.grpcover.websocket;

public class WebSocketCloseException extends Exception {
    private final int statusCode;
    private final String reasonText;

    public WebSocketCloseException(int statusCode, String reasonText) {
        super(reasonText);
        this.statusCode = statusCode;
        this.reasonText = reasonText;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public String getReasonText() {
        return reasonText;
    }
}
