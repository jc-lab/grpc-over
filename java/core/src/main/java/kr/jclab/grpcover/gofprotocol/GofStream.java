package kr.jclab.grpcover.gofprotocol;

public interface GofStream {

    /**
     * The allowed states of an HTTP2 stream.
     */
    enum State {
        IDLE,
        OPEN,
        CLOSED;
    }

    int id();
    State state();

    <T> void setProperty(GofConnection.PropertyKey<T> key, T value);
    <T> T getProperty(GofConnection.PropertyKey<T> key);

    void open() throws GofException;

    void close();

    /**
     * Closes the local side of this stream. If this makes the stream closed, the child is closed as
     * well.
     */
    void closeLocalSide();

    /**
     * Closes the remote side of this stream. If this makes the stream closed, the child is closed
     * as well.
     */
    void closeRemoteSide();

    /**
     * Indicates whether a {@code RST_STREAM} frame has been sent from the local endpoint for this stream.
     */
    boolean isResetSent();

    /**
     * Sets the flag indicating that a {@code RST_STREAM} frame has been sent from the local endpoint
     * for this stream. This does not affect the stream state.
     */
    void resetSent();

    /**
     * Indicates that headers have been sent to the remote endpoint on this stream. The first call to this method would
     * be for the initial headers (see {@link #isHeadersSent()}} and the second call would indicate the trailers
     * (see /@link #isTrailersReceived()/).
     * @param isInformational {@code true} if the headers contain an informational status code (for responses only).
     */
    void headersSent(boolean isInformational);

    /**
     * Indicates whether or not headers were sent to the remote endpoint.
     */
    boolean isHeadersSent();
}
