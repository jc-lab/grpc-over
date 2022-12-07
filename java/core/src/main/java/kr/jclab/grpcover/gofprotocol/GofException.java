package kr.jclab.grpcover.gofprotocol;

import io.grpc.internal.GrpcUtil.Http2Error;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.SuppressJava6Requirement;
import io.netty.util.internal.ThrowableUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static io.netty.util.internal.ObjectUtil.checkNotNull;

public class GofException extends Exception {
    private static final long serialVersionUID = -6941186345430164209L;
    private final Http2Error error;
    private final ShutdownHint shutdownHint;

    public GofException(Http2Error error) {
        this(error, ShutdownHint.HARD_SHUTDOWN);
    }

    public GofException(Http2Error error, ShutdownHint shutdownHint) {
        this.error = checkNotNull(error, "error");
        this.shutdownHint = checkNotNull(shutdownHint, "shutdownHint");
    }

    public GofException(Http2Error error, String message) {
        this(error, message, ShutdownHint.HARD_SHUTDOWN);
    }

    public GofException(Http2Error error, String message, ShutdownHint shutdownHint) {
        super(message);
        this.error = checkNotNull(error, "error");
        this.shutdownHint = checkNotNull(shutdownHint, "shutdownHint");
    }

    public GofException(Http2Error error, String message, Throwable cause) {
        this(error, message, cause, ShutdownHint.HARD_SHUTDOWN);
    }

    public GofException(Http2Error error, String message, Throwable cause, ShutdownHint shutdownHint) {
        super(message, cause);
        this.error = checkNotNull(error, "error");
        this.shutdownHint = checkNotNull(shutdownHint, "shutdownHint");
    }

    static GofException newStatic(Http2Error error, String message, ShutdownHint shutdownHint,
                                    Class<?> clazz, String method) {
        final GofException exception;
        if (PlatformDependent.javaVersion() >= 7) {
            exception = new StacklessGofException(error, message, shutdownHint, true);
        } else {
            exception = new StacklessGofException(error, message, shutdownHint);
        }
        return ThrowableUtil.unknownStackTrace(exception, clazz, method);
    }

    @SuppressJava6Requirement(reason = "uses Java 7+ Exception.<init>(String, Throwable, boolean, boolean)" +
            " but is guarded by version checks")
    private GofException(Http2Error error, String message, ShutdownHint shutdownHint, boolean shared) {
        super(message, null, false, true);
        assert shared;
        this.error = checkNotNull(error, "error");
        this.shutdownHint = checkNotNull(shutdownHint, "shutdownHint");
    }

    public Http2Error error() {
        return error;
    }

    /**
     * Provide a hint as to what type of shutdown should be executed. Note this hint may be ignored.
     */
    public ShutdownHint shutdownHint() {
        return shutdownHint;
    }

    /**
     * Use if an error has occurred which can not be isolated to a single stream, but instead applies
     * to the entire connection.
     * @param error The type of error as defined by the HTTP/2 specification.
     * @param fmt String with the content and format for the additional debug data.
     * @param args Objects which fit into the format defined by {@code fmt}.
     * @return An exception which can be translated into an HTTP/2 error.
     */
    public static GofException connectionError(Http2Error error, String fmt, Object... args) {
        return new GofException(error, formatErrorMessage(fmt, args));
    }

    /**
     * Use if an error has occurred which can not be isolated to a single stream, but instead applies
     * to the entire connection.
     * @param error The type of error as defined by the HTTP/2 specification.
     * @param cause The object which caused the error.
     * @param fmt String with the content and format for the additional debug data.
     * @param args Objects which fit into the format defined by {@code fmt}.
     * @return An exception which can be translated into an HTTP/2 error.
     */
    public static GofException connectionError(Http2Error error, Throwable cause,
                                                 String fmt, Object... args) {
        return new GofException(error, formatErrorMessage(fmt, args), cause);
    }

    /**
     * Use if an error has occurred which can not be isolated to a single stream, but instead applies
     * to the entire connection.
     * @param error The type of error as defined by the HTTP/2 specification.
     * @param fmt String with the content and format for the additional debug data.
     * @param args Objects which fit into the format defined by {@code fmt}.
     * @return An exception which can be translated into an HTTP/2 error.
     */
    public static GofException closedStreamError(Http2Error error, String fmt, Object... args) {
        return new ClosedStreamCreationException(error, formatErrorMessage(fmt, args));
    }

    public static GofException streamError(int id, Http2Error error, String fmt, Object... args) {
        return new StreamException(id, error, formatErrorMessage(fmt, args));
    }

    public static GofException streamError(int id, Http2Error error, Throwable cause,
                                             String fmt, Object... args) {
        return new StreamException(id, error, formatErrorMessage(fmt, args), cause);
    }

    private static String formatErrorMessage(String fmt, Object[] args) {
        if (fmt == null) {
            if (args == null || args.length == 0) {
                return "Unexpected error";
            }
            return "Unexpected error: " + Arrays.toString(args);
        }
        return String.format(fmt, args);
    }

    /**
     * Check if an exception is isolated to a single stream or the entire connection.
     * @param e The exception to check.
     * @return {@code true} if {@code e} is an instance of {@link StreamException}.
     * {@code false} otherwise.
     */
    public static boolean isStreamError(GofException e) {
        return e instanceof StreamException;
    }

    public static int streamId(GofException e) {
        return isStreamError(e) ? ((StreamException) e).streamId() : 0;
    }

    /**
     * Provides a hint as to if shutdown is justified, what type of shutdown should be executed.
     */
    public enum ShutdownHint {
        /**
         * Do not shutdown the underlying channel.
         */
        NO_SHUTDOWN,
        /**
         * Attempt to execute a "graceful" shutdown. The definition of "graceful" is left to the implementation.
         * An example of "graceful" would be wait for some amount of time until all active streams are closed.
         */
        GRACEFUL_SHUTDOWN,
        /**
         * Close the channel immediately after a {@code GOAWAY} is sent.
         */
        HARD_SHUTDOWN
    }

    /**
     * Used when a stream creation attempt fails but may be because the stream was previously closed.
     */
    public static final class ClosedStreamCreationException extends GofException {
        private static final long serialVersionUID = -6746542974372246206L;

        public ClosedStreamCreationException(Http2Error error) {
            super(error);
        }

        public ClosedStreamCreationException(Http2Error error, String message) {
            super(error, message);
        }

        public ClosedStreamCreationException(Http2Error error, String message, Throwable cause) {
            super(error, message, cause);
        }
    }

    /**
     * Represents an exception that can be isolated to a single stream (as opposed to the entire connection).
     */
    public static class StreamException extends GofException {
        private static final long serialVersionUID = 602472544416984384L;
        private final int streamId;

        StreamException(int streamId, Http2Error error, String message) {
            super(error, message, ShutdownHint.NO_SHUTDOWN);
            this.streamId = streamId;
        }

        StreamException(int streamId, Http2Error error, String message, Throwable cause) {
            super(error, message, cause, ShutdownHint.NO_SHUTDOWN);
            this.streamId = streamId;
        }

        public int streamId() {
            return streamId;
        }
    }

    public static final class HeaderListSizeException extends StreamException {
        private static final long serialVersionUID = -8807603212183882637L;

        private final boolean decode;

        HeaderListSizeException(int streamId, Http2Error error, String message, boolean decode) {
            super(streamId, error, message);
            this.decode = decode;
        }

        public boolean duringDecode() {
            return decode;
        }
    }

    /**
     * Provides the ability to handle multiple stream exceptions with one throw statement.
     */
    public static final class CompositeStreamException extends GofException implements Iterable<StreamException> {
        private static final long serialVersionUID = 7091134858213711015L;
        private final List<StreamException> exceptions;

        public CompositeStreamException(Http2Error error, int initialCapacity) {
            super(error, ShutdownHint.NO_SHUTDOWN);
            exceptions = new ArrayList<StreamException>(initialCapacity);
        }

        public void add(StreamException e) {
            exceptions.add(e);
        }

        @Override
        public Iterator<StreamException> iterator() {
            return exceptions.iterator();
        }
    }

    private static final class StacklessGofException extends GofException {

        private static final long serialVersionUID = 1077888485687219443L;

        StacklessGofException(Http2Error error, String message, ShutdownHint shutdownHint) {
            super(error, message, shutdownHint);
        }

        StacklessGofException(Http2Error error, String message, ShutdownHint shutdownHint, boolean shared) {
            super(error, message, shutdownHint, shared);
        }

        // Override fillInStackTrace() so we not populate the backtrace via a native call and so leak the
        // Classloader.
        @Override
        public Throwable fillInStackTrace() {
            return this;
        }
    }

    /**
     * Creates a buffer containing the error message from the given exception. If the cause is
     * {@code null} returns an empty buffer.
     */
    public static ByteBuf toByteBuf(ChannelHandlerContext ctx, Throwable cause) {
        if (cause == null || cause.getMessage() == null) {
            return Unpooled.EMPTY_BUFFER;
        }

        return ByteBufUtil.writeUtf8(ctx.alloc(), cause.getMessage());
    }
}
