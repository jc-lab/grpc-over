package kr.jclab.grpcover.gofprotocol;

public class GofUtils {
    /**
     * Iteratively looks through the causality chain for the given exception and returns the first
     * {@link GofException} or {@code null} if none.
     */
    public static GofException getEmbeddedGofException(Throwable cause) {
        while (cause != null) {
            if (cause instanceof GofException) {
                return (GofException) cause;
            }
            cause = cause.getCause();
        }
        return null;
    }
}
