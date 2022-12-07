package kr.jclab.grpcover.gofprotocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.PromiseNotifier;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static io.grpc.internal.GrpcUtil.Http2Error.*;
import static io.netty.util.internal.ObjectUtil.checkNotNull;
import static io.netty.util.internal.ObjectUtil.checkPositiveOrZero;
import static java.lang.Integer.MAX_VALUE;
import static kr.jclab.grpcover.gofprotocol.GofException.connectionError;
import static kr.jclab.grpcover.gofprotocol.GofException.streamError;

@Slf4j
public class DefaultGofConnection implements GofConnection {
    // Fields accessed by inner classes
    final IntObjectMap<GofStream> streamMap = new IntObjectHashMap<GofStream>();
    final DefaultEndpoint localEndpoint;
    final DefaultEndpoint remoteEndpoint;

    /**
     * We chose a {@link List} over a {@link Set} to avoid allocating an {@link Iterator} objects when iterating over
     * the listeners.
     * <p>
     * Initial size of 4 because the default configuration currently has 3 listeners
     * (local/remote flow controller and StreamByteDistributor) and we leave room for 1 extra.
     * We could be more aggressive but the ArrayList resize will double the size if we are too small.
     */
    final List<Listener> listeners = new ArrayList<Listener>(4);
    final ActiveStreams activeStreams;
    Promise<Void> closePromise;

    /**
     * Creates a new connection with the given settings.
     * @param server whether or not this end-point is the server-side of the HTTP/2 connection.
     */
    public DefaultGofConnection(boolean server) {
        activeStreams = new ActiveStreams(listeners);
        // Reserved streams are excluded from the SETTINGS_MAX_CONCURRENT_STREAMS limit according to [1] and the RFC
        // doesn't define a way to communicate the limit on reserved streams. We rely upon the peer to send RST_STREAM
        // in response to any locally enforced limits being exceeded [2].
        // [1] https://tools.ietf.org/html/rfc7540#section-5.1.2
        // [2] https://tools.ietf.org/html/rfc7540#section-8.2.2
        localEndpoint = new DefaultEndpoint(server, server ? MAX_VALUE : 100);
        remoteEndpoint = new DefaultEndpoint(!server, 100);
    }

    /**
     * Determine if {@link #close(Promise)} has been called and no more streams are allowed to be created.
     */
    final boolean isClosed() {
        return closePromise != null;
    }

    @Override
    public Future<Void> close(final Promise<Void> promise) {
        checkNotNull(promise, "promise");
        // Since we allow this method to be called multiple times, we must make sure that all the promises are notified
        // when all streams are removed and the close operation completes.
        if (closePromise != null) {
            if (closePromise == promise) {
                // Do nothing
            } else if (promise instanceof ChannelPromise && ((ChannelFuture) closePromise).isVoid()) {
                closePromise = promise;
            } else {
                PromiseNotifier.cascade(closePromise, promise);
            }
        } else {
            closePromise = promise;
        }
        if (isStreamMapEmpty()) {
            promise.trySuccess(null);
            return promise;
        }

        Iterator<IntObjectMap.PrimitiveEntry<GofStream>> itr = streamMap.entries().iterator();
        // We must take care while iterating the streamMap as to not modify while iterating in case there are other code
        // paths iterating over the active streams.
        if (activeStreams.allowModifications()) {
            activeStreams.incrementPendingIterations();
            try {
                while (itr.hasNext()) {
                    DefaultStream stream = (DefaultStream) itr.next().value();
                    // If modifications of the activeStream map is allowed, then a stream close operation will also
                    // modify the streamMap. Pass the iterator in so that remove will be called to prevent
                    // concurrent modification exceptions.
                    stream.close(itr);
                }
            } finally {
                activeStreams.decrementPendingIterations();
            }
        } else {
            while (itr.hasNext()) {
                DefaultStream stream = (DefaultStream) itr.next().value();
                // We are not allowed to make modifications, so the close calls will be executed after this
                // iteration completes.
                stream.close();
            }
        }
        return closePromise;
    }

    @Override
    public void addListener(Listener listener) {
        listeners.add(listener);
    }

    @Override
    public void removeListener(Listener listener) {
        listeners.remove(listener);
    }

    @Override
    public boolean isServer() {
        return localEndpoint.isServer();
    }

    @Override
    public GofStream stream(int streamId) {
        return streamMap.get(streamId);
    }

    @Override
    public boolean streamMayHaveExisted(int streamId) {
        return remoteEndpoint.mayHaveCreatedStream(streamId) || localEndpoint.mayHaveCreatedStream(streamId);
    }

    @Override
    public int numActiveStreams() {
        return activeStreams.size();
    }

    @Override
    public GofStream forEachActiveStream(GofStreamVisitor visitor) throws GofException {
        return activeStreams.forEachActiveStream(visitor);
    }

    @Override
    public Endpoint local() {
        return localEndpoint;
    }

    @Override
    public Endpoint remote() {
        return remoteEndpoint;
    }

    @Override
    public boolean goAwayReceived() {
        return localEndpoint.lastStreamKnownByPeer >= 0;
    }

    @Override
    public void goAwayReceived(final int lastKnownStream, long errorCode, ByteBuf debugData) throws GofException {
        if (localEndpoint.lastStreamKnownByPeer() >= 0 && localEndpoint.lastStreamKnownByPeer() < lastKnownStream) {
            throw connectionError(PROTOCOL_ERROR, "lastStreamId MUST NOT increase. Current value: %d new value: %d",
                    localEndpoint.lastStreamKnownByPeer(), lastKnownStream);
        }

        localEndpoint.lastStreamKnownByPeer(lastKnownStream);
        for (int i = 0; i < listeners.size(); ++i) {
            try {
                listeners.get(i).onGoAwayReceived(lastKnownStream, errorCode, debugData);
            } catch (Throwable cause) {
                log.error("Caught Throwable from listener onGoAwayReceived.", cause);
            }
        }

        closeStreamsGreaterThanLastKnownStreamId(lastKnownStream, localEndpoint);
    }

    @Override
    public boolean goAwaySent() {
        return remoteEndpoint.lastStreamKnownByPeer >= 0;
    }

    @Override
    public boolean goAwaySent(final int lastKnownStream, long errorCode, ByteBuf debugData) throws GofException {
        if (remoteEndpoint.lastStreamKnownByPeer() >= 0) {
            // Protect against re-entrancy. Could happen if writing the frame fails, and error handling
            // treating this is a connection handler and doing a graceful shutdown...
            if (lastKnownStream == remoteEndpoint.lastStreamKnownByPeer()) {
                return false;
            }
            if (lastKnownStream > remoteEndpoint.lastStreamKnownByPeer()) {
                throw connectionError(PROTOCOL_ERROR, "Last stream identifier must not increase between " +
                                "sending multiple GOAWAY frames (was '%d', is '%d').",
                        remoteEndpoint.lastStreamKnownByPeer(), lastKnownStream);
            }
        }

        remoteEndpoint.lastStreamKnownByPeer(lastKnownStream);
        for (int i = 0; i < listeners.size(); ++i) {
            try {
                listeners.get(i).onGoAwaySent(lastKnownStream, errorCode, debugData);
            } catch (Throwable cause) {
                log.error("Caught Throwable from listener onGoAwaySent.", cause);
            }
        }

        closeStreamsGreaterThanLastKnownStreamId(lastKnownStream, remoteEndpoint);
        return true;
    }

    private void closeStreamsGreaterThanLastKnownStreamId(
            final int lastKnownStream,
            final DefaultEndpoint endpoint
    ) throws GofException {
        forEachActiveStream(new GofStreamVisitor() {
            @Override
            public boolean visit(GofStream stream) {
                if (stream.id() > lastKnownStream && endpoint.isValidStreamId(stream.id())) {
                    stream.close();
                }
                return true;
            }
        });
    }

    /**
     * Determine if {@link #streamMap} only contains the connection stream.
     */
    private boolean isStreamMapEmpty() {
        return streamMap.size() == 1;
    }

    /**
     * Remove a stream from the {@link #streamMap}.
     * @param stream the stream to remove.
     * @param itr an iterator that may be pointing to the stream during iteration and {@link Iterator#remove()} will be
     * used if non-{@code null}.
     */
    void removeStream(GofStream stream, Iterator<?> itr) {
        final boolean removed;
        if (itr == null) {
            removed = streamMap.remove(stream.id()) != null;
        } else {
            itr.remove();
            removed = true;
        }

        if (removed) {
            for (int i = 0; i < listeners.size(); i++) {
                try {
                    listeners.get(i).onStreamRemoved(stream);
                } catch (Throwable cause) {
                    log.error("Caught Throwable from listener onStreamRemoved.", cause);
                }
            }

            if (closePromise != null && isStreamMapEmpty()) {
                closePromise.trySuccess(null);
            }
        }
    }

    void notifyClosed(GofStream stream) {
        for (int i = 0; i < listeners.size(); i++) {
            try {
                listeners.get(i).onStreamClosed(stream);
            } catch (Throwable cause) {
                log.error("Caught Throwable from listener onStreamClosed.", cause);
            }
        }
    }

    private final class DefaultStream implements GofStream {
        private static final byte META_STATE_SENT_RST = 1;
        private static final byte META_STATE_SENT_HEADERS = 1 << 1;
        private static final byte META_STATE_SENT_TRAILERS = 1 << 2;
        private static final byte META_STATE_SENT_PUSHPROMISE = 1 << 3;
        private static final byte META_STATE_RECV_HEADERS = 1 << 4;
        private static final byte META_STATE_RECV_TRAILERS = 1 << 5;

        private final DefaultEndpoint endpoint;
        private final int id;
        private final HashMap<Integer, Object> properties = new HashMap<>();

        private State state = State.OPEN; // IDLE?
        private byte metaState = 0;

        public DefaultStream(DefaultEndpoint endpoint, int id) {
            this.endpoint = endpoint;
            this.id = id;
        }

        @Override
        public int id() {
            return this.id;
        }

        @Override
        public State state() {
            return state;
        }

        @Override
        public <T> void setProperty(PropertyKey<T> key, T value) {
            properties.put(verifyKey(key).index, value);
        }

        @Override
        public <T> T getProperty(PropertyKey<T> key) {
            return (T) properties.get(verifyKey(key).index);
        }

        void close(Iterator<?> itr) {
            if (state == State.CLOSED) {
                return ;
            }

            state = State.CLOSED;

            --endpoint.numStreams;
            activeStreams.deactivate(this, itr);
        }

        @Override
        public void open() throws GofException {
            state = State.OPEN;
            activate();
        }

        void activate() {
            activeStreams.activate(this);
        }

        @Override
        public void close() {
            close(null);
        }

        @Override
        public void closeLocalSide() {
            close();
        }

        @Override
        public void closeRemoteSide() {
            close();
        }

        @Override
        public boolean isResetSent() {
            return (metaState & META_STATE_SENT_RST) != 0;
        }

        @Override
        public void resetSent() {
            metaState |= META_STATE_SENT_RST;
        }

        @Override
        public void headersSent(boolean isInformational) {
            if (!isInformational) {
                metaState |= isHeadersSent() ? META_STATE_SENT_TRAILERS : META_STATE_SENT_HEADERS;
            }
        }

        @Override
        public boolean isHeadersSent() {
            return (metaState & META_STATE_SENT_HEADERS) != 0;
        }
    }

    /**
     * Simple endpoint implementation.
     */
    private final class DefaultEndpoint implements Endpoint {
        private final boolean server;
        /**
         * The minimum stream ID allowed when creating the next stream. This only applies at the time the stream is
         * created. If the ID of the stream being created is less than this value, stream creation will fail. Upon
         * successful creation of a stream, this value is incremented to the next valid stream ID.
         */
        private int nextStreamIdToCreate;
        /**
         * Used for reservation of stream IDs. Stream IDs can be reserved in advance by applications before the streams
         * are actually created.  For example, applications may choose to buffer stream creation attempts as a way of
         * working around {@code SETTINGS_MAX_CONCURRENT_STREAMS}, in which case they will reserve stream IDs for each
         * buffered stream.
         */
        private int nextReservationStreamId;
        private int lastStreamKnownByPeer = -1;
        private int maxStreams;
        private int maxActiveStreams;
        private final int maxReservedStreams;
        // Fields accessed by inner classes
        int numActiveStreams;
        int numStreams;

        DefaultEndpoint(boolean server, int maxReservedStreams) {
            this.server = server;

            // Determine the starting stream ID for this endpoint. Client-initiated streams
            // are odd and server-initiated streams are even. Zero is reserved for the
            // connection. Stream 1 is reserved client-initiated stream for responding to an
            // upgrade from HTTP 1.1.
            if (server) {
                nextStreamIdToCreate = 2;
                nextReservationStreamId = 0;
            } else {
                nextStreamIdToCreate = 1;
                // For manually created client-side streams, 1 is reserved for HTTP upgrade, so start at 3.
                nextReservationStreamId = 1;
            }

            // Push is disallowed by default for servers and allowed for clients.
            maxActiveStreams = MAX_VALUE;
            this.maxReservedStreams = checkPositiveOrZero(maxReservedStreams, "maxReservedStreams");
            updateMaxStreams();
        }

        @Override
        public int incrementAndGetNextStreamId() {
            return nextReservationStreamId >= 0 ? nextReservationStreamId += 2 : nextReservationStreamId;
        }

        private void incrementExpectedStreamId(int streamId) {
            if (streamId > nextReservationStreamId && nextReservationStreamId >= 0) {
                nextReservationStreamId = streamId;
            }
            nextStreamIdToCreate = streamId + 2;
            ++numStreams;
        }

        @Override
        public boolean isValidStreamId(int streamId) {
            return streamId > 0 && server == ((streamId & 1) == 0);
        }

        @Override
        public boolean mayHaveCreatedStream(int streamId) {
            return isValidStreamId(streamId) && streamId <= lastStreamCreated();
        }

        @Override
        public boolean created(GofStream stream) {
            return stream instanceof DefaultStream && ((DefaultStream) stream).endpoint == this;
        }

        @Override
        public boolean canOpenStream() {
            return numActiveStreams < maxActiveStreams;
        }

        @Override
        public GofStream createStream(int streamId) throws GofException {
            checkNewStreamAllowed(streamId);

            DefaultStream stream = new DefaultStream(this, streamId);

            incrementExpectedStreamId(streamId);

            addStream(stream);

            activeStreams.activate(stream);
            return stream;
        }

        @Override
        public boolean isServer() {
            return server;
        }

        private void addStream(GofStream stream) {
            // Add the stream to the map and priority tree.
            streamMap.put(stream.id(), stream);

            // Notify the listeners of the event.
            for (int i = 0; i < listeners.size(); i++) {
                try {
                    listeners.get(i).onStreamAdded(stream);
                } catch (Throwable cause) {
                    log.error("Caught Throwable from listener onStreamAdded.", cause);
                }
            }
        }

        @Override
        public int numActiveStreams() {
            return numActiveStreams;
        }

        @Override
        public int maxActiveStreams() {
            return maxActiveStreams;
        }

        @Override
        public void maxActiveStreams(int maxActiveStreams) {
            this.maxActiveStreams = maxActiveStreams;
            updateMaxStreams();
        }

        @Override
        public int lastStreamCreated() {
            return nextStreamIdToCreate > 1 ? nextStreamIdToCreate - 2 : 0;
        }

        @Override
        public int lastStreamKnownByPeer() {
            return lastStreamKnownByPeer;
        }

        private void lastStreamKnownByPeer(int lastKnownStream) {
            lastStreamKnownByPeer = lastKnownStream;
        }

        private void updateMaxStreams() {
            maxStreams = (int) Math.min(MAX_VALUE, (long) maxActiveStreams + maxReservedStreams);
        }

        private void checkNewStreamAllowed(int streamId) throws GofException {
            if (lastStreamKnownByPeer >= 0 && streamId > lastStreamKnownByPeer) {
                throw streamError(streamId, REFUSED_STREAM,
                        "Cannot create stream %d greater than Last-Stream-ID %d from GOAWAY.",
                        streamId, lastStreamKnownByPeer);
            }
            if (!isValidStreamId(streamId)) {
                if (streamId < 0) {
                    throw new GofNoMoreStreamIdsException();
                }
                throw connectionError(PROTOCOL_ERROR, "Request stream %d is not correct for %s connection", streamId,
                        server ? "server" : "client");
            }
            // This check must be after all id validated checks, but before the max streams check because it may be
            // recoverable to some degree for handling frames which can be sent on closed streams.
            if (streamId < nextStreamIdToCreate) {
                throw GofException.closedStreamError(PROTOCOL_ERROR, "Request stream %d is behind the next expected stream %d",
                        streamId, nextStreamIdToCreate);
            }
            if (nextStreamIdToCreate <= 0) {
                // We exhausted the stream id space that we  can use. Let's signal this back but also signal that
                // we still may want to process active streams.
                throw connectionError(REFUSED_STREAM, "Stream IDs are exhausted for this endpoint."); // GofException.ShutdownHint.GRACEFUL_SHUTDOWN
            }
            if (isClosed()) {
                throw connectionError(INTERNAL_ERROR, "Attempted to create stream id %d after connection was closed",
                        streamId);
            }
        }
    }

    /**
     * Allows events which would modify the collection of active streams to be queued while iterating via {@link
     * #forEachActiveStream(GofStreamVisitor)}.
     */
    interface Event {
        /**
         * Trigger the original intention of this event. Expect to modify the active streams list.
         * <p/>
         * If a {@link RuntimeException} object is thrown it will be logged and <strong>not propagated</strong>.
         * Throwing from this method is not supported and is considered a programming error.
         */
        void process();
    }

    /**
     * Manages the list of currently active streams.  Queues any {@link Event}s that would modify the list of
     * active streams in order to prevent modification while iterating.
     */
    private final class ActiveStreams {
        private final List<Listener> listeners;
        private final Queue<Event> pendingEvents = new ArrayDeque<Event>(4);
        private final Set<GofStream> streams = new LinkedHashSet<GofStream>();
        private int pendingIterations;

        ActiveStreams(List<Listener> listeners) {
            this.listeners = listeners;
        }

        public int size() {
            return streams.size();
        }

        public void activate(final DefaultStream stream) {
            if (allowModifications()) {
                addToActiveStreams(stream);
            } else {
                pendingEvents.add(new Event() {
                    @Override
                    public void process() {
                        addToActiveStreams(stream);
                    }
                });
            }
        }

        public void deactivate(final DefaultStream stream, final Iterator<?> itr) {
            if (allowModifications() || itr != null) {
                removeFromActiveStreams(stream, itr);
            } else {
                pendingEvents.add(new Event() {
                    @Override
                    public void process() {
                        removeFromActiveStreams(stream, itr);
                    }
                });
            }
        }

        public GofStream forEachActiveStream(GofStreamVisitor visitor) throws GofException {
            incrementPendingIterations();
            try {
                for (GofStream stream : streams) {
                    if (!visitor.visit(stream)) {
                        return stream;
                    }
                }
                return null;
            } finally {
                decrementPendingIterations();
            }
        }

        void addToActiveStreams(DefaultStream stream) {
            if (streams.add(stream)) {
                // Update the number of active streams initiated by the endpoint.
                stream.endpoint.numActiveStreams++;

                for (int i = 0; i < listeners.size(); i++) {
                    try {
                        listeners.get(i).onStreamActive(stream);
                    } catch (Throwable cause) {
                        log.error("Caught Throwable from listener onStreamActive.", cause);
                    }
                }
            }
        }

        void removeFromActiveStreams(DefaultStream stream, Iterator<?> itr) {
            if (streams.remove(stream)) {
                // Update the number of active streams initiated by the endpoint.
                stream.endpoint.numActiveStreams--;
                notifyClosed(stream);
            }
            removeStream(stream, itr);
        }

        boolean allowModifications() {
            return pendingIterations == 0;
        }

        void incrementPendingIterations() {
            ++pendingIterations;
        }

        void decrementPendingIterations() {
            --pendingIterations;
            if (allowModifications()) {
                for (;;) {
                    Event event = pendingEvents.poll();
                    if (event == null) {
                        break;
                    }
                    try {
                        event.process();
                    } catch (Throwable cause) {
                        log.error("Caught Throwable while processing pending ActiveStreams$Event.", cause);
                    }
                }
            }
        }
    }

    private final AtomicInteger propertyKeyCount = new AtomicInteger(0);

    @Override
    public <T> PropertyKey<T> newKey() {
        return new DefaultPropertyKey();
    }

    final DefaultPropertyKey verifyKey(PropertyKey key) {
        return checkNotNull((DefaultPropertyKey) key, "key").verifyConnection(this);
    }

    final class DefaultPropertyKey implements PropertyKey {
        final int index;

        DefaultPropertyKey() {
            this.index = propertyKeyCount.incrementAndGet();
        }

        DefaultPropertyKey verifyConnection(GofConnection connection) {
            if (connection != DefaultGofConnection.this) {
                throw new IllegalArgumentException("Using a key that was not created by this connection");
            }
            return this;
        }
    }
}
