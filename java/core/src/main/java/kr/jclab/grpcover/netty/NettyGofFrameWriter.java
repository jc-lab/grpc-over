package kr.jclab.grpcover.netty;

import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.util.concurrent.EventExecutor;
import kr.jclab.grpcover.core.protocol.v1.GofProto;
import kr.jclab.grpcover.gofprotocol.FrameSizePolicy;
import kr.jclab.grpcover.gofprotocol.FrameWriter;
import kr.jclab.grpcover.gofprotocol.GofException;

public class NettyGofFrameWriter implements FrameWriter, FrameSizePolicy {
    public static final int DEFAULT_MAX_FRAME_SIZE = 63 * 1024;
    public static final int MAX_FRAME_SIZE_LOWER_BOUND = 0x4000;
    public static final int MAX_FRAME_SIZE_UPPER_BOUND = 0xffffff;

    private int maxFrameSize = DEFAULT_MAX_FRAME_SIZE;

    @Override
    public void maxFrameSize(int max) throws GofException {
        if (maxFrameSize < MAX_FRAME_SIZE_LOWER_BOUND || maxFrameSize > MAX_FRAME_SIZE_UPPER_BOUND) {
            return ; // TODO: throw exception
        }
        maxFrameSize = max;
    }

    @Override
    public int maxFrameSize() {
        return maxFrameSize;
    }

    @Override
    public ChannelFuture writeHeaders(ChannelHandlerContext ctx, int streamId, GofProto.Header headers, boolean endStream, ChannelPromise promise) {
        GofProto.Frame.Builder builder = GofProto.Frame.newBuilder()
                .setStreamId(streamId)
                .setType(GofProto.FrameType.HEADER)
                .setHeader(headers);
        if (endStream) {
            builder.addFlag(GofProto.FrameFlags.END_STREAM);
        }
        return ctx.write(builder.build(), promise);
    }

    @Override
    public ChannelFuture writeRstStream(ChannelHandlerContext ctx, int streamId, long errorCode, ChannelPromise promise) {
        GofProto.Frame.Builder builder = GofProto.Frame.newBuilder()
                .setStreamId(streamId)
                .setType(GofProto.FrameType.RST)
                .setErrorCode(errorCode);
        return ctx.write(builder.build(), promise);
    }

    @Override
    public ChannelFuture writePing(ChannelHandlerContext ctx, boolean ack, long data, ChannelPromise promise) {
        GofProto.Frame.Builder builder = GofProto.Frame.newBuilder()
                .setType(ack ? GofProto.FrameType.PONG : GofProto.FrameType.PING)
                .setPingData(data);
        return ctx.write(builder.build(), promise);
    }

    @Override
    public ChannelFuture writeGoAway(ChannelHandlerContext ctx, int lastStreamId, long errorCode, ByteBuf debugData, ChannelPromise promise) {
        GofProto.Frame.Builder builder = GofProto.Frame.newBuilder()
                .setType(GofProto.FrameType.GO_AWAY)
                .setStreamId(lastStreamId)
                .setErrorCode(errorCode);
        if (debugData != null && debugData.readableBytes() > 0) {
            builder.setData(ByteString.copyFrom(debugData.nioBuffer()));
            debugData.release();
        }
        return ctx.write(builder.build(), promise);
    }

    @Override
    public ChannelFuture writeData(ChannelHandlerContext ctx, int streamId, ByteBuf data, boolean endStream, ChannelPromise promise) {
        int remainingData = (data != null) ? data.readableBytes() : 0;

        SimpleChannelPromiseAggregator promiseAggregator =
                new SimpleChannelPromiseAggregator(promise, ctx.channel(), ctx.executor());

        try {
            do {
                boolean isLastFrame = remainingData <= maxFrameSize;
                int currentFrameSize = isLastFrame ? remainingData : maxFrameSize;

                GofProto.Frame.Builder builder = GofProto.Frame.newBuilder()
                        .setStreamId(streamId)
                        .setType(GofProto.FrameType.DATA);
                if (isLastFrame && endStream) {
                    builder.addFlag(GofProto.FrameFlags.END_STREAM);
                }
                if (data != null) {
                    builder.setData(ByteString.copyFrom(data.readSlice(currentFrameSize).nioBuffer()));
                }
                ctx.write(builder.build(), promiseAggregator.newPromise());

                remainingData -= currentFrameSize;
            } while (remainingData > 0);
        } catch (Exception e) {
            promiseAggregator.setFailure(e);
        } finally {
            if (data != null) {
                data.release();
            }
        }

        return promiseAggregator.doneAllocatingPromises();
    }

    @Override
    public void close() {

    }

    /**
     * Provides the ability to associate the outcome of multiple {@link ChannelPromise}
     * objects into a single {@link ChannelPromise} object.
     */
    static final class SimpleChannelPromiseAggregator extends DefaultChannelPromise {
        private final ChannelPromise promise;
        private int expectedCount;
        private int doneCount;
        private Throwable aggregateFailure;
        private boolean doneAllocating;

        SimpleChannelPromiseAggregator(ChannelPromise promise, Channel c, EventExecutor e) {
            super(c, e);
            assert promise != null && !promise.isDone();
            this.promise = promise;
        }

        /**
         * Allocate a new promise which will be used to aggregate the overall success of this promise aggregator.
         * @return A new promise which will be aggregated.
         * {@code null} if {@link #doneAllocatingPromises()} was previously called.
         */
        public ChannelPromise newPromise() {
            assert !doneAllocating : "Done allocating. No more promises can be allocated.";
            ++expectedCount;
            return this;
        }

        /**
         * Signify that no more {@link #newPromise()} allocations will be made.
         * The aggregation can not be successful until this method is called.
         * @return The promise that is the aggregation of all promises allocated with {@link #newPromise()}.
         */
        public ChannelPromise doneAllocatingPromises() {
            if (!doneAllocating) {
                doneAllocating = true;
                if (doneCount == expectedCount || expectedCount == 0) {
                    return setPromise();
                }
            }
            return this;
        }

        @Override
        public boolean tryFailure(Throwable cause) {
            if (allowFailure()) {
                ++doneCount;
                setAggregateFailure(cause);
                if (allPromisesDone()) {
                    return tryPromise();
                }
                // TODO: We break the interface a bit here.
                // Multiple failure events can be processed without issue because this is an aggregation.
                return true;
            }
            return false;
        }

        /**
         * Fail this object if it has not already been failed.
         * <p>
         * This method will NOT throw an {@link IllegalStateException} if called multiple times
         * because that may be expected.
         */
        @Override
        public ChannelPromise setFailure(Throwable cause) {
            if (allowFailure()) {
                ++doneCount;
                setAggregateFailure(cause);
                if (allPromisesDone()) {
                    return setPromise();
                }
            }
            return this;
        }

        @Override
        public ChannelPromise setSuccess(Void result) {
            if (awaitingPromises()) {
                ++doneCount;
                if (allPromisesDone()) {
                    setPromise();
                }
            }
            return this;
        }

        @Override
        public boolean trySuccess(Void result) {
            if (awaitingPromises()) {
                ++doneCount;
                if (allPromisesDone()) {
                    return tryPromise();
                }
                // TODO: We break the interface a bit here.
                // Multiple success events can be processed without issue because this is an aggregation.
                return true;
            }
            return false;
        }

        private boolean allowFailure() {
            return awaitingPromises() || expectedCount == 0;
        }

        private boolean awaitingPromises() {
            return doneCount < expectedCount;
        }

        private boolean allPromisesDone() {
            return doneCount == expectedCount && doneAllocating;
        }

        private ChannelPromise setPromise() {
            if (aggregateFailure == null) {
                promise.setSuccess();
                return super.setSuccess(null);
            } else {
                promise.setFailure(aggregateFailure);
                return super.setFailure(aggregateFailure);
            }
        }

        private boolean tryPromise() {
            if (aggregateFailure == null) {
                promise.trySuccess();
                return super.trySuccess(null);
            } else {
                promise.tryFailure(aggregateFailure);
                return super.tryFailure(aggregateFailure);
            }
        }

        private void setAggregateFailure(Throwable cause) {
            if (aggregateFailure == null) {
                aggregateFailure = cause;
            }
        }
    }
}
