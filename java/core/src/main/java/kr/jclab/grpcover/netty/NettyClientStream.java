/*
 * Copyright 2015 The gRPC Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kr.jclab.grpcover.netty;

import com.google.common.base.Preconditions;
import io.grpc.*;
import io.grpc.internal.*;
import io.grpc.internal.ClientStreamListener.RpcProgress;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoop;
import io.perfmark.PerfMark;
import io.perfmark.Tag;
import kr.jclab.grpcover.core.protocol.v1.GofProto;
import kr.jclab.grpcover.gofprotocol.GofStream;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.*;
import static io.netty.buffer.Unpooled.EMPTY_BUFFER;

/**
 * Client stream for a Netty transport. Must only be called from the sending application
 * thread.
 */
class NettyClientStream extends AbstractClientStream {
  private static final InternalMethodDescriptor methodDescriptorAccessor =
      new InternalMethodDescriptor(
          NettyClientTransport.class.getName().contains("grpc.netty.shaded")
              ? InternalKnownTransport.NETTY_SHADED : InternalKnownTransport.NETTY);

  private final Sink sink = new Sink();
  private final TransportState state;
  private final WriteQueue writeQueue;
  private final MethodDescriptor<?, ?> method;
  private String authority;

  NettyClientStream(
      TransportState state,
      MethodDescriptor<?, ?> method,
      Metadata headers,
      Channel channel,
      String authority,
      StatsTraceContext statsTraceCtx,
      TransportTracer transportTracer,
      CallOptions callOptions) {
    super(
        new NettyWritableBufferAllocator(channel.alloc()),
        statsTraceCtx,
        transportTracer,
        headers,
        callOptions,
        false);
    this.state = checkNotNull(state, "transportState");
    this.writeQueue = state.handler.getWriteQueue();
    this.method = checkNotNull(method, "method");
    this.authority = checkNotNull(authority, "authority");
  }

  @Override
  protected TransportState transportState() {
    return state;
  }

  @Override
  protected Sink abstractClientStreamSink() {
    return sink;
  }

  @Override
  public void setAuthority(String authority) {
    this.authority = checkNotNull(authority, "authority");
  }

  @Override
  public Attributes getAttributes() {
    return state.handler.getAttributes();
  }

  private class Sink implements AbstractClientStream.Sink {

    @Override
    public void writeHeaders(Metadata headers, byte[] requestPayload) {
      PerfMark.startTask("NettyClientStream$Sink.writeHeaders");
      try {
        writeHeadersInternal(headers, requestPayload);
      } finally {
        PerfMark.stopTask("NettyClientStream$Sink.writeHeaders");
      }
    }

    private void writeHeadersInternal(Metadata headers, byte[] requestPayload) {
      // Convert the headers into Netty HTTP/2 headers.
      GofProto.Header gofHeaders = Utils.convertClientHeaders(headers, method.getFullMethodName(), authority);

      ChannelFutureListener failureListener = new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
          if (!future.isSuccess()) {
            // Stream creation failed. Close the stream if not already closed.
            // When the channel is shutdown, the lifecycle manager has a better view of the failure,
            // especially before negotiation completes (because the negotiator commonly doesn't
            // receive the exceptionCaught because NettyClientHandler does not propagate it).
            Status s = transportState().handler.getLifecycleManager().getShutdownStatus();
            if (s == null) {
              s = transportState().statusFromFailedFuture(future);
            }
            if (transportState().isNonExistent()) {
              transportState().transportReportStatus(
                  s, RpcProgress.MISCARRIED, true, new Metadata());
            } else {
              transportState().transportReportStatus(
                  s, RpcProgress.PROCESSED, true, new Metadata());
            }
          }
        }
      };
      // Write the command requesting the creation of the stream.
      writeQueue.enqueue(
          new CreateStreamCommand(gofHeaders, transportState(), shouldBeCountedForInUse()),
          !method.getType().clientSendsOneMessage() || (requestPayload != null)).addListener(failureListener);
    }

    private void writeFrameInternal(
        WritableBuffer frame, boolean endOfStream, boolean flush, final int numMessages) {
      Preconditions.checkArgument(numMessages >= 0);
      ByteBuf bytebuf =
          frame == null ? EMPTY_BUFFER : ((NettyWritableBuffer) frame).bytebuf().touch();
      final int numBytes = bytebuf.readableBytes();
      if (numBytes > 0) {
        // Add the bytes to outbound flow control.
        onSendingBytes(numBytes);
        writeQueue.enqueue(new SendGrpcFrameCommand(transportState(), bytebuf, endOfStream), flush)
            .addListener(new ChannelFutureListener() {
              @Override
              public void operationComplete(ChannelFuture future) throws Exception {
                // If the future succeeds when http2stream is null, the stream has been cancelled
                // before it began and Netty is purging pending writes from the flow-controller.
                if (future.isSuccess() && transportState().http2Stream() != null) {
                  // Remove the bytes from outbound flow control, optionally notifying
                  // the client that they can send more bytes.
                  transportState().onSentBytes(numBytes);
                  NettyClientStream.this.getTransportTracer().reportMessageSent(numMessages);
                }
              }
            });
      } else {
        // The frame is empty and will not impact outbound flow control. Just send it.
        writeQueue.enqueue(
            new SendGrpcFrameCommand(transportState(), bytebuf, endOfStream), flush);
      }
    }

    @Override
    public void writeFrame(
        WritableBuffer frame, boolean endOfStream, boolean flush, int numMessages) {
      PerfMark.startTask("NettyClientStream$Sink.writeFrame");
      try {
        writeFrameInternal(frame, endOfStream, flush, numMessages);
      } finally {
        PerfMark.stopTask("NettyClientStream$Sink.writeFrame");
      }
    }

    @Override
    public void cancel(Status status) {
      PerfMark.startTask("NettyClientStream$Sink.cancel");
      try {
        writeQueue.enqueue(new CancelClientStreamCommand(transportState(), status), true);
      } finally {
        PerfMark.stopTask("NettyClientStream$Sink.cancel");
      }
    }
  }

  /** This should only be called from the transport thread. */
  public abstract static class TransportState extends AbstractGofClientStream.AbstractTransportState
      implements StreamIdHolder {
    private static final int NON_EXISTENT_ID = -1;

    private final String methodName;
    private final NettyClientHandler handler;
    private final EventLoop eventLoop;
    private int id;
    private GofStream gofStream;
    private Tag tag;

    protected TransportState(
        NettyClientHandler handler,
        EventLoop eventLoop,
        int maxMessageSize,
        StatsTraceContext statsTraceCtx,
        TransportTracer transportTracer,
        String methodName) {
      super(maxMessageSize, statsTraceCtx, transportTracer);
      this.methodName = checkNotNull(methodName, "methodName");
      this.handler = checkNotNull(handler, "handler");
      this.eventLoop = checkNotNull(eventLoop, "eventLoop");
      tag = PerfMark.createTag(methodName);
    }

    @Override
    public int id() {
      // id should be positive
      return id;
    }

    public void setId(int id) {
      checkArgument(id > 0, "id must be positive %s", id);
      checkState(this.id == 0, "id has been previously set: %s", this.id);
      this.id = id;
      this.tag = PerfMark.createTag(methodName, id);
    }

    /**
     * Marks the stream state as if it had never existed.  This can happen if the stream is
     * cancelled after it is created, but before it has been started.
     */
    void setNonExistent() {
      checkState(this.id == 0, "Id has been previously set: %s", this.id);
      this.id = NON_EXISTENT_ID;
    }

    boolean isNonExistent() {
      return this.id == NON_EXISTENT_ID || this.id == 0;
    }

    /**
     * Sets the underlying Netty {@link GofStream} for this stream. This must be called in the
     * context of the transport thread.
     */
    public void setGofStream(GofStream gofStream) {
      checkNotNull(gofStream, "http2Stream");
      checkState(this.gofStream == null, "Can only set http2Stream once");
      this.gofStream = gofStream;

      // Now that the stream has actually been initialized, call the listener's onReady callback if
      // appropriate.
      onStreamAllocated();
      getTransportTracer().reportLocalStreamStarted();
    }

    /**
     * Gets the underlying Netty {@link GofStream} for this stream.
     */
    @Nullable
    public GofStream http2Stream() {
      return gofStream;
    }

    /**
     * Intended to be overridden by NettyClientTransport, which has more information about failures.
     * May only be called from event loop.
     */
    protected abstract Status statusFromFailedFuture(ChannelFuture f);

    protected void gofProcessingFailed(Status status, boolean stopDelivery, Metadata trailers) {
      transportReportStatus(status, stopDelivery, trailers);
      handler.getWriteQueue().enqueue(new CancelClientStreamCommand(this, status), true);
    }

    @Override
    public void runOnTransportThread(final Runnable r) {
      if (eventLoop.inEventLoop()) {
        r.run();
      } else {
        eventLoop.execute(r);
      }
    }

    @Override
    public void bytesRead(int processedBytes) {
      // handler.returnProcessedBytes(gofStream, processedBytes);
      handler.getWriteQueue().scheduleFlush();
    }

    @Override
    public void deframeFailed(Throwable cause) {
      gofProcessingFailed(Status.fromThrowable(cause), true, new Metadata());
    }

    void transportHeadersReceived(GofProto.Header headers, boolean endOfStream) {
      if (endOfStream) {
        if (!isOutboundClosed()) {
          handler.getWriteQueue().enqueue(new CancelClientStreamCommand(this, null), true);
        }
        transportTrailersReceived(Utils.convertTrailers(headers));
      } else {
        transportHeadersReceived(Utils.convertHeaders(headers));
      }
    }

    void transportDataReceived(ByteBuf frame, boolean endOfStream) {
      transportDataReceived(new NettyReadableBuffer(frame.retain()), endOfStream);
    }

    @Override
    public final Tag tag() {
      return tag;
    }
  }
}
