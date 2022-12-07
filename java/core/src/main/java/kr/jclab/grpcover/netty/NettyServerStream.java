/*
 * Copyright 2014 The gRPC Authors
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
import io.grpc.Attributes;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.internal.AbstractServerStream;
import io.grpc.internal.StatsTraceContext;
import io.grpc.internal.TransportTracer;
import io.grpc.internal.WritableBuffer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoop;
import io.perfmark.Link;
import io.perfmark.PerfMark;
import io.perfmark.Tag;
import kr.jclab.grpcover.core.protocol.v1.GofProto;
import kr.jclab.grpcover.gofprotocol.GofStream;

import java.util.logging.Level;
import java.util.logging.Logger;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Server stream for a Netty HTTP2 transport. Must only be called from the sending application
 * thread.
 */
class NettyServerStream extends AbstractServerStream {
  private static final Logger log = Logger.getLogger(NettyServerStream.class.getName());

  private final Sink sink = new Sink();
  private final TransportState state;
  private final WriteQueue writeQueue;
  private final Attributes attributes;
  private final String authority;
  private final TransportTracer transportTracer;
  private final int streamId;

  public NettyServerStream(
      Channel channel,
      TransportState state,
      Attributes transportAttrs,
      String authority,
      StatsTraceContext statsTraceCtx,
      TransportTracer transportTracer) {
    super(new NettyWritableBufferAllocator(channel.alloc()), statsTraceCtx);
    this.state = checkNotNull(state, "transportState");
    this.writeQueue = state.handler.getWriteQueue();
    this.attributes = checkNotNull(transportAttrs);
    this.authority = authority;
    this.transportTracer = checkNotNull(transportTracer, "transportTracer");
    // Read the id early to avoid reading transportState later.
    this.streamId = transportState().id();
  }

  @Override
  protected TransportState transportState() {
    return state;
  }

  @Override
  protected Sink abstractServerStreamSink() {
    return sink;
  }

  @Override
  public Attributes getAttributes() {
    return attributes;
  }

  @Override
  public String getAuthority() {
    return authority;
  }

  private class Sink implements AbstractServerStream.Sink {
    @Override
    public void writeHeaders(Metadata headers) {
      PerfMark.startTask("NettyServerStream$Sink.writeHeaders");
      try {
        writeQueue.enqueue(
            SendResponseHeadersCommand.createHeaders(
                transportState(),
                Utils.convertServerHeaders(headers)),
            true);
      } finally {
        PerfMark.stopTask("NettyServerStream$Sink.writeHeaders");
      }
    }

    private void writeFrameInternal(WritableBuffer frame, boolean flush, final int numMessages) {
      Preconditions.checkArgument(numMessages >= 0);
      ByteBuf bytebuf = ((NettyWritableBuffer) frame).bytebuf().touch();
      final int numBytes = bytebuf.readableBytes();
      // Add the bytes to outbound flow control.
      onSendingBytes(numBytes);
      writeQueue.enqueue(new SendGrpcFrameCommand(transportState(), bytebuf, false), flush)
          .addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
              // Remove the bytes from outbound flow control, optionally notifying
              // the client that they can send more bytes.
              transportState().onSentBytes(numBytes);
              if (future.isSuccess()) {
                transportTracer.reportMessageSent(numMessages);
              }
            }
          });
    }

    @Override
    public void writeFrame(WritableBuffer frame, boolean flush, final int numMessages) {
      PerfMark.startTask("NettyServerStream$Sink.writeFrame");
      try {
        writeFrameInternal(frame, flush, numMessages);
      } finally {
        PerfMark.stopTask("NettyServerStream$Sink.writeFrame");
      }
    }

    @Override
    public void writeTrailers(Metadata trailers, boolean headersSent, Status status) {
      PerfMark.startTask("NettyServerStream$Sink.writeTrailers");
      try {
        GofProto.Header gofTrailers = Utils.convertTrailers(trailers, headersSent);
        writeQueue.enqueue(
            SendResponseHeadersCommand.createTrailers(transportState(), gofTrailers, status),
            true);
      } finally {
        PerfMark.stopTask("NettyServerStream$Sink.writeTrailers");
      }
    }

    @Override
    public void cancel(Status status) {
      PerfMark.startTask("NettyServerStream$Sink.cancel");
      try {
        writeQueue.enqueue(new CancelServerStreamCommand(transportState(), status), true);
      } finally {
        PerfMark.startTask("NettyServerStream$Sink.cancel");
      }
    }
  }

  /** This should only be called from the transport thread. */
  public static class TransportState extends AbstractServerStream.TransportState
      implements StreamIdHolder {
    private final GofStream gofStream;
    private final NettyServerHandler handler;
    private final EventLoop eventLoop;
    private final Tag tag;

    public TransportState(
        NettyServerHandler handler,
        EventLoop eventLoop,
        GofStream gofStream,
        int maxMessageSize,
        StatsTraceContext statsTraceCtx,
        TransportTracer transportTracer,
        String methodName) {
      super(maxMessageSize, statsTraceCtx, transportTracer);
      this.gofStream = gofStream;
      this.handler = checkNotNull(handler, "handler");
      this.eventLoop = eventLoop;
      this.tag = PerfMark.createTag(methodName, gofStream.id());
    }

    @Override
    public void runOnTransportThread(final Runnable r) {
      if (eventLoop.inEventLoop()) {
        r.run();
      } else {
        final Link link = PerfMark.linkOut();
        eventLoop.execute(new Runnable() {
          @Override
          public void run() {
            PerfMark.startTask("NettyServerStream$TransportState.runOnTransportThread", tag);
            PerfMark.linkIn(link);
            try {
              r.run();
            } finally {
              PerfMark.stopTask("NettyServerStream$TransportState.runOnTransportThread", tag);
            }
          }
        });
      }
    }

    @Override
    public void bytesRead(int processedBytes) {
      // handler.returnProcessedBytes(http2Stream, processedBytes);
      handler.getWriteQueue().scheduleFlush();
    }

    @Override
    public void deframeFailed(Throwable cause) {
      log.log(Level.WARNING, "Exception processing message", cause);
      Status status = Status.fromThrowable(cause);
      transportReportStatus(status);
      handler.getWriteQueue().enqueue(new CancelServerStreamCommand(this, status), true);
    }

    void inboundDataReceived(ByteBuf frame, boolean endOfStream) {
      super.inboundDataReceived(new NettyReadableBuffer(frame.retain()), endOfStream);
    }

    @Override
    public int id() {
      return gofStream.id();
    }

    @Override
    public Tag tag() {
      return tag;
    }
  }

  @Override
  public int streamId() {
    return streamId;
  }
}
