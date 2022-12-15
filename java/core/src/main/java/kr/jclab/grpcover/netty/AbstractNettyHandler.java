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

import io.grpc.ChannelLogger;
import io.netty.channel.*;
import kr.jclab.grpcover.core.protocol.v1.GofProto;
import kr.jclab.grpcover.gofprotocol.*;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;

import static com.google.common.base.Preconditions.checkState;

/**
 * Base class for all Netty gRPC handlers. This class standardizes exception handling (always
 * shutdown the connection) as well as sending the initial connection window at startup.
 */
@Slf4j
abstract class AbstractNettyHandler extends GofConnectionHandler implements GofConnectionHandlerCallback {
  private static final long GRACEFUL_SHUTDOWN_NO_TIMEOUT = -1;

  private final GofConnection connection;
  private final ChannelLogger negotiationLogger;

  @Override
  protected GofConnection connection() {
    return this.connection;
  }

  protected abstract DefaultGofDecoder decoder();

  public AbstractNettyHandler(
          GofConnection connection,
          int maxStreams,
          ChannelLogger negotiationLogger
  ) {
    this.connection = connection;
    this.connection.remote().maxActiveStreams(maxStreams);
    this.connection.local().maxActiveStreams(maxStreams);
    this.negotiationLogger = negotiationLogger;

    // During a graceful shutdown, wait until all streams are closed.
    gracefulShutdownTimeoutMillis(GRACEFUL_SHUTDOWN_NO_TIMEOUT);
  }

  /**
   * Returns the channel logger for the given channel context.
   */
  public ChannelLogger getNegotiationLogger() {
    checkState(negotiationLogger != null, "NegotiationLogger must not be null");
    return negotiationLogger;
  }

  @Override
  public void channelInactive(@NotNull ChannelHandlerContext ctx) throws Exception {
    // Call super class first, as this may result in decode being called.
    super.channelInactive(ctx);

    // We need to remove all streams (not just the active ones).
    // See https://github.com/netty/netty/issues/4838.
    connection().close(ctx.voidPromise());
  }

  @Override
  public void channelRead(@NotNull ChannelHandlerContext ctx, @NotNull Object msg) throws Exception {
    if (msg instanceof GofProto.Frame) {
      GofProto.Frame frame = (GofProto.Frame) msg;
      FrameParser.handle(ctx, frame, decoder().getFrameListener());
    } else {
      ctx.fireExceptionCaught(new Exception("Invalid message type: " + msg.getClass()));
    }
  }
}
