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
import com.google.protobuf.ByteString;
import com.google.rpc.Code;
import io.grpc.InternalChannelz;
import io.grpc.InternalMetadata;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.internal.GrpcUtil;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.handler.codec.DecoderException;
import io.netty.util.AsciiString;
import kr.jclab.grpcover.core.protocol.v1.GofProto;
import kr.jclab.grpcover.gofprotocol.GofException;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.UnresolvedAddressException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static io.grpc.internal.GrpcUtil.CONTENT_TYPE_KEY;
import static io.netty.channel.ChannelOption.SO_LINGER;
import static io.netty.channel.ChannelOption.SO_TIMEOUT;

/**
 * Common utility methods.
 */
class Utils {
  private static final Logger logger = Logger.getLogger(Utils.class.getName());

  /**
   * Metadata marshaller for HTTP status lines.
   */
  private static final InternalMetadata.TrustedAsciiMarshaller<Integer> HTTP_STATUS_MARSHALLER =
          new InternalMetadata.TrustedAsciiMarshaller<Integer>() {
            @Override
            public byte[] toAsciiString(Integer value) {
              return String.valueOf(value).getBytes(StandardCharsets.US_ASCII);
            }

            /**
             * RFC 7231 says status codes are 3 digits long.
             *
             * @see <a href="https://tools.ietf.org/html/rfc7231#section-6">RFC 7231</a>
             */
            @Override
            public Integer parseAsciiString(byte[] serialized) {
              return Integer.parseInt(new String(serialized));
            }
          };
  public static final Metadata.Key<Integer> GOF_STATUS = InternalMetadata.keyOf(":status",
          HTTP_STATUS_MARSHALLER);

  public static final AsciiString STATUS_OK = AsciiString.of("200");
  public static final AsciiString HTTP_METHOD = AsciiString.of(GrpcUtil.HTTP_METHOD);
  public static final AsciiString HTTP_GET_METHOD = AsciiString.of("GET");
  public static final AsciiString HTTPS = AsciiString.of("https");
  public static final AsciiString HTTP = AsciiString.of("http");
  public static final AsciiString CONTENT_TYPE_HEADER = AsciiString.of(CONTENT_TYPE_KEY.name());
  public static final AsciiString CONTENT_TYPE_GRPC = AsciiString.of(GrpcUtil.CONTENT_TYPE_GRPC);
  public static final AsciiString TE_HEADER = AsciiString.of(GrpcUtil.TE_HEADER.name());
  public static final AsciiString TE_TRAILERS = AsciiString.of(GrpcUtil.TE_TRAILERS);
  public static final AsciiString USER_AGENT = AsciiString.of(GrpcUtil.USER_AGENT_KEY.name());

  // This class is initialized on first use, thus provides delayed allocator creation.
  private static final class ByteBufAllocatorPreferDirectHolder {
    private static final ByteBufAllocator allocator = createByteBufAllocator(true);
  }

  // This class is initialized on first use, thus provides delayed allocator creation.
  private static final class ByteBufAllocatorPreferHeapHolder {
    private static final ByteBufAllocator allocator = createByteBufAllocator(false);
  }

  public static ByteBufAllocator getByteBufAllocator(boolean forceHeapBuffer) {
    if (Boolean.parseBoolean(
            System.getProperty("io.grpc.netty.useCustomAllocator", "true"))) {
      boolean defaultPreferDirect = PooledByteBufAllocator.defaultPreferDirect();
      logger.log(
          Level.FINE,
          String.format(
              "Using custom allocator: forceHeapBuffer=%s, defaultPreferDirect=%s",
              forceHeapBuffer,
              defaultPreferDirect));
      if (forceHeapBuffer || !defaultPreferDirect) {
        return ByteBufAllocatorPreferHeapHolder.allocator;
      } else {
        return ByteBufAllocatorPreferDirectHolder.allocator;
      }
    } else {
      logger.log(Level.FINE, "Using default allocator");
      return ByteBufAllocator.DEFAULT;
    }
  }

  private static ByteBufAllocator createByteBufAllocator(boolean preferDirect) {
    int maxOrder;
    logger.log(Level.FINE, "Creating allocator, preferDirect=" + preferDirect);
    if (System.getProperty("io.netty.allocator.maxOrder") == null) {
      // See the implementation of PooledByteBufAllocator.  DEFAULT_MAX_ORDER in there is
      // 11, which makes chunk size to be 8192 << 11 = 16 MiB.  We want the chunk size to be
      // 2MiB, thus reducing the maxOrder to 8.
      maxOrder = 8;
      logger.log(Level.FINE, "Forcing maxOrder=" + maxOrder);
    } else {
      maxOrder = PooledByteBufAllocator.defaultMaxOrder();
      logger.log(Level.FINE, "Using default maxOrder=" + maxOrder);
    }
    return new PooledByteBufAllocator(
        preferDirect,
        PooledByteBufAllocator.defaultNumHeapArena(),
        // Assuming neither gRPC nor netty are using allocator.directBuffer() to request
        // specifically for direct buffers, which is true as I just checked, setting arenas to 0
        // will make sure no direct buffer is ever created.
        preferDirect ? PooledByteBufAllocator.defaultNumDirectArena() : 0,
        PooledByteBufAllocator.defaultPageSize(),
        maxOrder,
        PooledByteBufAllocator.defaultSmallCacheSize(),
        PooledByteBufAllocator.defaultNormalCacheSize(),
        PooledByteBufAllocator.defaultUseCacheForAllThreads());
  }

  public static Metadata convertHeaders(GofProto.Header headers) {
    Metadata metadata = metadataDeserialize(headers.getMetadataList());
    metadata.put(GOF_STATUS, headers.getStatus().getCode());
    return metadata;
  }

  public static Status statusFromMetadata(Metadata metadata) {
    Integer statusCode = metadata.get(GOF_STATUS);
    if (statusCode != null) {
      return Status.fromCodeValue(statusCode);
    } else {
      return Status.INTERNAL.withDescription("missing HTTP status code");
    }
  }

  public static GofProto.Header convertClientHeaders(Metadata headers,
      String method,
      String authority
  ) {
    Preconditions.checkNotNull(authority, "authority");
    Preconditions.checkNotNull(method, "method");

    // Discard any application supplied duplicates of the reserved headers
    headers.discardAll(CONTENT_TYPE_KEY);
    headers.discardAll(GrpcUtil.TE_HEADER);
    headers.discardAll(GrpcUtil.USER_AGENT_KEY);

    return GofProto.Header.newBuilder()
            .addAllMetadata(metadataSerialize(headers))
            .setMethod(method)
            .setAuthority(authority)
            .build();
  }

  public static GofProto.Header convertServerHeaders(Metadata headers) {
    // Discard any application supplied duplicates of the reserved headers
    headers.discardAll(CONTENT_TYPE_KEY);
    headers.discardAll(GrpcUtil.TE_HEADER);
    headers.discardAll(GrpcUtil.USER_AGENT_KEY);
    return serverResponseHeaders(headers);
  }

  public static GofProto.Header serverResponseHeaders(Metadata headers) {
    return GofProto.Header.newBuilder()
            .setStatus(com.google.rpc.Status.newBuilder().setCode(Code.OK_VALUE).build())
            .addAllMetadata(metadataSerialize(headers))
            .build();
  }

  public static GofProto.Header serverResponseTrailers(Metadata headers) {
    return GofProto.Header.newBuilder()
            .setStatus(com.google.rpc.Status.newBuilder().setCode(Code.OK_VALUE).build())
            .addAllMetadata(metadataSerialize(headers))
            .build();
  }

  public static Metadata convertTrailers(GofProto.Header headers) {
    Metadata metadata = metadataDeserialize(headers.getMetadataList());
    metadata.put(GOF_STATUS, headers.getStatus().getCode());
    return metadata;
  }

  public static GofProto.Header convertTrailers(Metadata trailers, boolean headersSent) {
    if (!headersSent) {
      return convertServerHeaders(trailers);
    }
    return serverResponseTrailers(trailers);
  }

  public static Status statusFromThrowable(Throwable t) {
    Status s = Status.fromThrowable(t);
    if (s.getCode() != Status.Code.UNKNOWN) {
      return s;
    }
    if (t instanceof ClosedChannelException) {
      // ClosedChannelException is used any time the Netty channel is closed. Proper error
      // processing requires remembering the error that occurred before this one and using it
      // instead.
      //
      // Netty uses an exception that has no stack trace, while we would never hope to show this to
      // users, if it happens having the extra information may provide a small hint of where to
      // look.
      ClosedChannelException extraT = new ClosedChannelException();
      extraT.initCause(t);
      return Status.UNKNOWN.withDescription("channel closed").withCause(extraT);
    }
    if (t instanceof DecoderException && t.getCause() instanceof SSLException) {
      return Status.UNAVAILABLE.withDescription("ssl exception").withCause(t);
    }
    if (t instanceof IOException) {
      return Status.UNAVAILABLE.withDescription("io exception").withCause(t);
    }
    if (t instanceof UnresolvedAddressException) {
      return Status.UNAVAILABLE.withDescription("unresolved address").withCause(t);
    }
    if (t instanceof GofException) {
      return Status.INTERNAL.withDescription("gof exception").withCause(t);
    }
    return s;
  }

  static InternalChannelz.SocketOptions getSocketOptions(Channel channel) {
    ChannelConfig config = channel.config();
    InternalChannelz.SocketOptions.Builder b = new InternalChannelz.SocketOptions.Builder();

    // The API allows returning null but not sure if it can happen in practice.
    // Let's be paranoid and do null checking just in case.
    Integer lingerSeconds = config.getOption(SO_LINGER);
    if (lingerSeconds != null) {
      b.setSocketOptionLingerSeconds(lingerSeconds);
    }

    Integer timeoutMillis = config.getOption(SO_TIMEOUT);
    if (timeoutMillis != null) {
      // in java, SO_TIMEOUT only applies to receiving
      b.setSocketOptionTimeoutMillis(timeoutMillis);
    }

    for (Map.Entry<ChannelOption<?>, Object> opt : config.getOptions().entrySet()) {
      ChannelOption<?> key = opt.getKey();
      // Constants are pooled, so there should only be one instance of each constant
      if (key.equals(SO_LINGER) || key.equals(SO_TIMEOUT)) {
        continue;
      }
      Object value = opt.getValue();
      // zpencer: Can a netty option be null?
      b.addOption(key.name(), String.valueOf(value));
    }

    return b.build();
  }

  static List<ByteString> metadataSerialize(Metadata metadata) {
    if (metadata == null) return Collections.emptyList();
    return Arrays.stream(InternalMetadata.serialize(metadata))
            .map(ByteString::copyFrom)
            .collect(Collectors.toList());
  }

  static Metadata metadataDeserialize(List<ByteString> payload) {
    byte[][] serializedMetadata = payload.stream()
            .map(ByteString::toByteArray)
            .toArray(byte[][]::new);
    return InternalMetadata.newMetadata(serializedMetadata);
  }


  public static Status statusFromProto(com.google.rpc.Status input) {
    Status status = Status.fromCodeValue(input.getCode());
    String message = input.getMessage();
    if (!message.isEmpty()) {
      status = status.withDescription(message);
    }
    return status;
  }

  public static com.google.rpc.Status statusToProto(Status input) {
    return com.google.rpc.Status.newBuilder()
            .setCode(
                    input.getCode().value()
            )
            .setMessage(
                    Optional.ofNullable(input.getDescription())
                            .orElse("")
            )
            .build();
  }

  private Utils() {
    // Prevents instantiation
  }
}
