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

import com.google.common.base.MoreObjects;
import io.grpc.InternalChannelz.SocketOptions;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.internal.GrpcUtil;
import io.netty.channel.*;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import kr.jclab.grpcover.gofprotocol.GofException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.nio.channels.UnresolvedAddressException;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.*;

/** Unit tests for {@link Utils}. */
@RunWith(JUnit4.class)
public class UtilsTest {
  private final Metadata.Key<String> userKey =
      Metadata.Key.of("user-key", Metadata.ASCII_STRING_MARSHALLER);
  private final String userValue =  "user-value";

  @Test
  public void testStatusFromThrowable() {
    Status s = Status.CANCELLED.withDescription("msg");
    assertSame(s, Utils.statusFromThrowable(new Exception(s.asException())));
    Throwable t;
    t = new ConnectTimeoutException("msg");
    assertStatusEquals(Status.UNAVAILABLE.withCause(t), Utils.statusFromThrowable(t));
    t = new UnresolvedAddressException();
    assertStatusEquals(Status.UNAVAILABLE.withCause(t), Utils.statusFromThrowable(t));
    t = new GofException(GrpcUtil.Http2Error.INTERNAL_ERROR, "msg");
    assertStatusEquals(Status.INTERNAL.withCause(t), Utils.statusFromThrowable(t));
    t = new Exception("msg");
    assertStatusEquals(Status.UNKNOWN.withCause(t), Utils.statusFromThrowable(t));
  }

//  @Test
//  public void convertClientHeaders_sanitizes() {
//    Metadata metaData = new Metadata();
//
//    // Intentionally being explicit here rather than relying on any pre-defined lists of headers,
//    // since the goal of this test is to validate the correctness of such lists in the first place.
//    metaData.put(GrpcUtil.CONTENT_TYPE_KEY, "to-be-removed");
//    metaData.put(GrpcUtil.USER_AGENT_KEY, "to-be-removed");
//    metaData.put(GrpcUtil.TE_HEADER, "to-be-removed");
//    metaData.put(userKey, userValue);
//
//    String scheme = "https";
//    String userAgent = "user-agent";
//    String method = "POST";
//    String authority = "authority";
//    String path = "//testService/test";
//
//    Http2Headers output =
//        Utils.convertClientHeaders(
//            metaData,
//            new AsciiString(scheme),
//            new AsciiString(path),
//            new AsciiString(authority),
//            new AsciiString(method),
//            new AsciiString(userAgent));
//    DefaultHttp2Headers headers = new DefaultHttp2Headers();
//    for (Map.Entry<CharSequence, CharSequence> entry : output) {
//      headers.add(entry.getKey(), entry.getValue());
//    }
//
//    // 7 reserved headers, 1 user header
//    assertEquals(7 + 1, headers.size());
//    // Check the 3 reserved headers that are non pseudo
//    // Users can not create pseudo headers keys so no need to check for them here
//    assertEquals(GrpcUtil.CONTENT_TYPE_GRPC,
//        headers.get(GrpcUtil.CONTENT_TYPE_KEY.name()).toString());
//    assertEquals(userAgent, headers.get(GrpcUtil.USER_AGENT_KEY.name()).toString());
//    assertEquals(GrpcUtil.TE_TRAILERS, headers.get(GrpcUtil.TE_HEADER.name()).toString());
//    // Check the user header is in tact
//    assertEquals(userValue, headers.get(userKey.name()).toString());
//  }

//  @Test
//  @SuppressWarnings("UndefinedEquals") // AsciiString.equals
//  public void convertServerHeaders_sanitizes() {
//    Metadata metaData = new Metadata();
//
//    // Intentionally being explicit here rather than relying on any pre-defined lists of headers,
//    // since the goal of this test is to validate the correctness of such lists in the first place.
//    metaData.put(GrpcUtil.CONTENT_TYPE_KEY, "to-be-removed");
//    metaData.put(GrpcUtil.TE_HEADER, "to-be-removed");
//    metaData.put(GrpcUtil.USER_AGENT_KEY, "to-be-removed");
//    metaData.put(userKey, userValue);
//
//    Http2Headers output = Utils.convertServerHeaders(metaData);
//    DefaultHttp2Headers headers = new DefaultHttp2Headers();
//    for (Map.Entry<CharSequence, CharSequence> entry : output) {
//      headers.add(entry.getKey(), entry.getValue());
//    }
//    // 2 reserved headers, 1 user header
//    assertEquals(2 + 1, headers.size());
//    assertEquals(Utils.CONTENT_TYPE_GRPC, headers.get(GrpcUtil.CONTENT_TYPE_KEY.name()));
//  }

  @Test
  public void channelOptionsTest_noLinger() {
    Channel channel = new EmbeddedChannel();
    assertNull(channel.config().getOption(ChannelOption.SO_LINGER));
    SocketOptions socketOptions = Utils.getSocketOptions(channel);
    assertNull(socketOptions.lingerSeconds);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void channelOptionsTest_oio() {
    Channel channel = new io.netty.channel.socket.oio.OioSocketChannel();
    SocketOptions socketOptions = setAndValidateGeneric(channel);
    assertEquals(250, (int) socketOptions.soTimeoutMillis);
  }

  @Test
  public void channelOptionsTest_nio() {
    Channel channel = new NioSocketChannel();
    SocketOptions socketOptions = setAndValidateGeneric(channel);
    assertNull(socketOptions.soTimeoutMillis);
  }

  private static SocketOptions setAndValidateGeneric(Channel channel) {
    channel.config().setOption(ChannelOption.SO_LINGER, 3);
    // only applicable for OIO channels:
    channel.config().setOption(ChannelOption.SO_TIMEOUT, 250);
    // Test some arbitrarily chosen options with a non numeric values
    channel.config().setOption(ChannelOption.SO_KEEPALIVE, true);
    WriteBufferWaterMark writeBufWaterMark = new WriteBufferWaterMark(10, 20);
    channel.config().setOption(ChannelOption.WRITE_BUFFER_WATER_MARK, writeBufWaterMark);

    SocketOptions socketOptions = Utils.getSocketOptions(channel);
    assertEquals(3, (int) socketOptions.lingerSeconds);
    assertEquals("true", socketOptions.others.get("SO_KEEPALIVE"));
    assertEquals(
        writeBufWaterMark.toString(),
        socketOptions.others.get(ChannelOption.WRITE_BUFFER_WATER_MARK.toString()));
    return socketOptions;
  }

  private static void assertStatusEquals(Status expected, Status actual) {
    assertEquals(expected.getCode(), actual.getCode());
    assertThat(MoreObjects.firstNonNull(actual.getDescription(), ""))
        .contains(MoreObjects.firstNonNull(expected.getDescription(), ""));
    assertEquals(expected.getCause(), actual.getCause());
  }
}
