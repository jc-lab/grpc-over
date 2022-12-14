/*
 * Copyright 2017 The gRPC Authors
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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.net.SocketException;
import java.util.logging.Level;

import static com.google.common.truth.Truth.assertThat;
import static kr.jclab.grpcover.netty.NettyServerTransport.getLogLevel;
import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

@RunWith(JUnit4.class)
public class NettyServerTransportTest {
  @Test
  public void unknownException() {
    assertEquals(Level.INFO, getLogLevel(new Exception()));
  }

  @Test
  public void socketException() {
    assertEquals(Level.FINE, getLogLevel(new SocketException("Connection reset")));
  }

  @Test
  public void ioException() {
    assertEquals(Level.FINE, getLogLevel(new IOException("Connection reset by peer")));
    assertEquals(Level.FINE, getLogLevel(new IOException(
        "An existing connection was forcibly closed by the remote host")));
  }

  @Test
  public void ioException_nullMessage() {
    IOException e = new IOException();
    assertNull(e.getMessage());
    assertEquals(Level.FINE, getLogLevel(e));
  }

  @Test
  public void extendedIoException() {
    class ExtendedIoException extends IOException {}

    ExtendedIoException e = new ExtendedIoException();
    assertThat(e.getMessage()).isNull();
    assertThat(getLogLevel(e)).isEqualTo(Level.INFO);
  }

  @Test
  public void fakeNettyNativeIoException() {
    class NativeIoException extends IOException {}

    NativeIoException fakeNativeIoException = new NativeIoException();

    assertThat(getLogLevel(fakeNativeIoException)).isEqualTo(Level.FINE);
  }
}
