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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import io.grpc.InsecureServerCredentials;
import io.grpc.ServerCredentials;
import io.grpc.ServerProvider;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link NettyServerProvider}. */
@RunWith(JUnit4.class)
public class NettyServerProviderTest {
  private NettyServerProvider provider = new NettyServerProvider();

  @Test
  public void provided() {
    assertSame(NettyServerProvider.class, ServerProvider.provider().getClass());
  }

  @Test
  public void basicMethods() {
    assertTrue(provider.isAvailable());
    assertEquals(5, provider.priority());
  }

  @Test
  public void builderIsANettyBuilder() {
    assertSame(NettyServerBuilder.class, provider.builderForPort(443).getClass());
  }

  @Test
  public void newServerBuilderForPort_success() {
    ServerProvider.NewServerBuilderResult result =
        provider.newServerBuilderForPort(80, InsecureServerCredentials.create());
    assertThat(result.getServerBuilder()).isInstanceOf(NettyServerBuilder.class);
  }

  @Test
  public void newServerBuilderForPort_fail() {
    ServerProvider.NewServerBuilderResult result = provider.newServerBuilderForPort(
        80, new FakeServerCredentials());
    assertThat(result.getError()).contains("FakeServerCredentials");
  }

  private static final class FakeServerCredentials extends ServerCredentials {}
}
