/*
 * Copyright 2019 is-land
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

package oharastream.ohara.agent

import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.util.Releasable
import oharastream.ohara.testing.service.SshdServer
import org.junit.jupiter.api.{AfterEach, Test}
import org.scalatest.matchers.should.Matchers._

import scala.jdk.CollectionConverters._

class TestAgent extends OharaTest {
  private[this] val server = SshdServer.local(
    0,
    java.util.Map.of(
      "hello",
      (_: String) => java.util.List.of("world"),
      "oharastream",
      (_: String) => java.util.List.of("ohara")
    )
  )

  @Test
  def testJaveVersion(): Unit = {
    val agent =
      Agent.builder.hostname(server.hostname).port(server.port).user(server.user).password(server.password).build
    try {
      val result = agent.execute("java -version").get
      result.toLowerCase should include("jdk")
    } finally agent.close()
  }

  @Test
  def testCustomCommand(): Unit = {
    def assertResponse(request: String, response: java.util.List[String]): Unit = {
      val agent =
        Agent.builder.hostname(server.hostname).port(server.port).user(server.user).password(server.password).build
      try agent.execute(request).get.split("\n").toSeq shouldBe response.asScala.toSeq
      finally agent.close()
    }
    assertResponse("hello", java.util.List.of("world"))
    assertResponse("oharastream", java.util.List.of("ohara"))
  }

  @Test
  def nullHostname(): Unit = an[NullPointerException] should be thrownBy Agent.builder.hostname(null)

  @Test
  def emptyHostname(): Unit = an[IllegalArgumentException] should be thrownBy Agent.builder.hostname("")

  @Test
  def negativePort(): Unit = {
    an[IllegalArgumentException] should be thrownBy Agent.builder.port(0)
    an[IllegalArgumentException] should be thrownBy Agent.builder.port(-1)
  }

  @Test
  def nullUser(): Unit = an[NullPointerException] should be thrownBy Agent.builder.user(null)

  @Test
  def emptyUser(): Unit = an[IllegalArgumentException] should be thrownBy Agent.builder.user("")

  @Test
  def nullPassword(): Unit = an[NullPointerException] should be thrownBy Agent.builder.password(null)

  @Test
  def emptyPassword(): Unit = an[IllegalArgumentException] should be thrownBy Agent.builder.password("")

  @Test
  def nullTimeout(): Unit = an[NullPointerException] should be thrownBy Agent.builder.timeout(null)

  @Test
  def nullCharset(): Unit = an[NullPointerException] should be thrownBy Agent.builder.charset(null)

  @AfterEach
  def tearDown(): Unit = Releasable.close(server)
}
