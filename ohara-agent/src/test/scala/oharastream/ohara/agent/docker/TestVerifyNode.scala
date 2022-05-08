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

package oharastream.ohara.agent.docker

import java.util
import java.util.concurrent.TimeUnit

import oharastream.ohara.agent.{DataCollie, ServiceCollie}
import oharastream.ohara.client.configurator.NodeApi.{Node, State}
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.testing.service.SshdServer
import org.junit.jupiter.api.{AfterEach, Test}
import org.scalatest.matchers.should.Matchers._

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

/**
  * the default implementation of verifying node consists of 1 actions.
  * 1) list resources
  * this test injects command handler for above actions that return correct response or throw exception.
  */
class TestVerifyNode extends OharaTest {
  private[this] var errorMessage: String = _
  private[this] val sshServer = SshdServer.local(
    0,
    java.util.Map.of(
      "docker info --format '{{json .}}'",
      (_: String) =>
        if (errorMessage != null)
          throw new IllegalArgumentException(errorMessage)
        else util.List.of("""
                        |  {
                        |    "NCPU": 1,
                        |    "MemTotal": 1024
                        |  }
                        |""".stripMargin)
    )
  )

  private[this] val node = Node(
    hostname = sshServer.hostname(),
    port = sshServer.port(),
    user = sshServer.user(),
    password = sshServer.password(),
    services = Seq.empty,
    state = State.AVAILABLE,
    error = None,
    lastModified = CommonUtils.current(),
    resources = Seq.empty,
    tags = Map.empty
  )

  private[this] val collie = ServiceCollie.dockerModeBuilder.dataCollie(DataCollie(Seq(node))).build

  @Test
  def happyCase(): Unit = Await.result(collie.verifyNode(node), Duration(30, TimeUnit.SECONDS))

  @Test
  def badCase(): Unit = {
    errorMessage = CommonUtils.randomString()
    intercept[Exception] {
      Await.result(collie.verifyNode(node), Duration(30, TimeUnit.SECONDS))
    }.getMessage should include("unavailable")
  }

  @AfterEach
  def tearDown(): Unit = {
    Releasable.close(collie)
    Releasable.close(sshServer)
  }
}
