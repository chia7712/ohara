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

import java.util.concurrent.TimeUnit

import oharastream.ohara.client.configurator.NodeApi.Node
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.testing.service.SshdServer
import org.junit.jupiter.api.{AfterEach, Test}
import org.scalatest.matchers.should.Matchers._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class TestRemoteFolderHandler extends OharaTest {
  private[this] val server   = SshdServer.local(0)
  private[this] val hostname = server.hostname()
  private[this] val dataCollie = DataCollie(
    Seq(
      Node(
        hostname = hostname,
        port = server.port(),
        user = server.user(),
        password = server.password()
      )
    )
  )
  private[this] val folderHandler = RemoteFolderHandler(dataCollie)

  @Test
  def testFolderNotExists(): Unit =
    result(folderHandler.exist(server.hostname(), "/home/ohara100")) shouldBe false

  @Test
  def testCreateFolderAndDelete(): Unit = {
    val path = s"/tmp/${CommonUtils.randomString(5)}"
    result(folderHandler.create(hostname, path)) shouldBe true
    result(folderHandler.exist(hostname, path)) shouldBe true
    // file exists so it does nothing
    result(folderHandler.create(hostname, path)) shouldBe false
    result(folderHandler.delete(hostname, path)) shouldBe true
    result(folderHandler.delete(hostname, path)) shouldBe false
  }

  @Test
  def testListFolder(): Unit = {
    result(folderHandler.list(hostname, "/tmp")).size should not be 0
    val path = s"/tmp/${CommonUtils.randomString(5)}"
    result(folderHandler.create(hostname, path)) shouldBe true
    result(folderHandler.list(hostname, "/tmp")) should contain(path)
  }

  @Test
  def testInspectFolder(): Unit = {
    val folderInfo = result(folderHandler.inspect(hostname, "/tmp"))
    folderInfo.name shouldBe "tmp"
    folderInfo.permission shouldBe FolderPermission.READWRITE
    folderInfo.uid should be >= 0
  }

  private[this] def result[T](f: Future[T]): T = Await.result(f, Duration(120, TimeUnit.SECONDS))

  @AfterEach
  def tearDown(): Unit = Releasable.close(server)
}
