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

package oharastream.ohara.configurator.route

import oharastream.ohara.client.configurator.NodeApi
import oharastream.ohara.client.configurator.NodeApi.{Node, Request}
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.setting.ObjectKey
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.configurator.Configurator
import org.junit.jupiter.api.{AfterEach, Test}
import org.scalatest.matchers.should.Matchers._
import spray.json.{DeserializationException, JsNumber, JsString}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
class TestNodeRoute extends OharaTest {
  private[this] val numberOfCluster = 1
  private[this] val configurator    = Configurator.builder.fake(numberOfCluster, numberOfCluster).build()

  /**
    * a fake cluster has 3 fake node.
    */
  private[this] val numberOfDefaultNodes = 3 * numberOfCluster
  private[this] val nodeApi              = NodeApi.access.hostname(configurator.hostname).port(configurator.port)

  private[this] def compare(lhs: Node, rhs: Node): Unit = {
    lhs.name shouldBe rhs.name
    lhs.port shouldBe rhs.port
    lhs.user shouldBe rhs.user
    lhs.password shouldBe rhs.password
  }

  private[this] def result[T](f: Future[T]): T = Await.result(f, Duration("20 seconds"))

  @Test
  def testServices(): Unit = {
    val nodes = result(nodeApi.list())
    nodes.isEmpty shouldBe false
    nodes.foreach(_.services.size should not be 0)
    nodes.foreach(_.services.flatMap(_.clusterKeys).size should not be 0)
  }

  @Test
  def testAdd(): Unit = {
    val hostname = CommonUtils.randomString(10)
    val port     = CommonUtils.availablePort()
    val user     = CommonUtils.randomString()
    val password = CommonUtils.randomString()
    val res      = result(nodeApi.request.nodeName(hostname).port(port).user(user).password(password).create())
    res.name shouldBe hostname
    res.hostname shouldBe hostname
    res.port shouldBe port
    res.user shouldBe user
    res.password shouldBe password

    result(nodeApi.list()).size shouldBe (1 + numberOfDefaultNodes)
    compare(result(nodeApi.list()).find(_.name == hostname).get, res)

    an[IllegalArgumentException] should be thrownBy result(
      nodeApi.request.nodeName(hostname).port(port).user(user).password(password).create()
    )
  }

  @Test
  def testDelete(): Unit = {
    val res = result(
      nodeApi.request
        .nodeName(CommonUtils.randomString(10))
        .port(CommonUtils.availablePort())
        .user(CommonUtils.randomString())
        .password(CommonUtils.randomString())
        .create()
    )

    result(nodeApi.list()).size shouldBe (1 + numberOfDefaultNodes)

    result(nodeApi.delete(res.key))
    result(nodeApi.list()).size shouldBe numberOfDefaultNodes
  }

  @Test
  def disableToDeleteNodeRunningService(): Unit = {
    val nodes       = result(nodeApi.list())
    val runningNode = nodes.filter(_.services.exists(_.clusterKeys.nonEmpty)).head
    an[IllegalArgumentException] should be thrownBy result(nodeApi.delete(runningNode.key))
  }

  @Test
  def testUpdate(): Unit = {
    val res = result(
      nodeApi.request
        .nodeName(CommonUtils.randomString(10))
        .port(CommonUtils.availablePort())
        .user(CommonUtils.randomString())
        .password(CommonUtils.randomString())
        .create()
    )

    result(nodeApi.list()).size shouldBe (1 + numberOfDefaultNodes)

    result(
      nodeApi.request
        .nodeName(res.hostname)
        .port(CommonUtils.availablePort())
        .user(CommonUtils.randomString())
        .password(CommonUtils.randomString())
        .update()
    )

    result(nodeApi.list()).size shouldBe (1 + numberOfDefaultNodes)
  }

  @Test
  def duplicateDelete(): Unit =
    (0 to 10).foreach(
      _ => result(nodeApi.delete(ObjectKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))))
    )

  @Test
  def duplicateUpdate(): Unit = {
    val init  = result(nodeApi.list()).size
    val count = 10
    (0 until count).foreach { _ =>
      result(
        nodeApi.request
          .nodeName(CommonUtils.randomString(10))
          .port(CommonUtils.availablePort())
          .user(CommonUtils.randomString())
          .password(CommonUtils.randomString())
          .create()
      )
    }
    result(nodeApi.list()).size shouldBe count + init
  }

  @Test
  def testInvalidNameOnCreation(): Unit = {
    val invalidStrings = Seq("a@", "a=", "a\\", "a~", "a//")
    invalidStrings.foreach { invalidString =>
      an[DeserializationException] should be thrownBy result(
        nodeApi.request
          .nodeName(invalidString)
          .port(CommonUtils.availablePort())
          .user(CommonUtils.randomString())
          .password(CommonUtils.randomString())
          .create()
      )
    }
  }

  @Test
  def testUpdatePort(): Unit = {
    val port = CommonUtils.availablePort()
    updatePartOfField(_.port(port), _.copy(port = port))
  }

  @Test
  def testUpdateUser(): Unit = {
    val user = CommonUtils.randomString()
    updatePartOfField(_.user(user), _.copy(user = user))
  }

  @Test
  def testUpdatePassword(): Unit = {
    val password = CommonUtils.randomString()
    updatePartOfField(_.password(password), _.copy(password = password))
  }

  private[this] def updatePartOfField(req: Request => Request, _expected: Node => Node): Unit = {
    val previous = result(
      nodeApi.request
        .nodeName(CommonUtils.randomString(10))
        .port(CommonUtils.availablePort())
        .user(CommonUtils.randomString())
        .password(CommonUtils.randomString())
        .update()
    )
    val updated  = result(req(nodeApi.request.nodeName(previous.hostname)).update())
    val expected = _expected(previous)
    updated.name shouldBe expected.name
    updated.port shouldBe expected.port
    updated.user shouldBe expected.user
    updated.password shouldBe expected.password
  }

  @Test
  def createNodeWithoutPort(): Unit =
    result(
      nodeApi.request
        .nodeName(CommonUtils.randomString(10))
        .user(CommonUtils.randomString())
        .password(CommonUtils.randomString())
        .update()
    )

  @Test
  def createNodeWithoutUser(): Unit =
    an[IllegalArgumentException] should be thrownBy result(
      nodeApi.request
        .nodeName(CommonUtils.randomString(10))
        .port(CommonUtils.availablePort())
        .password(CommonUtils.randomString())
        .update()
    )

  @Test
  def createNodeWithoutPassword(): Unit =
    an[IllegalArgumentException] should be thrownBy result(
      nodeApi.request
        .nodeName(CommonUtils.randomString(10))
        .port(CommonUtils.availablePort())
        .user(CommonUtils.randomString())
        .update()
    )

  @Test
  def updateTags(): Unit = {
    val tags = Map(
      CommonUtils.randomString(10) -> JsString(CommonUtils.randomString(10)),
      CommonUtils.randomString(10) -> JsNumber(CommonUtils.randomInteger())
    )
    val nodeDesc = result(
      nodeApi.request
        .nodeName(CommonUtils.randomString(10))
        .port(22)
        .user("user")
        .password("password")
        .tags(tags)
        .create()
    )
    nodeDesc.tags shouldBe tags

    val tags2 = Map(
      CommonUtils.randomString(10) -> JsString(CommonUtils.randomString(10)),
      CommonUtils.randomString(10) -> JsNumber(CommonUtils.randomInteger())
    )
    val nodeDesc2 = result(nodeApi.request.nodeName(nodeDesc.name).tags(tags2).update())
    nodeDesc2.tags shouldBe tags2

    val nodeDesc3 = result(nodeApi.request.nodeName(nodeDesc.name).update())
    nodeDesc3.tags shouldBe tags2

    val nodeDesc4 = result(nodeApi.request.nodeName(nodeDesc.name).tags(Map.empty).update())
    nodeDesc4.tags shouldBe Map.empty
  }

  @Test
  def resourcesShouldBeInResponse(): Unit = {
    val resources = result(
      nodeApi.request
        .nodeName(CommonUtils.randomString(10))
        .port(22)
        .user("user")
        .password("password")
        .create()
    ).resources
    resources should not be Set.empty
    resources.foreach(r => r.value should not be 0)
  }

  @Test
  def stateShouldBeAvailable(): Unit =
    result(
      nodeApi.request
        .nodeName(CommonUtils.randomString(10))
        .port(22)
        .user("user")
        .password("password")
        .create()
    ).state shouldBe NodeApi.State.AVAILABLE

  @AfterEach
  def tearDown(): Unit = Releasable.close(configurator)
}
