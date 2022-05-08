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

import oharastream.ohara.client.configurator.VolumeApi.VolumeState
import oharastream.ohara.client.configurator.{NodeApi, VolumeApi, ZookeeperApi}
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.setting.ObjectKey
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.configurator.Configurator
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.scalatest.matchers.should.Matchers._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class TestVolumeRoute extends OharaTest {
  private[this] val configurator = Configurator.builder.fake(0, 0).build()
  private[this] val zookeeperApi = ZookeeperApi.access.hostname(configurator.hostname).port(configurator.port)
  private[this] val volumeApi    = VolumeApi.access.hostname(configurator.hostname).port(configurator.port)

  private[this] val nodeNames: Set[String] = Set("n0", "n1")

  private[this] def result[T](f: Future[T]): T = Await.result(f, Duration("20 seconds"))

  @BeforeEach
  def setup(): Unit = {
    val nodeAccess = NodeApi.access.hostname(configurator.hostname).port(configurator.port)

    nodeNames.isEmpty shouldBe false
    nodeNames.foreach(n => result(nodeAccess.request.nodeName(n).port(22).user("user").password("pwd").create()))
  }

  @Test
  def testCreate(): Unit = {
    val key    = ObjectKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))
    val path   = CommonUtils.randomString(5)
    val volume = result(volumeApi.request.key(key).path(path).nodeNames(nodeNames).create())
    volume.key shouldBe key
    volume.path shouldBe path
    volume.nodeNames shouldBe nodeNames
  }

  @Test
  def testList(): Unit = {
    val initializeSize = result(volumeApi.list()).size
    val volume         = result(volumeApi.request.path(CommonUtils.randomString(5)).nodeNames(nodeNames).create())
    result(volumeApi.list()).size shouldBe initializeSize + 1
    result(volumeApi.list()).map(_.key) should contain(volume.key)
  }

  @Test
  def testDelete(): Unit = {
    val volume = result(volumeApi.request.path(CommonUtils.randomString(5)).nodeNames(nodeNames).create())
    result(volumeApi.list()).map(_.key) should contain(volume.key)

    result(volumeApi.start(volume.key))
    // you can't delete a running volume
    an[IllegalArgumentException] should be thrownBy result(volumeApi.delete(volume.key))

    result(volumeApi.stop(volume.key))
    result(volumeApi.delete(volume.key))

    result(volumeApi.list()).map(_.key) should not contain volume.key
  }

  @Test
  def testUsedByZookeeper(): Unit = {
    val volume    = result(volumeApi.request.path(CommonUtils.randomString(5)).nodeNames(nodeNames).create())
    val zookeeper = result(zookeeperApi.request.dataDir(volume.key).nodeNames(nodeNames).create())

    // you can't delete a used volume
    an[IllegalArgumentException] should be thrownBy result(volumeApi.delete(volume.key))

    // you can't start zk if the volume is not running
    an[IllegalArgumentException] should be thrownBy result(zookeeperApi.start(zookeeper.key))

    result(volumeApi.start(volume.key))
    result(volumeApi.get(volume.key)).state.get shouldBe VolumeState.RUNNING
    result(zookeeperApi.start(zookeeper.key))

    // you can't stop a volume used by another running service
    an[IllegalArgumentException] should be thrownBy result(volumeApi.stop(volume.key))

    result(zookeeperApi.stop(zookeeper.key))
    result(volumeApi.stop(volume.key))

    result(volumeApi.get(volume.key)).state shouldBe None
  }

  @Test
  def testAddNode(): Unit = {
    val volume = result(volumeApi.request.path(CommonUtils.randomString(5)).nodeNames(Set(nodeNames.head)).create())
    result(volumeApi.start(volume.key))
    result(volumeApi.get(volume.key)).state.get shouldBe VolumeState.RUNNING
    result(volumeApi.get(volume.key)).nodeNames shouldBe Set(nodeNames.head)
    result(volumeApi.addNode(volume.key, nodeNames.last))
    result(volumeApi.get(volume.key)).nodeNames shouldBe nodeNames
  }

  @AfterEach
  def tearDown(): Unit = Releasable.close(configurator)
}
