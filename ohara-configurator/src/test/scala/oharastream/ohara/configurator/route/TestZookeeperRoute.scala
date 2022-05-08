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

import oharastream.ohara.client.configurator.{BrokerApi, ClusterState, NodeApi, VolumeApi, ZookeeperApi}
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.setting.ObjectKey
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.configurator.Configurator
import oharastream.ohara.configurator.fake.FakeZookeeperCollie
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.scalatest.matchers.should.Matchers._
import spray.json.{DeserializationException, JsArray, JsNumber, JsObject, JsString, JsTrue}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
class TestZookeeperRoute extends OharaTest {
  private[this] val numberOfCluster = 1
  private[this] val configurator    = Configurator.builder.fake(numberOfCluster, 0).build()

  /**
    * a fake cluster has 3 fake node.
    */
  private[this] val numberOfDefaultNodes = 3 * numberOfCluster
  private[this] val zookeeperApi         = ZookeeperApi.access.hostname(configurator.hostname).port(configurator.port)
  private[this] val volumeApi            = VolumeApi.access.hostname(configurator.hostname).port(configurator.port)

  private[this] val nodeNames: Set[String] = Set("n0", "n1")

  private[this] def result[T](f: Future[T]): T = Await.result(f, Duration("20 seconds"))
  @BeforeEach
  def setup(): Unit = {
    val nodeAccess = NodeApi.access.hostname(configurator.hostname).port(configurator.port)

    nodeNames.isEmpty shouldBe false
    nodeNames.foreach { n =>
      result(nodeAccess.request.nodeName(n).port(22).user("user").password("pwd").create())
    }

    result(nodeAccess.list()).size shouldBe (nodeNames.size + numberOfDefaultNodes)
  }

  @Test
  def repeatedlyDelete(): Unit = {
    (0 to 10).foreach { index =>
      result(zookeeperApi.delete(ObjectKey.of(index.toString, index.toString)))
    }
  }

  @Test
  def removeZookeeperClusterUsedByBrokerCluster(): Unit = {
    val zks = result(zookeeperApi.list())
    val bks = result(BrokerApi.access.hostname(configurator.hostname).port(configurator.port).list())
    System.out.println(zks)
    System.out.println(bks)
    // we have a default zk cluster
    zks.isEmpty shouldBe false

    val zk = zks.head

    // this zookeeper cluster is used by broker cluster
    an[IllegalArgumentException] should be thrownBy result(zookeeperApi.stop(zk.key))

    // remove all broker clusters
    result(BrokerApi.access.hostname(configurator.hostname).port(configurator.port).list())
      .map(_.key)
      .foreach(
        key =>
          result(
            BrokerApi.access.hostname(configurator.hostname).port(configurator.port).stop(key)
              flatMap (_ => BrokerApi.access.hostname(configurator.hostname).port(configurator.port).delete(key))
          )
      )

    // pass
    result(zookeeperApi.stop(zk.key))
    result(zookeeperApi.delete(zk.key))
  }

  @Test
  def testCreateOnNonexistentNode(): Unit =
    an[IllegalArgumentException] should be thrownBy result(
      zookeeperApi.request.name(CommonUtils.randomString(10)).nodeName(CommonUtils.randomString(10)).create()
    )
  @Test
  def testList(): Unit = {
    val init  = result(zookeeperApi.list()).size
    val count = 3
    (0 until count).foreach { _ =>
      result(
        zookeeperApi.request.name(CommonUtils.randomString(10)).nodeNames(nodeNames).create()
      )
    }
    result(zookeeperApi.list()).size shouldBe count + init
  }

  @Test
  def testDelete(): Unit = {
    val init    = result(zookeeperApi.list()).size
    val cluster = result(zookeeperApi.request.name(CommonUtils.randomString(10)).nodeNames(nodeNames).create())
    result(zookeeperApi.list()).size shouldBe init + 1
    result(zookeeperApi.delete(cluster.key))
    result(zookeeperApi.list()).size shouldBe init
  }

  @Test
  def testStop(): Unit = {
    val init    = result(zookeeperApi.list()).size
    val cluster = result(zookeeperApi.request.name(CommonUtils.randomString(10)).nodeNames(nodeNames).create())
    result(zookeeperApi.start(cluster.key))
    result(zookeeperApi.list()).size shouldBe init + 1
    result(zookeeperApi.stop(cluster.key))
    result(zookeeperApi.delete(cluster.key))
    result(zookeeperApi.list()).size shouldBe init
  }

  @Test
  def testAddNode(): Unit = {
    val zk = result(
      zookeeperApi.request.name(CommonUtils.randomString(10)).nodeName(nodeNames.head).create()
    )
    result(zookeeperApi.start(zk.key))
    zk.nodeNames.size shouldBe 1
    zk.nodeNames.head shouldBe nodeNames.head
    // we don't support to add zk node at runtime
    an[IllegalArgumentException] should be thrownBy result(zookeeperApi.addNode(zk.key, nodeNames.last))
  }

  @Test
  def testRemoveNode(): Unit = {
    val cluster = result(
      zookeeperApi.request.name(CommonUtils.randomString(10)).nodeNames(nodeNames).create()
    )
    result(zookeeperApi.start(cluster.key))
    result(zookeeperApi.removeNode(cluster.key, nodeNames.head))
    result(zookeeperApi.get(cluster.key)).state shouldBe Some(ClusterState.RUNNING)
    result(zookeeperApi.get(cluster.key)).nodeNames.size shouldBe nodeNames.size - 1
    nodeNames should contain(result(zookeeperApi.get(cluster.key)).nodeNames.head)
    intercept[IllegalArgumentException] {
      result(zookeeperApi.get(cluster.key)).nodeNames.foreach { nodeName =>
        result(zookeeperApi.removeNode(cluster.key, nodeName))
      }
    }.getMessage should include("there is only one instance")
  }

  @Test
  def testInvalidClusterName(): Unit =
    an[DeserializationException] should be thrownBy result(
      zookeeperApi.request.name("abc def").nodeNames(nodeNames).create()
    )

  @Test
  def createZkClusterWithSameName(): Unit = {
    val name = CommonUtils.randomString(10)
    result(
      zookeeperApi.request.name(name).nodeNames(nodeNames).create()
    )

    an[IllegalArgumentException] should be thrownBy result(
      zookeeperApi.request.name(name).nodeNames(nodeNames).create()
    )
  }

  @Test
  def jmxPortConflict(): Unit = {
    val jmxPort = CommonUtils.availablePort()
    val zk = result(
      zookeeperApi.request.name(CommonUtils.randomString(10)).jmxPort(jmxPort).nodeNames(nodeNames).create()
    )
    result(zookeeperApi.start(zk.key))

    val zk2 = result(
      zookeeperApi.request.name(CommonUtils.randomString(10)).jmxPort(jmxPort).nodeNames(nodeNames).create()
    )

    intercept[IllegalArgumentException] {
      result(zookeeperApi.start(zk2.key))
    }.getMessage should include(jmxPort.toString)
  }

  @Test
  def clientPortConflict(): Unit = {
    val clientPort = CommonUtils.availablePort()
    val zk = result(
      zookeeperApi.request.name(CommonUtils.randomString(10)).clientPort(clientPort).nodeNames(nodeNames).create()
    )
    result(zookeeperApi.start(zk.key))

    val zk2 = result(
      zookeeperApi.request.name(CommonUtils.randomString(10)).clientPort(clientPort).nodeNames(nodeNames).create()
    )

    intercept[IllegalArgumentException] {
      result(zookeeperApi.start(zk2.key))
    }.getMessage should include(clientPort.toString)
  }

  @Test
  def peerPortConflict(): Unit = {
    val peerPort = CommonUtils.availablePort()
    val zk = result(
      zookeeperApi.request.name(CommonUtils.randomString(10)).peerPort(peerPort).nodeNames(nodeNames).create()
    )
    result(zookeeperApi.start(zk.key))

    val zk2 = result(
      zookeeperApi.request.name(CommonUtils.randomString(10)).peerPort(peerPort).nodeNames(nodeNames).create()
    )

    intercept[IllegalArgumentException] {
      result(zookeeperApi.start(zk2.key))
    }.getMessage should include(peerPort.toString)
  }

  @Test
  def electionPortConflict(): Unit = {
    val electionPort = CommonUtils.availablePort()
    val zk = result(
      zookeeperApi.request.name(CommonUtils.randomString(10)).electionPort(electionPort).nodeNames(nodeNames).create()
    )
    result(zookeeperApi.start(zk.key))

    val zk2 = result(
      zookeeperApi.request.name(CommonUtils.randomString(10)).electionPort(electionPort).nodeNames(nodeNames).create()
    )

    intercept[IllegalArgumentException] {
      result(zookeeperApi.start(zk2.key))
    }.getMessage should include(electionPort.toString)
  }

  @Test
  def testIdempotentStart(): Unit = {
    val zk = result(
      zookeeperApi.request.nodeNames(nodeNames).create()
    )
    result(zookeeperApi.start(zk.key))
    val info1 = result(zookeeperApi.get(zk.key))
    // duplicated start will return the current cluster info
    result(zookeeperApi.start(zk.key))
    val info2 = result(zookeeperApi.get(zk.key))
    info1.name shouldBe info2.name
    info1.imageName shouldBe info2.imageName
    info1.nodeNames shouldBe info2.nodeNames
    info1.state shouldBe info2.state
    info1.error shouldBe info2.error
    info1.electionPort shouldBe info2.electionPort

    // we could graceful stop zookeeper
    result(zookeeperApi.stop(zk.key))
    // stop should be idempotent
    result(zookeeperApi.stop(zk.key))
    // delete should be idempotent also
    result(zookeeperApi.delete(zk.key))
    result(zookeeperApi.delete(zk.key))
    // after delete, stop will cause NoSuchElement exception
    an[IllegalArgumentException] should be thrownBy result(zookeeperApi.stop(zk.key))
  }

  @Test
  def testForceStop(): Unit = {
    val initialCount = configurator.serviceCollie.zookeeperCollie.asInstanceOf[FakeZookeeperCollie].forceRemoveCount
    val name         = CommonUtils.randomString(10)
    // graceful stop
    val zk = result(
      zookeeperApi.request.name(name).nodeNames(nodeNames).create()
    )
    result(zookeeperApi.start(zk.key))
    result(zookeeperApi.stop(zk.key))
    result(zookeeperApi.delete(zk.key))
    configurator.serviceCollie.zookeeperCollie.asInstanceOf[FakeZookeeperCollie].forceRemoveCount shouldBe initialCount

    // force stop
    val zk2 = result(
      zookeeperApi.request.name(name).nodeNames(nodeNames).create()
    )
    result(zookeeperApi.start(zk2.key))
    result(zookeeperApi.forceStop(zk2.key))
    result(zookeeperApi.delete(zk2.key))
    configurator.serviceCollie.zookeeperCollie
      .asInstanceOf[FakeZookeeperCollie]
      .forceRemoveCount shouldBe initialCount + 1
  }

  @Test
  def failToUpdateRunningZookeeperCluster(): Unit = {
    val zk = result(zookeeperApi.request.nodeName(nodeNames.head).create())
    result(zookeeperApi.start(zk.key))
    an[IllegalArgumentException] should be thrownBy result(
      zookeeperApi.request.name(zk.name).nodeNames(nodeNames).update()
    )
    result(zookeeperApi.stop(zk.key))
    result(zookeeperApi.request.name(zk.name).nodeNames(nodeNames).update())
    result(zookeeperApi.start(zk.key))
  }

  @Test
  def testCustomTagsShouldExistAfterRunning(): Unit = {
    val tags = Map(
      "aa" -> JsString("bb"),
      "cc" -> JsNumber(123),
      "dd" -> JsArray(JsString("bar"), JsString("foo"))
    )
    val zk = result(zookeeperApi.request.tags(tags).nodeNames(nodeNames).create())
    zk.tags shouldBe tags

    // after create, tags should exist
    result(zookeeperApi.get(zk.key)).tags shouldBe tags

    // after start, tags should still exist
    result(zookeeperApi.start(zk.key))
    result(zookeeperApi.get(zk.key)).tags shouldBe tags

    // after stop, tags should still exist
    result(zookeeperApi.stop(zk.key))
    result(zookeeperApi.get(zk.key)).tags shouldBe tags
  }

  @Test
  def testGroup(): Unit = {
    val group = CommonUtils.randomString(10)
    // different name but same group
    result(zookeeperApi.request.group(group).nodeNames(nodeNames).create()).group shouldBe group
    result(zookeeperApi.request.group(group).nodeNames(nodeNames).create()).group shouldBe group

    // number of created (2) + configurator default (1)
    result(zookeeperApi.list()).size shouldBe 3

    // same name but different group
    val name = CommonUtils.randomString(10)
    val zk1  = result(zookeeperApi.request.name(name).nodeNames(nodeNames).create())
    zk1.name shouldBe name
    zk1.group should not be group
    val zk2 = result(zookeeperApi.request.name(name).group(group).nodeNames(nodeNames).create())
    zk2.name shouldBe name
    zk2.group shouldBe group

    // number of created (4) + configurator default (1)
    result(zookeeperApi.list()).size shouldBe 5
  }

  @Test
  def testUpdateAsCreateRequest(): Unit = {
    val info = result(zookeeperApi.request.nodeNames(nodeNames).create())

    // use same name and group will cause a update request
    result(zookeeperApi.request.name(info.name).group(info.group).clientPort(1234).update()).clientPort shouldBe 1234

    // use different group will cause a create request
    result(
      zookeeperApi.request
        .name(info.name)
        .group(CommonUtils.randomString(10))
        .nodeNames(nodeNames)
        .peerPort(1234)
        .update()
    ).peerPort should not be info.peerPort
  }

  @Test
  def testNameFilter(): Unit = {
    val name      = CommonUtils.randomString(10)
    val zookeeper = result(zookeeperApi.request.name(name).nodeNames(nodeNames).create())
    (0 until 3).foreach(_ => result(zookeeperApi.request.nodeNames(nodeNames).create()))
    result(zookeeperApi.list()).size shouldBe 5
    val zookeepers = result(zookeeperApi.query.name(name).execute())
    zookeepers.size shouldBe 1
    zookeepers.head.key shouldBe zookeeper.key
  }

  @Test
  def testGroupFilter(): Unit = {
    val group     = CommonUtils.randomString(10)
    val zookeeper = result(zookeeperApi.request.group(group).nodeNames(nodeNames).create())
    (0 until 3).foreach(_ => result(zookeeperApi.request.nodeNames(nodeNames).create()))
    result(zookeeperApi.list()).size shouldBe 5
    val zookeepers = result(zookeeperApi.query.group(group).execute())
    zookeepers.size shouldBe 1
    zookeepers.head.key shouldBe zookeeper.key
  }

  @Test
  def testTagsFilter(): Unit = {
    val tags = Map(
      "a" -> JsString("b"),
      "b" -> JsNumber(123),
      "c" -> JsTrue,
      "d" -> JsArray(JsString("B")),
      "e" -> JsObject("a" -> JsNumber(123))
    )
    val zookeeper = result(zookeeperApi.request.tags(tags).nodeNames(nodeNames).create())
    (0 until 3).foreach(_ => result(zookeeperApi.request.nodeNames(nodeNames).create()))
    result(zookeeperApi.list()).size shouldBe 5
    val zookeepers = result(zookeeperApi.query.tags(tags).execute())
    zookeepers.size shouldBe 1
    zookeepers.head.key shouldBe zookeeper.key
  }

  @Test
  def testStateFilter(): Unit = {
    val zookeeper = result(zookeeperApi.request.nodeNames(nodeNames).create())
    (0 until 3).foreach(_ => result(zookeeperApi.request.nodeNames(nodeNames).create()))
    result(zookeeperApi.list()).size shouldBe 5
    result(zookeeperApi.start(zookeeper.key))
    val zookeepers = result(zookeeperApi.query.state("RUNNING").execute())
    zookeepers.size shouldBe 2
    zookeepers.find(_.key == zookeeper.key) should not be None

    result(zookeeperApi.query.group(CommonUtils.randomString()).state("RUNNING").execute()).size shouldBe 0
    result(zookeeperApi.query.state("none").execute()).size shouldBe 3
  }

  @Test
  def testAliveNodesFilter(): Unit = {
    val zookeeper = result(zookeeperApi.request.nodeNames(nodeNames).create())
    (0 until 3).foreach(
      _ => result(zookeeperApi.request.nodeName(nodeNames.head).create().flatMap(z => zookeeperApi.start(z.key)))
    )
    result(zookeeperApi.list()).size shouldBe 5
    result(zookeeperApi.start(zookeeper.key))
    val zookeepers = result(zookeeperApi.query.aliveNodes(nodeNames).execute())
    zookeepers.size shouldBe 1
    zookeepers.head.key shouldBe zookeeper.key
  }

  @Test
  def failToStopZookeeperIfThereIsBroker(): Unit = {
    val zookeeper = result(zookeeperApi.request.nodeNames(nodeNames).create())
    result(zookeeperApi.start(zookeeper.key))
    def brokerApi = BrokerApi.access.hostname(configurator.hostname).port(configurator.port)
    val broker    = result(brokerApi.request.zookeeperClusterKey(zookeeper.key).nodeNames(nodeNames).create())
    result(brokerApi.start(broker.key))

    intercept[IllegalArgumentException] {
      result(zookeeperApi.stop(zookeeper.key))
    }.getMessage should include(broker.key.toString)

    result(brokerApi.stop(broker.key))
    result(zookeeperApi.stop(zookeeper.key))

    intercept[IllegalArgumentException] {
      result(zookeeperApi.delete(zookeeper.key))
    }.getMessage should include(broker.key.toString)

    result(brokerApi.delete(broker.key))
    result(zookeeperApi.delete(zookeeper.key))
  }

  @Test
  def testInvalidNodeName(): Unit =
    Set(START_COMMAND, STOP_COMMAND, PAUSE_COMMAND, RESUME_COMMAND).foreach { nodeName =>
      intercept[DeserializationException] {
        result(zookeeperApi.request.nodeName(nodeName).create())
      }.getMessage should include(nodeName)
    }

  @Test
  def testInitHeap(): Unit =
    result(zookeeperApi.request.nodeNames(nodeNames).initHeap(12345).create()).initHeap shouldBe 12345

  @Test
  def testMaxHeap(): Unit =
    result(zookeeperApi.request.nodeNames(nodeNames).maxHeap(12345).create()).maxHeap shouldBe 12345

  @Test
  def userDefinedStateShouldBeRemoveFromCreation(): Unit =
    result(
      zookeeperApi.request
        .nodeNames(nodeNames)
        .initHeap(12345)
        .setting("state", JsString("this is illegal field"))
        .create()
    ).state shouldBe None

  @Test
  def userDefinedStateShouldBeRemoveFromUpdating(): Unit =
    result(
      zookeeperApi.request
        .name(CommonUtils.randomString(5))
        .nodeNames(nodeNames)
        .initHeap(12345)
        .setting("state", JsString("this is illegal field"))
        .update()
    ).state shouldBe None

  @Test
  def testVolumes(): Unit = {
    val volume = result(volumeApi.request.path(CommonUtils.randomString(5)).nodeNames(Set(nodeNames.head)).create())
    result(volumeApi.start(volume.key))
    val zookeeper = result(
      zookeeperApi.request
        .name(CommonUtils.randomString(5))
        .nodeNames(Set(nodeNames.head))
        .initHeap(12345)
        .dataDir(volume.key)
        .update()
    )
    result(zookeeperApi.start(zookeeper.key))
    result(zookeeperApi.get(zookeeper.key)).nodeNames shouldBe Set(nodeNames.head)

    an[IllegalArgumentException] should be thrownBy result(zookeeperApi.addNode(zookeeper.key, nodeNames.last))
    result(zookeeperApi.get(zookeeper.key)).nodeNames shouldBe Set(nodeNames.head)
  }

  @AfterEach
  def tearDown(): Unit = Releasable.close(configurator)
}
