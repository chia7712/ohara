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

import java.util.concurrent.TimeUnit

import oharastream.ohara.client.configurator.{
  BrokerApi,
  ClusterState,
  ConnectorApi,
  FileInfoApi,
  NodeApi,
  TopicApi,
  WorkerApi
}
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.setting.ObjectKey
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.configurator.Configurator
import oharastream.ohara.configurator.fake.FakeWorkerCollie
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.scalatest.matchers.should.Matchers._
import spray.json.{DeserializationException, JsArray, JsNumber, JsObject, JsString, JsTrue}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
class TestWorkerRoute extends OharaTest {
  private[this] val numberOfCluster = 1
  private[this] val configurator    = Configurator.builder.fake(numberOfCluster, 0).build()

  /**
    * a fake cluster has 3 fake node.
    */
  private[this] val numberOfDefaultNodes = 3 * numberOfCluster
  private[this] val workerApi            = WorkerApi.access.hostname(configurator.hostname).port(configurator.port)

  private[this] val brokerClusterKey =
    Await
      .result(
        BrokerApi.access.hostname(configurator.hostname).port(configurator.port).list(),
        Duration(20, TimeUnit.SECONDS)
      )
      .head
      .key

  private[this] val nodeNames: Set[String] = Set("n0", "n1")

  private[this] val fileApi: FileInfoApi.Access =
    FileInfoApi.access.hostname(configurator.hostname).port(configurator.port)

  private[this] def result[T](f: Future[T]): T = Await.result(f, Duration(30, TimeUnit.SECONDS))

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
      result(workerApi.delete(ObjectKey.of(index.toString, index.toString)))
      result(workerApi.removeNode(ObjectKey.of(index.toString, index.toString), index.toString))
    }
  }

  @Test
  def testDefaultBk(): Unit =
    result(
      workerApi.request
        .name(CommonUtils.randomString(10))
        .nodeNames(nodeNames)
        .brokerClusterKey(brokerClusterKey)
        .create()
    ).brokerClusterKey shouldBe brokerClusterKey

  @Test
  def createWithIncorrectBk(): Unit =
    an[IllegalArgumentException] should be thrownBy result(
      workerApi.request
        .name(CommonUtils.randomString(10))
        .nodeNames(nodeNames)
        .brokerClusterKey(ObjectKey.of("default", CommonUtils.randomString()))
        .create()
    )

  @Test
  def testAllSetting(): Unit = {
    val name                           = CommonUtils.randomString(10)
    val clientPort                     = CommonUtils.availablePort()
    val jmxPort                        = CommonUtils.availablePort()
    val groupId                        = CommonUtils.randomString(10)
    val configTopicName                = CommonUtils.randomString(10)
    val configTopicReplications: Short = 2
    val offsetTopicName                = CommonUtils.randomString(10)
    val offsetTopicPartitions          = 2
    val offsetTopicReplications: Short = 2
    val statusTopicName                = CommonUtils.randomString(10)
    val statusTopicPartitions          = 2
    val statusTopicReplications: Short = 2

    val wkCluster = result(
      workerApi.request
        .name(name)
        .clientPort(clientPort)
        .jmxPort(jmxPort)
        .groupId(groupId)
        .brokerClusterKey(brokerClusterKey)
        .configTopicName(configTopicName)
        .configTopicReplications(configTopicReplications)
        .offsetTopicName(offsetTopicName)
        .offsetTopicPartitions(offsetTopicPartitions)
        .offsetTopicReplications(offsetTopicReplications)
        .statusTopicName(statusTopicName)
        .statusTopicPartitions(statusTopicPartitions)
        .statusTopicReplications(statusTopicReplications)
        .nodeNames(nodeNames)
        .create()
    )
    wkCluster.jmxPort shouldBe jmxPort
    wkCluster.clientPort shouldBe clientPort
    wkCluster.groupId shouldBe groupId
    wkCluster.configTopicName shouldBe configTopicName
    wkCluster.configTopicReplications shouldBe configTopicReplications
    wkCluster.offsetTopicName shouldBe offsetTopicName
    wkCluster.offsetTopicPartitions shouldBe offsetTopicPartitions
    wkCluster.offsetTopicReplications shouldBe offsetTopicReplications
    wkCluster.statusTopicName shouldBe statusTopicName
    wkCluster.statusTopicPartitions shouldBe statusTopicPartitions
    wkCluster.statusTopicReplications shouldBe statusTopicReplications
  }

  @Test
  def testCreateOnNonexistentNode(): Unit =
    an[IllegalArgumentException] should be thrownBy result(
      workerApi.request
        .name(CommonUtils.randomString(10))
        .nodeName(CommonUtils.randomString(10))
        .brokerClusterKey(brokerClusterKey)
        .create()
        .flatMap(wk => workerApi.start(wk.key))
    )

  @Test
  def testList(): Unit = {
    val count = 5
    (0 until count).foreach { _ =>
      result(
        workerApi.request
          .name(CommonUtils.randomString(10))
          .nodeNames(nodeNames)
          .brokerClusterKey(brokerClusterKey)
          .create()
      )
    }
    result(workerApi.list()).size shouldBe count
  }

  @Test
  def testRemove(): Unit = {
    val cluster = result(
      workerApi.request
        .name(CommonUtils.randomString(10))
        .nodeNames(nodeNames)
        .brokerClusterKey(brokerClusterKey)
        .create()
    )
    result(workerApi.list()).size shouldBe 1
    result(workerApi.delete(cluster.key))
    result(workerApi.list()).size shouldBe 0
  }

  @Test
  def testAddNode(): Unit = {
    val cluster = result(
      workerApi.request
        .name(CommonUtils.randomString(10))
        .nodeName(nodeNames.head)
        .brokerClusterKey(brokerClusterKey)
        .create()
    )
    result(workerApi.start(cluster.key))
    result(workerApi.addNode(cluster.key, nodeNames.last).flatMap(_ => workerApi.get(cluster.key))).nodeNames shouldBe cluster.nodeNames ++ Set(
      nodeNames.last
    )
  }

  @Test
  def testRemoveNode(): Unit = {
    val cluster = result(
      workerApi.request
        .name(CommonUtils.randomString(10))
        .nodeNames(nodeNames)
        .brokerClusterKey(brokerClusterKey)
        .create()
    )
    result(workerApi.start(cluster.key))
    result(workerApi.removeNode(cluster.key, nodeNames.head))
    result(workerApi.get(cluster.key)).state shouldBe Some(ClusterState.RUNNING)
    result(workerApi.get(cluster.key)).nodeNames.size shouldBe nodeNames.size - 1
    nodeNames should contain(result(workerApi.get(cluster.key)).nodeNames.head)
    intercept[IllegalArgumentException] {
      result(workerApi.get(cluster.key)).nodeNames.foreach { nodeName =>
        result(workerApi.removeNode(cluster.key, nodeName))
      }
    }.getMessage should include("there is only one instance")
  }

  @Test
  def testInvalidClusterName(): Unit = an[DeserializationException] should be thrownBy result(
    workerApi.request.name("123123-").nodeNames(nodeNames).brokerClusterKey(brokerClusterKey).create()
  )

  @Test
  def createWorkerClusterWithSameName(): Unit = {
    val name = CommonUtils.randomString(10)

    // pass
    result(
      workerApi.request.name(name).nodeNames(nodeNames).brokerClusterKey(brokerClusterKey).create()
    )

    // we don't need to create another bk cluster since it is feasible to create multi wk cluster on same broker cluster
    an[IllegalArgumentException] should be thrownBy result(
      workerApi.request.name(name).nodeNames(nodeNames).brokerClusterKey(brokerClusterKey).create()
    )
  }

  @Test
  def clientPortConflict(): Unit = {
    val clientPort = CommonUtils.availablePort()
    // pass
    result(
      workerApi.request
        .name(CommonUtils.randomString(10))
        .clientPort(clientPort)
        .nodeNames(nodeNames)
        .brokerClusterKey(brokerClusterKey)
        .create()
        .flatMap(wk => workerApi.start(wk.key))
    )

    an[IllegalArgumentException] should be thrownBy result(
      workerApi.request
        .name(CommonUtils.randomString(10))
        .clientPort(clientPort)
        .nodeNames(nodeNames)
        .brokerClusterKey(brokerClusterKey)
        .create()
        .flatMap(wk => workerApi.start(wk.key))
    )
  }

  @Test
  def jmxPortConflict(): Unit = {
    val jmxPort = CommonUtils.availablePort()
    // pass
    result(
      workerApi.request
        .name(CommonUtils.randomString(10))
        .jmxPort(jmxPort)
        .nodeNames(nodeNames)
        .brokerClusterKey(brokerClusterKey)
        .create()
        .flatMap(wk => workerApi.start(wk.key))
    )

    an[IllegalArgumentException] should be thrownBy result(
      workerApi.request
        .name(CommonUtils.randomString(10))
        .jmxPort(jmxPort)
        .nodeNames(nodeNames)
        .brokerClusterKey(brokerClusterKey)
        .create()
        .flatMap(wk => workerApi.start(wk.key))
    )
  }

  @Test
  def duplicateGroupId(): Unit = {
    val groupId = CommonUtils.randomString(10)
    // pass
    result(
      workerApi.request
        .name(CommonUtils.randomString(10))
        .groupId(groupId)
        .nodeNames(nodeNames)
        .brokerClusterKey(brokerClusterKey)
        .create()
        .flatMap(wk => workerApi.start(wk.key))
    )

    an[IllegalArgumentException] should be thrownBy result(
      workerApi.request
        .name(CommonUtils.randomString(10))
        .groupId(groupId)
        .nodeNames(nodeNames)
        .brokerClusterKey(brokerClusterKey)
        .create()
        .flatMap(wk => workerApi.start(wk.key))
    )
  }

  @Test
  def duplicateConfigTopic(): Unit = {
    val configTopicName = CommonUtils.randomString(10)
    // pass
    result(
      workerApi.request
        .name(CommonUtils.randomString(10))
        .configTopicName(configTopicName)
        .nodeNames(nodeNames)
        .brokerClusterKey(brokerClusterKey)
        .create()
        .flatMap(wk => workerApi.start(wk.key))
    )

    an[IllegalArgumentException] should be thrownBy result(
      workerApi.request
        .name(CommonUtils.randomString(10))
        .configTopicName(configTopicName)
        .nodeNames(nodeNames)
        .brokerClusterKey(brokerClusterKey)
        .create()
        .flatMap(wk => workerApi.start(wk.key))
    )
  }

  @Test
  def duplicateOffsetTopic(): Unit = {
    val offsetTopicName = CommonUtils.randomString(10)
    // pass
    result(
      workerApi.request
        .name(CommonUtils.randomString(10))
        .offsetTopicName(offsetTopicName)
        .nodeNames(nodeNames)
        .brokerClusterKey(brokerClusterKey)
        .create()
        .flatMap(wk => workerApi.start(wk.key))
    )

    an[IllegalArgumentException] should be thrownBy result(
      workerApi.request
        .name(CommonUtils.randomString(10))
        .offsetTopicName(offsetTopicName)
        .nodeNames(nodeNames)
        .brokerClusterKey(brokerClusterKey)
        .create()
        .flatMap(wk => workerApi.start(wk.key))
    )
  }

  @Test
  def duplicateStatusTopic(): Unit = {
    val statusTopicName = CommonUtils.randomString(10)
    // pass
    result(
      workerApi.request
        .name(CommonUtils.randomString(10))
        .statusTopicName(statusTopicName)
        .nodeNames(nodeNames)
        .brokerClusterKey(brokerClusterKey)
        .create()
        .flatMap(wk => workerApi.start(wk.key))
    )

    an[IllegalArgumentException] should be thrownBy result(
      workerApi.request
        .name(CommonUtils.randomString(10))
        .statusTopicName(statusTopicName)
        .nodeNames(nodeNames)
        .brokerClusterKey(brokerClusterKey)
        .create()
        .flatMap(wk => workerApi.start(wk.key))
    )
  }

  @Test
  def testForceDelete(): Unit = {
    val initialCount = configurator.serviceCollie.workerCollie.asInstanceOf[FakeWorkerCollie].forceRemoveCount

    // graceful delete
    val wk0 = result(
      workerApi.request
        .name(CommonUtils.randomString(10))
        .nodeNames(nodeNames)
        .brokerClusterKey(brokerClusterKey)
        .create()
    )
    result(workerApi.start(wk0.key))
    result(workerApi.stop(wk0.key))
    result(workerApi.delete(wk0.key))
    configurator.serviceCollie.workerCollie.asInstanceOf[FakeWorkerCollie].forceRemoveCount shouldBe initialCount

    // force delete
    val wk1 = result(
      workerApi.request
        .name(CommonUtils.randomString(10))
        .nodeNames(nodeNames)
        .brokerClusterKey(brokerClusterKey)
        .create()
    )
    result(workerApi.start(wk1.key))
    result(workerApi.forceStop(wk1.key))
    result(workerApi.delete(wk1.key))
    configurator.serviceCollie.workerCollie.asInstanceOf[FakeWorkerCollie].forceRemoveCount shouldBe initialCount + 1
  }

  @Test
  def testCustomTagsShouldExistAfterRunning(): Unit = {
    val tags = Map(
      "aa" -> JsString("bb"),
      "cc" -> JsNumber(123),
      "dd" -> JsArray(JsString("bar"), JsString("foo"))
    )
    val wk = result(workerApi.request.tags(tags).nodeNames(nodeNames).brokerClusterKey(brokerClusterKey).create())
    wk.tags shouldBe tags

    // after create, tags should exist
    val res = result(workerApi.get(wk.key))
    res.tags shouldBe tags

    // after start, tags should still exist
    result(workerApi.start(wk.key))
    val res1 = result(workerApi.get(wk.key))
    res1.tags shouldBe tags

    // after stop, tags should still exist
    result(workerApi.stop(wk.key))
    val res2 = result(workerApi.get(wk.key))
    res2.tags shouldBe tags
  }

  @Test
  def testGroup(): Unit = {
    val group = CommonUtils.randomString(10)
    // different name but same group
    result(workerApi.request.group(group).nodeNames(nodeNames).brokerClusterKey(brokerClusterKey).create()).group shouldBe group
    result(workerApi.request.group(group).nodeNames(nodeNames).brokerClusterKey(brokerClusterKey).create()).group shouldBe group

    result(workerApi.list()).size shouldBe 2

    // same name but different group
    val name = CommonUtils.randomString(10)
    val bk1  = result(workerApi.request.name(name).nodeNames(nodeNames).brokerClusterKey(brokerClusterKey).create())
    bk1.name shouldBe name
    bk1.group should not be group
    val bk2 = result(
      workerApi.request.name(name).group(group).nodeNames(nodeNames).brokerClusterKey(brokerClusterKey).create()
    )
    bk2.name shouldBe name
    bk2.group shouldBe group

    result(workerApi.list()).size shouldBe 4
  }

  @Test
  def testNameFilter(): Unit = {
    val name   = CommonUtils.randomString(10)
    val worker = result(workerApi.request.name(name).nodeNames(nodeNames).brokerClusterKey(brokerClusterKey).create())
    (0 until 3).foreach(_ => result(workerApi.request.nodeNames(nodeNames).brokerClusterKey(brokerClusterKey).create()))
    result(workerApi.list()).size shouldBe 4
    val workers = result(workerApi.query.name(name).execute())
    workers.size shouldBe 1
    workers.head.key shouldBe worker.key
  }

  @Test
  def testGroupFilter(): Unit = {
    val group  = CommonUtils.randomString(10)
    val worker = result(workerApi.request.group(group).nodeNames(nodeNames).brokerClusterKey(brokerClusterKey).create())
    (0 until 3).foreach(_ => result(workerApi.request.nodeNames(nodeNames).brokerClusterKey(brokerClusterKey).create()))
    result(workerApi.list()).size shouldBe 4
    val workers = result(workerApi.query.group(group).execute())
    workers.size shouldBe 1
    workers.head.key shouldBe worker.key
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
    val worker = result(workerApi.request.tags(tags).nodeNames(nodeNames).brokerClusterKey(brokerClusterKey).create())
    (0 until 3).foreach(_ => result(workerApi.request.nodeNames(nodeNames).brokerClusterKey(brokerClusterKey).create()))
    result(workerApi.list()).size shouldBe 4
    val workers = result(workerApi.query.tags(tags).execute())
    workers.size shouldBe 1
    workers.head.key shouldBe worker.key
  }

  @Test
  def testStateFilter(): Unit = {
    val worker = result(workerApi.request.nodeNames(nodeNames).brokerClusterKey(brokerClusterKey).create())
    (0 until 3).foreach(_ => result(workerApi.request.nodeNames(nodeNames).brokerClusterKey(brokerClusterKey).create()))
    result(workerApi.list()).size shouldBe 4
    result(workerApi.start(worker.key))
    val workers = result(workerApi.query.state("RUNNING").execute())
    workers.size shouldBe 1
    workers.find(_.key == worker.key) should not be None

    result(workerApi.query.group(CommonUtils.randomString()).state("RUNNING").execute()).size shouldBe 0
    result(workerApi.query.state("none").execute()).size shouldBe 3
  }

  @Test
  def testAliveNodesFilter(): Unit = {
    val worker = result(workerApi.request.nodeNames(nodeNames).brokerClusterKey(brokerClusterKey).create())
    (0 until 3).foreach(
      _ =>
        result(
          workerApi.request
            .nodeName(nodeNames.head)
            .brokerClusterKey(brokerClusterKey)
            .create()
            .flatMap(z => workerApi.start(z.key))
        )
    )
    result(workerApi.list()).size shouldBe 4
    result(workerApi.start(worker.key))
    val workers = result(workerApi.query.aliveNodes(nodeNames).execute())
    workers.size shouldBe 1
    workers.head.key shouldBe worker.key
  }

  @Test
  def removeWorkerClusterUsedByConnector(): Unit = {
    def connectorApi =
      ConnectorApi.access.hostname(configurator.hostname).port(configurator.port)
    def topicApi =
      TopicApi.access.hostname(configurator.hostname).port(configurator.port)

    val topic = result(topicApi.request.name(CommonUtils.randomString(10)).brokerClusterKey(brokerClusterKey).create())
    result(topicApi.start(topic.key))

    val worker = result(workerApi.request.nodeName(nodeNames.head).brokerClusterKey(brokerClusterKey).create())
    val connector = result(
      connectorApi.request
        .name(CommonUtils.randomString(10))
        .className(CommonUtils.randomString(10))
        .topicKey(topic.key)
        .workerClusterKey(worker.key)
        .create()
    )

    intercept[IllegalArgumentException] {
      result(workerApi.delete(worker.key))
    }.getMessage should include(connector.key.toString)

    result(workerApi.start(worker.key))
    result(connectorApi.start(connector.key))

    intercept[IllegalArgumentException] {
      result(workerApi.stop(worker.key))
    }.getMessage should include(connector.key.toString)
  }

  @Test
  def testInvalidNodeName(): Unit =
    Set(START_COMMAND, STOP_COMMAND, PAUSE_COMMAND, RESUME_COMMAND).foreach { nodeName =>
      intercept[DeserializationException] {
        result(workerApi.request.nodeName(nodeName).brokerClusterKey(brokerClusterKey).create())
      }.getMessage should include(nodeName)
    }

  @Test
  def testInitHeap(): Unit =
    result(workerApi.request.brokerClusterKey(brokerClusterKey).nodeNames(nodeNames).initHeap(12345).create()).initHeap shouldBe 12345

  @Test
  def testMaxHeap(): Unit =
    result(workerApi.request.brokerClusterKey(brokerClusterKey).nodeNames(nodeNames).maxHeap(12345).create()).maxHeap shouldBe 12345

  @Test
  def testPluginJars(): Unit = {
    val fileInfo = result(fileApi.request.file(RouteUtils.connectorFile).upload())
    val worker = result(
      workerApi.request
        .nodeNames(nodeNames)
        .pluginKeys(Set(fileInfo.key))
        .brokerClusterKey(brokerClusterKey)
        .create()
    )
    worker.pluginKeys.size shouldBe 1
    worker.pluginKeys.head shouldBe fileInfo.key
    worker.sharedJarKeys.size shouldBe 0
  }

  @Test
  def testSharedJars(): Unit = {
    val fileInfo = result(fileApi.request.file(RouteUtils.connectorFile).upload())
    val worker = result(
      workerApi.request
        .nodeNames(nodeNames)
        .sharedJarKeys(Set(fileInfo.key))
        .brokerClusterKey(brokerClusterKey)
        .create()
    )
    worker.sharedJarKeys.size shouldBe 1
    worker.sharedJarKeys.head shouldBe fileInfo.key
    worker.pluginKeys.size shouldBe 0
  }

  @Test
  def testConcurrentlyRestart(): Unit = {
    val worker = result(
      workerApi.request
        .nodeNames(nodeNames)
        .brokerClusterKey(brokerClusterKey)
        .create()
    )

    result(workerApi.start(worker.key))
    result(workerApi.get(worker.key)).state.get shouldBe ClusterState.RUNNING

    val exceptions = (0 until 10)
      .map { index =>
        if (index % 2 == 0) workerApi.stop(worker.key)
        else workerApi.start(worker.key)
      }
      .flatMap(
        f =>
          try {
            result(f)
            None
          } catch {
            case e: Throwable => Some(e)
          }
      )

    exceptions.size should not be 0
    exceptions.count(e => e.getMessage.contains("is stopping") || e.getMessage.contains("is starting")) should not be 0
  }

  @Test
  def testCompressionType(): Unit =
    WorkerApi.CompressionType.all.foreach { compressionType =>
      result(
        workerApi.request
          .nodeNames(nodeNames)
          .brokerClusterKey(brokerClusterKey)
          .compressionType(compressionType)
          .create()
      ).compressionType shouldBe compressionType
    }

  @Test
  def testJvmPerformanceOptions(): Unit =
    result(
      workerApi.request
        .nodeNames(nodeNames)
        .brokerClusterKey(brokerClusterKey)
        .jvmPerformanceOptions("-XX:+UnlockExperimentalVMOptions -XX:+UseZGC")
        .create()
    ).jvmPerformanceOptions.get shouldBe "-XX:+UnlockExperimentalVMOptions -XX:+UseZGC"

  @AfterEach
  def tearDown(): Unit = Releasable.close(configurator)
}
