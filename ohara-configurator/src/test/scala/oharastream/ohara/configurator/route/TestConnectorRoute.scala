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

import oharastream.ohara.client.configurator.ConnectorApi.State
import oharastream.ohara.client.configurator.{BrokerApi, ConnectorApi, TopicApi, WorkerApi, ZookeeperApi}
import oharastream.ohara.common.data.{Column, DataType}
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.setting.{ConnectorKey, ObjectKey, TopicKey, WithDefinitions}
import oharastream.ohara.common.util.{CommonUtils, Releasable, VersionUtils}
import oharastream.ohara.configurator.Configurator
import oharastream.ohara.connector.perf.PerfSourceProps
import oharastream.ohara.kafka.RowDefaultPartitioner
import oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.scalatest.matchers.should.Matchers._
import spray.json.{JsArray, JsNumber, JsObject, JsString, JsTrue, JsValue}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
class TestConnectorRoute extends OharaTest {
  private[this] val configurator = Configurator.builder.fake(1, 1).build()

  private[this] val zookeeperApi = ZookeeperApi.access.hostname(configurator.hostname).port(configurator.port)
  private[this] val brokerApi    = BrokerApi.access.hostname(configurator.hostname).port(configurator.port)
  private[this] val workerApi    = WorkerApi.access.hostname(configurator.hostname).port(configurator.port)
  private[this] val connectorApi = ConnectorApi.access.hostname(configurator.hostname).port(configurator.port)
  private[this] val topicApi     = TopicApi.access.hostname(configurator.hostname).port(configurator.port)

  private[this] val topicKey = TopicKey.of(CommonUtils.randomString(10), CommonUtils.randomString(10))
  private[this] val workerClusterInfo = result(
    WorkerApi.access.hostname(configurator.hostname).port(configurator.port).list()
  ).head
  private[this] val brokerClusterInfo = result(
    BrokerApi.access.hostname(configurator.hostname).port(configurator.port).list()
  ).head
  private[this] val nodeNames = brokerClusterInfo.nodeNames
  private[this] val bkKey     = brokerClusterInfo.key

  private[this] def result[T](f: Future[T]): T = Await.result(f, Duration("20 seconds"))

  private[this] val fakeFtpSettings: Map[String, JsValue] = Map(
    oharastream.ohara.connector.ftp.FTP_HOSTNAME_KEY  -> JsString("hostname"),
    oharastream.ohara.connector.ftp.FTP_PORT_KEY      -> JsNumber(22),
    oharastream.ohara.connector.ftp.FTP_USER_NAME_KEY -> JsString("user"),
    oharastream.ohara.connector.ftp.FTP_PASSWORD_KEY  -> JsString("password"),
    CsvConnectorDefinitions.OUTPUT_FOLDER_KEY         -> JsString("output")
  )

  @BeforeEach
  def setupTopic(): Unit = {
    result(topicApi.request.key(topicKey).brokerClusterKey(bkKey).create())
    result(topicApi.start(topicKey))
  }

  @Test
  def listConnectorDeployedOnStoppedCluster(): Unit = {
    val connector = result(
      connectorApi.request
        .className(CommonUtils.randomString(10))
        .workerClusterKey(workerClusterInfo.key)
        .topicKey(topicKey)
        .create()
    )

    result(WorkerApi.access.hostname(configurator.hostname).port(configurator.port).stop(workerClusterInfo.key))

    result(connectorApi.get(connector.key)).key shouldBe connector.key
  }

  @Test
  def createConnectorWithoutTopics(): Unit =
    result(
      connectorApi.request
        .className(CommonUtils.randomString(10))
        .workerClusterKey(workerClusterInfo.key)
        .create()
    ).topicKeys shouldBe Set.empty

  @Test
  def test(): Unit = {
    // test add
    result(connectorApi.list()).size shouldBe 0

    val columns = Seq(
      Column.builder().name("cf").dataType(DataType.BOOLEAN).order(1).build(),
      Column.builder().name("cf").dataType(DataType.BOOLEAN).order(2).build()
    )
    val name          = CommonUtils.randomString(10)
    val className     = CommonUtils.randomString()
    val numberOfTasks = 3
    val response = result(
      connectorApi.request
        .name(name)
        .className(className)
        .columns(columns)
        .numberOfTasks(numberOfTasks)
        .workerClusterKey(workerClusterInfo.key)
        .topicKey(topicKey)
        .create()
    )
    response.name shouldBe name
    response.className shouldBe className
    response.columns shouldBe columns
    response.numberOfTasks shouldBe numberOfTasks
    response.topicKeys shouldBe Set(topicKey)

    // test update
    val className2     = CommonUtils.randomString()
    val numberOfTasks2 = 5
    val columns2       = Seq(Column.builder().name("cf").dataType(DataType.BOOLEAN).order(1).build())
    val response2 = result(
      connectorApi.request
        .key(response.key)
        .className(className2)
        .columns(columns2)
        .numberOfTasks(numberOfTasks2)
        .workerClusterKey(workerClusterInfo.key)
        .topicKey(topicKey)
        .update()
    )
    response2.name shouldBe name
    // the classname is NOT editable
    response2.className shouldBe className
    response2.columns shouldBe columns2
    response2.numberOfTasks shouldBe numberOfTasks2

    // test delete
    result(connectorApi.list()).size shouldBe 1
    result(connectorApi.delete(response.key))
    result(connectorApi.list()).size shouldBe 0

    // test nonexistent data
    an[IllegalArgumentException] should be thrownBy result(
      connectorApi.get(ObjectKey.of(CommonUtils.randomString(), CommonUtils.randomString()))
    )
  }

  @Test
  def removeConnectorFromDeletedCluster(): Unit = {
    val connector = result(
      connectorApi.request
        .name(CommonUtils.randomString(10))
        .className(CommonUtils.randomString(10))
        .workerClusterKey(workerClusterInfo.key)
        .topicKey(topicKey)
        .create()
    )

    connector.workerClusterKey shouldBe workerClusterInfo.key
    result(configurator.serviceCollie.workerCollie.remove(workerClusterInfo.key))

    result(connectorApi.delete(connector.key))

    result(connectorApi.list()).exists(_.name == connector.name) shouldBe false
  }

  @Test
  def runConnectorOnNonexistentCluster(): Unit =
    an[IllegalArgumentException] should be thrownBy result(
      connectorApi.request
        .name(CommonUtils.randomString(10))
        .className(CommonUtils.randomString(10))
        .workerClusterKey(ObjectKey.of(CommonUtils.randomString(), CommonUtils.randomString()))
        .topicKey(topicKey)
        .create()
    )

  @Test
  def runConnectorWithoutSpecificCluster(): Unit = {
    val topic = result(topicApi.request.name(CommonUtils.randomString(10)).brokerClusterKey(bkKey).create())

    // absent worker cluster is ok since there is only one worker cluster
    val connector = result(
      connectorApi.request
        .className("oharastream.ohara.connector.ftp.FtpSink")
        .settings(fakeFtpSettings)
        .topicKey(topic.key)
        .workerClusterKey(workerClusterInfo.key)
        .topicKey(topicKey)
        .create()
    )
    // In creation, workerClusterName will not be auto-filled
    connector.workerClusterKey shouldBe workerClusterInfo.key
    // data stored in configurator should also get the auto-filled result
    result(connectorApi.get(connector.key)).workerClusterKey shouldBe workerClusterInfo.key

    val bk = result(BrokerApi.access.hostname(configurator.hostname).port(configurator.port).list()).head

    val wk = result(
      WorkerApi.access
        .hostname(configurator.hostname)
        .port(configurator.port)
        .request
        .name(CommonUtils.randomString(10))
        .brokerClusterKey(bk.key)
        .nodeNames(bk.nodeNames)
        .create()
    )
    result(WorkerApi.access.hostname(configurator.hostname).port(configurator.port).start(wk.key))

    val c2 = result(
      ConnectorApi.access
        .hostname(configurator.hostname)
        .port(configurator.port)
        .request
        .name(CommonUtils.randomString(10))
        .className(CommonUtils.randomString(10))
        .topicKey(topic.key)
        .workerClusterKey(wk.key)
        .create()
    )
    //pass since we have assigned a worker cluster
    result(topicApi.start(topic.key))
    result(connectorApi.start(c2.key))
  }

  @Test
  def testIdempotentPause(): Unit = {
    val topic = result(topicApi.request.name(CommonUtils.randomString(10)).brokerClusterKey(bkKey).create())

    val connector = result(
      connectorApi.request
        .name(CommonUtils.randomString(10))
        .className(CommonUtils.randomString(10))
        .topicKey(topic.key)
        .workerClusterKey(workerClusterInfo.key)
        .create()
    )

    result(topicApi.start(topic.key))
    result(connectorApi.start(connector.key))

    (0 to 10).foreach(_ => result(connectorApi.pause(connector.key)))
  }

  @Test
  def testIdempotentResume(): Unit = {
    val topic = result(topicApi.request.name(CommonUtils.randomString(10)).brokerClusterKey(bkKey).create())

    val connector = result(
      connectorApi.request
        .name(CommonUtils.randomString(10))
        .className(CommonUtils.randomString(10))
        .topicKey(topic.key)
        .workerClusterKey(workerClusterInfo.key)
        .create()
    )

    result(topicApi.start(topic.key))
    result(connectorApi.start(connector.key))

    (0 to 10).foreach(_ => result(connectorApi.resume(connector.key)))
  }

  @Test
  def testIdempotentStop(): Unit = {
    val topic = result(topicApi.request.name(CommonUtils.randomString(10)).brokerClusterKey(bkKey).create())

    val connector = result(
      connectorApi.request
        .name(CommonUtils.randomString(10))
        .className(CommonUtils.randomString(10))
        .topicKey(topic.key)
        .workerClusterKey(workerClusterInfo.key)
        .create()
    )

    result(topicApi.start(topic.key))
    result(connectorApi.start(connector.key))

    (0 to 10).foreach(_ => result(connectorApi.stop(connector.key)))
  }

  @Test
  def testIdempotentStart(): Unit = {
    val topic = result(topicApi.request.name(CommonUtils.randomString(10)).brokerClusterKey(bkKey).create())

    val connector = result(
      connectorApi.request
        .name(CommonUtils.randomString(10))
        .className(CommonUtils.randomString(10))
        .topicKey(topic.key)
        .workerClusterKey(workerClusterInfo.key)
        .create()
    )

    result(topicApi.start(topic.key))
    result(connectorApi.start(connector.key))

    (0 to 10).foreach(_ => result(connectorApi.start(connector.key)))
  }

  @Test
  def failToChangeWorkerCluster(): Unit = {
    val bk = result(BrokerApi.access.hostname(configurator.hostname).port(configurator.port).list()).head

    val wk = result(
      WorkerApi.access
        .hostname(configurator.hostname)
        .port(configurator.port)
        .request
        .name(CommonUtils.randomString(10))
        .brokerClusterKey(bk.key)
        .nodeNames(bk.nodeNames)
        .create()
    )
    result(WorkerApi.access.hostname(configurator.hostname).port(configurator.port).start(wk.key))
    val topic = result(topicApi.request.name(CommonUtils.randomString(10)).brokerClusterKey(bkKey).create())

    val response = result(
      connectorApi.request
        .name(CommonUtils.randomString(10))
        .className(CommonUtils.randomString(10))
        .workerClusterKey(workerClusterInfo.key)
        .topicKey(topic.key)
        .create()
    )

    response.workerClusterKey shouldBe workerClusterInfo.key
    result(topicApi.start(topic.key))
    result(connectorApi.start(response.key))
    // after start, you cannot change worker cluster
    an[IllegalArgumentException] should be thrownBy result(
      connectorApi.request.key(response.key).className(CommonUtils.randomString(10)).workerClusterKey(wk.key).update()
    )

    result(connectorApi.stop(response.key))

    // the connector is stopped so it is ok to update it now.
    result(
      connectorApi.request
        .key(response.key)
        .className(CommonUtils.randomString(10))
        .workerClusterKey(wk.key)
        .update()
    ).workerClusterKey shouldBe wk.key
  }

  @Test
  def testStartAnNonexistentConnector(): Unit =
    an[IllegalArgumentException] should be thrownBy result(
      connectorApi.start(ConnectorKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5)))
    )

  @Test
  def testStopAnNonexistentConnector(): Unit =
    an[IllegalArgumentException] should be thrownBy result(
      connectorApi.stop(ConnectorKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5)))
    )
  @Test
  def testPauseAnNonexistentConnector(): Unit =
    an[IllegalArgumentException] should be thrownBy result(
      connectorApi.pause(ConnectorKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5)))
    )

  @Test
  def testResumeAnNonexistentConnector(): Unit =
    an[IllegalArgumentException] should be thrownBy result(
      connectorApi.resume(ConnectorKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5)))
    )

  @Test
  def updateTags(): Unit = {
    val tags = Map(
      CommonUtils.randomString(10) -> JsString(CommonUtils.randomString(10)),
      CommonUtils.randomString(10) -> JsNumber(CommonUtils.randomInteger())
    )
    val connectorDesc = result(
      connectorApi.request
        .className("oharastream.ohara.connector.ftp.FtpSink")
        .settings(fakeFtpSettings)
        .tags(tags)
        .workerClusterKey(workerClusterInfo.key)
        .topicKey(topicKey)
        .create()
    )
    connectorDesc.tags shouldBe tags

    val tags2 = Map(
      CommonUtils.randomString(10) -> JsString(CommonUtils.randomString(10)),
      CommonUtils.randomString(10) -> JsNumber(CommonUtils.randomInteger())
    )
    val connectorDesc2 = result(
      connectorApi.request
        .className("oharastream.ohara.connector.ftp.FtpSink")
        .settings(fakeFtpSettings)
        .name(connectorDesc.name)
        .tags(tags2)
        .update()
    )
    connectorDesc2.tags shouldBe tags2

    val connectorDesc3 = result(connectorApi.request.name(connectorDesc.name).update())
    connectorDesc3.tags shouldBe tags2

    val connectorDesc4 = result(connectorApi.request.name(connectorDesc.name).tags(Map.empty).update())
    connectorDesc4.tags shouldBe Map.empty
  }

  @Test
  def failToDeletePropertiesOfRunningConnector(): Unit = {
    val topic = result(topicApi.request.name(CommonUtils.randomString(10)).brokerClusterKey(bkKey).create())
    val connectorDesc = result(
      connectorApi.request
        .className("oharastream.ohara.connector.ftp.FtpSink")
        .settings(fakeFtpSettings)
        .topicKey(topic.key)
        .workerClusterKey(workerClusterInfo.key)
        .create()
    )
    result(topicApi.start(topic.key))
    result(connectorApi.start(connectorDesc.key))

    an[IllegalArgumentException] should be thrownBy result(connectorApi.request.key(connectorDesc.key).update())
    result(connectorApi.stop(connectorDesc.key))
    result(connectorApi.request.key(connectorDesc.key).update())
  }

  @Test
  def failToRunConnectorWithStoppedTopic(): Unit = {
    val topic = result(topicApi.request.name(CommonUtils.randomString(10)).brokerClusterKey(bkKey).create())
    val connectorDesc = result(
      connectorApi.request
        .className("oharastream.ohara.connector.ftp.FtpSink")
        .settings(fakeFtpSettings)
        .topicKey(topic.key)
        .workerClusterKey(workerClusterInfo.key)
        .create()
    )
    an[IllegalArgumentException] should be thrownBy result(connectorApi.start(connectorDesc.key))

    result(topicApi.start(topic.key))
    result(connectorApi.start(connectorDesc.key))
  }

  @Test
  def testNameFilter(): Unit = {
    val name  = CommonUtils.randomString(10)
    val topic = result(topicApi.request.brokerClusterKey(bkKey).create())
    val connectorInfo = result(
      connectorApi.request
        .name(name)
        .className("oharastream.ohara.connector.ftp.FtpSink")
        .settings(fakeFtpSettings)
        .topicKey(topic.key)
        .workerClusterKey(workerClusterInfo.key)
        .create()
    )
    (0 until 3).foreach(
      _ =>
        result(
          connectorApi.request
            .className("oharastream.ohara.connector.ftp.FtpSink")
            .settings(fakeFtpSettings)
            .topicKey(topic.key)
            .workerClusterKey(workerClusterInfo.key)
            .create()
        )
    )
    result(connectorApi.list()).size shouldBe 4
    val connectors = result(connectorApi.query.name(name).execute())
    connectors.size shouldBe 1
    connectors.head.key shouldBe connectorInfo.key
  }

  @Test
  def testGroupFilter(): Unit = {
    val group = CommonUtils.randomString(10)
    val topic = result(topicApi.request.brokerClusterKey(bkKey).create())
    val connectorInfo = result(
      connectorApi.request
        .group(group)
        .className("oharastream.ohara.connector.ftp.FtpSink")
        .settings(fakeFtpSettings)
        .topicKey(topic.key)
        .workerClusterKey(workerClusterInfo.key)
        .create()
    )
    (0 until 3).foreach(
      _ =>
        result(
          connectorApi.request
            .className("oharastream.ohara.connector.ftp.FtpSink")
            .settings(fakeFtpSettings)
            .topicKey(topic.key)
            .workerClusterKey(workerClusterInfo.key)
            .create()
        )
    )
    result(connectorApi.list()).size shouldBe 4
    val connectors = result(connectorApi.query.group(group).execute())
    connectors.size shouldBe 1
    connectors.head.key shouldBe connectorInfo.key
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
    val topic = result(topicApi.request.brokerClusterKey(bkKey).create())
    val connectorInfo = result(
      connectorApi.request
        .className("oharastream.ohara.connector.ftp.FtpSink")
        .settings(fakeFtpSettings)
        .topicKey(topic.key)
        .tags(tags)
        .workerClusterKey(workerClusterInfo.key)
        .create()
    )
    (0 until 3).foreach(
      _ =>
        result(
          connectorApi.request
            .className("oharastream.ohara.connector.ftp.FtpSink")
            .settings(fakeFtpSettings)
            .topicKey(topic.key)
            .workerClusterKey(workerClusterInfo.key)
            .create()
        )
    )
    result(connectorApi.list()).size shouldBe 4
    val connectors = result(connectorApi.query.tags(tags).execute())
    connectors.size shouldBe 1
    connectors.head.key shouldBe connectorInfo.key
  }

  @Test
  def testSettingFilter(): Unit = {
    val topic = result(topicApi.request.brokerClusterKey(bkKey).create())
    val connectorInfo = result(
      connectorApi.request
        .className("oharastream.ohara.connector.ftp.FtpSink2")
        .topicKey(topic.key)
        .workerClusterKey(workerClusterInfo.key)
        .create()
    )
    (0 until 3).foreach(
      _ =>
        result(
          connectorApi.request
            .className("oharastream.ohara.connector.ftp.FtpSink")
            .settings(fakeFtpSettings)
            .topicKey(topic.key)
            .workerClusterKey(workerClusterInfo.key)
            .create()
        )
    )
    result(connectorApi.list()).size shouldBe 4
    val connectors = result(
      connectorApi.query
        .setting(ConnectorApi.CONNECTOR_CLASS_KEY, JsString("oharastream.ohara.connector.ftp.FtpSink2"))
        .execute()
    )
    connectors.size shouldBe 1
    connectors.head.key shouldBe connectorInfo.key
  }

  @Test
  def failToRunOnStoppedCluster(): Unit = {
    val topic = result(topicApi.request.brokerClusterKey(bkKey).create())
    result(topicApi.start(topic.key))
    val worker = result(
      workerApi.request.nodeNames(brokerClusterInfo.nodeNames).brokerClusterKey(brokerClusterInfo.key).create()
    )

    val connector = result(
      connectorApi.request
        .topicKey(topic.key)
        .className("oharastream.ohara.connector.ftp.FtpSink")
        .settings(fakeFtpSettings)
        .workerClusterKey(worker.key)
        .create()
    )
    an[IllegalArgumentException] should be thrownBy result(connectorApi.start(connector.key))

    result(workerApi.start(worker.key))
    result(connectorApi.start(connector.key))
  }

  @Test
  def topicMustOnSameBrokerCluster(): Unit = {
    val zk = result(zookeeperApi.request.nodeNames(nodeNames).create())
    result(zookeeperApi.start(zk.key))
    val bk = result(brokerApi.request.zookeeperClusterKey(zk.key).nodeNames(nodeNames).create())
    result(brokerApi.start(bk.key))

    // put those topics on different broker cluster
    val topic = result(topicApi.request.brokerClusterKey(bk.key).create())
    result(topicApi.start(topic.key))

    val connector = result(
      connectorApi.request
        .topicKey(topic.key)
        .className("oharastream.ohara.connector.ftp.FtpSink")
        .settings(fakeFtpSettings)
        .workerClusterKey(workerClusterInfo.key)
        .create()
    )

    intercept[IllegalArgumentException] {
      result(connectorApi.start(connector.key))
    }.getMessage should include("another broker cluster")
  }

  @Test
  def testPartialFilter(): Unit = {
    val topic = result(topicApi.request.brokerClusterKey(brokerClusterInfo.key).create())
    val tags1 = Map(
      "a" -> JsString("b"),
      "b" -> JsNumber(123),
      "c" -> JsTrue,
      "d" -> JsArray(JsString("B")),
      "e" -> JsObject("a" -> JsNumber(123))
    )
    val tags2 = tags1 - "e"
    val connector = result(
      connectorApi.request
        .topicKey(topic.key)
        .className("oharastream.ohara.connector.ftp.FtpSink")
        .settings(fakeFtpSettings)
        .workerClusterKey(workerClusterInfo.key)
        .tags(tags1)
        .create()
    )
    result(topicApi.start(topic.key))
    result(connectorApi.start(connector.key))
    (0 until 3).foreach(
      _ =>
        result(
          connectorApi.request
            .topicKey(topic.key)
            .className("oharastream.ohara.connector.ftp.FtpSink")
            .settings(fakeFtpSettings)
            .workerClusterKey(workerClusterInfo.key)
            .tags(tags2)
            .create()
        )
    )
    result(connectorApi.list()).size shouldBe 4
    result(connectorApi.query.state(State.RUNNING).execute()).size shouldBe 1
    result(connectorApi.query.noState.execute()).size shouldBe 3
    result(connectorApi.query.tags(tags1).execute()).size shouldBe 1
    result(connectorApi.query.tags(tags2).execute()).size shouldBe 4
    result(connectorApi.query.tags(tags2).name(connector.name).execute()).size shouldBe 1
  }

  @Test
  def testDefaultPartitioner(): Unit = {
    val topic = result(topicApi.request.brokerClusterKey(brokerClusterInfo.key).create())
    result(
      connectorApi.request
        .topicKey(topic.key)
        .className("oharastream.ohara.connector.ftp.FtpSink")
        .settings(fakeFtpSettings)
        .workerClusterKey(workerClusterInfo.key)
        .create()
    ).partitionClass shouldBe classOf[RowDefaultPartitioner].getName
  }

  @Test
  def testDefaultAuthor(): Unit = {
    val topic = result(topicApi.request.brokerClusterKey(brokerClusterInfo.key).create())
    result(
      connectorApi.request
        .topicKey(topic.key)
        .className("oharastream.ohara.connector.ftp.FtpSink")
        .settings(fakeFtpSettings)
        .workerClusterKey(workerClusterInfo.key)
        .create()
    ).settings(WithDefinitions.AUTHOR_KEY).asInstanceOf[JsString].value shouldBe VersionUtils.USER
  }

  @Test
  def testDefaultVersion(): Unit = {
    val topic = result(topicApi.request.brokerClusterKey(brokerClusterInfo.key).create())
    result(
      connectorApi.request
        .topicKey(topic.key)
        .className("oharastream.ohara.connector.ftp.FtpSink")
        .settings(fakeFtpSettings)
        .workerClusterKey(workerClusterInfo.key)
        .create()
    ).settings(WithDefinitions.VERSION_KEY).asInstanceOf[JsString].value shouldBe VersionUtils.VERSION
  }

  @Test
  def testDefaultRevision(): Unit = {
    val topic = result(topicApi.request.brokerClusterKey(brokerClusterInfo.key).create())
    result(
      connectorApi.request
        .topicKey(topic.key)
        .className("oharastream.ohara.connector.ftp.FtpSink")
        .settings(fakeFtpSettings)
        .workerClusterKey(workerClusterInfo.key)
        .create()
    ).settings(WithDefinitions.REVISION_KEY).asInstanceOf[JsString].value shouldBe VersionUtils.REVISION
  }

  @Test
  def testDurationString(): Unit = {
    val topic = result(topicApi.request.brokerClusterKey(brokerClusterInfo.key).create())
    result(
      connectorApi.request
        .topicKey(topic.key)
        .className("oharastream.ohara.connector.perf.PerfSource")
        .workerClusterKey(workerClusterInfo.key)
        .create()
    ).settings(PerfSourceProps.PERF_FREQUENCY_KEY)
      .asInstanceOf[JsString]
      // the time conversion is based on milliseconds
      .value shouldBe Duration(PerfSourceProps.PERF_FREQUENCY_DEFAULT.toMillis, TimeUnit.MILLISECONDS)
      .toString()
  }

  @AfterEach
  def tearDown(): Unit = Releasable.close(configurator)
}
