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

import java.io.FileOutputStream
import java.util.concurrent.TimeUnit

import oharastream.ohara.client.configurator.InspectApi.{RdbColumn, RdbInfo}
import oharastream.ohara.client.configurator.{
  BrokerApi,
  InspectApi,
  ShabondiApi,
  StreamApi,
  TopicApi,
  WorkerApi,
  ZookeeperApi
}
import oharastream.ohara.client.database.DatabaseClient
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.setting.{ClassType, ObjectKey, WithDefinitions}
import oharastream.ohara.common.util.{CommonUtils, Releasable, VersionUtils}
import oharastream.ohara.configurator.Configurator.Mode
import oharastream.ohara.configurator.{Configurator, ReflectionUtils}
import oharastream.ohara.testing.service.Database
import org.junit.jupiter.api.{AfterEach, Test}
import org.scalatest.matchers.should.Matchers._
import spray.json.{JsArray, JsBoolean, JsNull, JsNumber, JsObject, JsString}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class TestInspectRoute extends OharaTest {
  private[this] val db           = Database.local()
  private[this] val configurator = Configurator.builder.fake().build()

  private[this] def result[T](f: Future[T]): T = Await.result(f, Duration(30, TimeUnit.SECONDS))

  private[this] val zookeeperClusterInfo = result(
    ZookeeperApi.access.hostname(configurator.hostname).port(configurator.port).list()
  ).head

  private[this] val brokerClusterInfo = result(
    BrokerApi.access.hostname(configurator.hostname).port(configurator.port).list()
  ).head

  private[this] val workerClusterInfo = result(
    WorkerApi.access.hostname(configurator.hostname).port(configurator.port).list()
  ).head

  private[this] def inspectApi = InspectApi.access.hostname(configurator.hostname).port(configurator.port)

  @Test
  def testQueryIncorrectPassword(): Unit = an[IllegalArgumentException] should be thrownBy result(
    inspectApi.rdbRequest
      .jdbcUrl(db.url())
      .user(db.user())
      .password(CommonUtils.randomString(10))
      .catalogPattern(db.databaseName)
      .workerClusterKey(workerClusterInfo.key)
      .query()
  )

  @Test
  def testQueryNonexistentTable(): Unit =
    result(
      inspectApi.rdbRequest
        .jdbcUrl(db.url())
        .user(db.user())
        .password(db.password())
        .catalogPattern(db.databaseName)
        .tableName(CommonUtils.randomString(10))
        .workerClusterKey(workerClusterInfo.key)
        .query()
    ).tables shouldBe Seq.empty

  private[this] def createTable(): (DatabaseClient, String, Seq[String], Seq[String]) = {
    val client    = DatabaseClient.builder.url(db.url()).user(db.user()).password(db.password()).build
    val tableName = CommonUtils.randomString(10)
    val cfNames   = Seq(CommonUtils.randomString(5), CommonUtils.randomString(5))
    val pkNames   = Seq(CommonUtils.randomString(5), CommonUtils.randomString(5))
    client.createTable(
      tableName,
      pkNames.map(cfName => RdbColumn(cfName, "INTEGER", true)) ++ cfNames.map(
        cfName => RdbColumn(cfName, "INTEGER", false)
      )
    )
    (client, tableName, pkNames, cfNames)
  }

  private[this] def verify(info: RdbInfo, tableName: String, pkNames: Seq[String], cfNames: Seq[String]): Unit = {
    info.tables.count(_.name == tableName) shouldBe 1
    val table = info.tables.filter(_.name == tableName).head
    table.columns.size shouldBe (pkNames.size + cfNames.size)
    pkNames.foreach { cfName =>
      table.columns.map(_.name) should contain(cfName)
      table.columns.find(_.name == cfName).get.pk shouldBe true
    }
    cfNames.foreach { cfName =>
      table.columns.map(_.name) should contain(cfName)
      table.columns.find(_.name == cfName).get.pk shouldBe false
    }
  }

  @Test
  def testQueryTable(): Unit = {
    val (client, tableName, pkNames, cfNames) = createTable()
    try verify(
      info = result(
        inspectApi.rdbRequest
          .jdbcUrl(db.url())
          .user(db.user())
          .password(db.password())
          .catalogPattern(db.databaseName)
          .tableName(tableName)
          .workerClusterKey(workerClusterInfo.key)
          .query()
      ),
      tableName = tableName,
      pkNames = pkNames,
      cfNames = cfNames
    )
    finally {
      client.dropTable(tableName)
      client.close()
    }
  }

  @Test
  def testQueryAllTables(): Unit = {
    val (client, tableName, pkNames, cfNames) = createTable()
    try verify(
      info = result(
        inspectApi.rdbRequest
          .jdbcUrl(db.url())
          .user(db.user())
          .password(db.password())
          .workerClusterKey(workerClusterInfo.key)
          .query()
      ),
      tableName = tableName,
      pkNames = pkNames,
      cfNames = cfNames
    )
    finally {
      client.dropTable(tableName)
      client.close()
    }
  }

  @Test
  def testQueryStreamFile(): Unit = {
    val streamFile = RouteUtils.streamFile
    streamFile.exists() shouldBe true
    val fileInfo = result(inspectApi.fileRequest.file(streamFile).query())

    fileInfo.classInfos should not be Seq.empty
    fileInfo.sourceClassInfos.size shouldBe 0
    fileInfo.sinkClassInfos.size shouldBe 0
    fileInfo.streamClassInfos.size shouldBe 1
    fileInfo.url shouldBe Option.empty
  }

  @Test
  def testQueryConnectorFile(): Unit = {
    val connectorFile = RouteUtils.connectorFile
    connectorFile.exists() shouldBe true
    val fileInfo = result(inspectApi.fileRequest.file(connectorFile).query())

    fileInfo.classInfos should not be Seq.empty
    fileInfo.sourceClassInfos.size should not be 0
    fileInfo.sourceClassInfos.foreach(d => d.settingDefinitions should not be Seq.empty)
    fileInfo.sinkClassInfos.size should not be 0
    fileInfo.sinkClassInfos.foreach(d => d.settingDefinitions should not be Seq.empty)
    fileInfo.streamClassInfos.size shouldBe 0
    fileInfo.url shouldBe Option.empty
  }

  @Test
  def testQueryIllegalFile(): Unit = {
    val file = {
      val f      = CommonUtils.createTempFile(CommonUtils.randomString(10), ".jar")
      val output = new FileOutputStream(f)
      try output.write("asdasdsad".getBytes())
      finally output.close()
      f
    }
    val fileInfo = result(inspectApi.fileRequest.file(file).query())

    fileInfo.classInfos shouldBe Seq.empty
  }

  @Test
  def testConfiguratorInfo(): Unit = {
    // only test the configurator based on mini cluster
    val clusterInformation = result(inspectApi.configuratorInfo())
    clusterInformation.versionInfo.version shouldBe VersionUtils.VERSION
    clusterInformation.versionInfo.branch shouldBe VersionUtils.BRANCH
    clusterInformation.versionInfo.user shouldBe VersionUtils.USER
    clusterInformation.versionInfo.revision shouldBe VersionUtils.REVISION
    clusterInformation.versionInfo.date shouldBe VersionUtils.DATE
    clusterInformation.mode shouldBe Mode.FAKE.toString
  }

  @Test
  def testZookeeperInfo(): Unit = {
    val info = result(inspectApi.zookeeperInfo())
    info.imageName shouldBe ZookeeperApi.IMAGE_NAME_DEFAULT
    info.settingDefinitions.size shouldBe ZookeeperApi.DEFINITIONS.size
    info.settingDefinitions.foreach { definition =>
      definition shouldBe ZookeeperApi.DEFINITIONS.find(_.key() == definition.key()).get
    }
    info.classInfos shouldBe Seq.empty
  }

  @Test
  def testZookeeperInfoWithKey(): Unit =
    result(inspectApi.zookeeperInfo()) shouldBe result(inspectApi.zookeeperInfo(zookeeperClusterInfo.key))

  @Test
  def testBrokerInfo(): Unit = {
    val info = result(inspectApi.brokerInfo())
    info.imageName shouldBe BrokerApi.IMAGE_NAME_DEFAULT
    info.settingDefinitions.size shouldBe BrokerApi.DEFINITIONS.size
    info.settingDefinitions.foreach { definition =>
      definition shouldBe BrokerApi.DEFINITIONS.find(_.key() == definition.key()).get
    }
    info.classInfos.size shouldBe 1
    info.classInfos.head.classType shouldBe ClassType.TOPIC
    info.classInfos.head.settingDefinitions.size shouldBe TopicApi.DEFINITIONS.size
  }

  @Test
  def testBrokerInfoWithKey(): Unit =
    result(inspectApi.brokerInfo()) shouldBe result(inspectApi.brokerInfo(brokerClusterInfo.key))

  @Test
  def testWorkerInfo(): Unit = {
    val info = result(inspectApi.workerInfo())
    info.imageName shouldBe WorkerApi.IMAGE_NAME_DEFAULT
    info.settingDefinitions.size shouldBe WorkerApi.DEFINITIONS.size
    info.settingDefinitions.foreach { definition =>
      definition shouldBe WorkerApi.DEFINITIONS.find(_.key() == definition.key()).get
    }
    info.classInfos shouldBe Seq.empty
  }

  @Test
  def testWorkerInfoWithKey(): Unit = {
    val info = result(inspectApi.workerInfo(workerClusterInfo.key))
    info.imageName shouldBe WorkerApi.IMAGE_NAME_DEFAULT
    info.settingDefinitions.size shouldBe WorkerApi.DEFINITIONS.size
    info.settingDefinitions.foreach { definition =>
      definition shouldBe WorkerApi.DEFINITIONS.find(_.key() == definition.key()).get
    }
    info.classInfos.size shouldBe ReflectionUtils.localConnectorDefinitions.size
  }

  @Test
  def testStreamInfo(): Unit = {
    val info = result(inspectApi.streamInfo())
    info.imageName shouldBe StreamApi.IMAGE_NAME_DEFAULT
    // the jar is empty but we still see the default definitions
    info.settingDefinitions should not be Seq.empty
    info.classInfos shouldBe Seq.empty
  }

  @Test
  def testStreamInfoWithKey(): Unit = {
    val info = result(inspectApi.streamInfo(ObjectKey.of("g", "n")))
    info.imageName shouldBe StreamApi.IMAGE_NAME_DEFAULT
    // the jar is empty but we still see the default definitions
    info.settingDefinitions should not be Seq.empty
    info.classInfos shouldBe Seq.empty
  }

  @Test
  def testShabondiInfo(): Unit = {
    val info = result(inspectApi.shabondiInfo())
    info.imageName shouldBe ShabondiApi.IMAGE_NAME_DEFAULT
    info.settingDefinitions should not be Seq.empty
    info.classInfos(0).classType shouldBe ClassType.SOURCE
    info.classInfos(0).className shouldBe ShabondiApi.SHABONDI_SOURCE_CLASS_NAME
    info.classInfos(0).settingDefinitions should not be Seq.empty
    info
      .classInfos(0)
      .settingDefinitions
      .find(_.key() == WithDefinitions.KIND_KEY)
      .get
      .defaultString() shouldBe ClassType.SOURCE.key()

    info.classInfos(1).classType shouldBe ClassType.SINK
    info.classInfos(1).className shouldBe ShabondiApi.SHABONDI_SINK_CLASS_NAME
    info.classInfos(1).settingDefinitions should not be Seq.empty
    info
      .classInfos(1)
      .settingDefinitions
      .find(_.key() == WithDefinitions.KIND_KEY)
      .get
      .defaultString() shouldBe ClassType.SINK.key()
  }

  @Test
  def testInspectFakeTopicData(): Unit = {
    val topicApi  = TopicApi.access.hostname(configurator.hostname).port(configurator.port)
    val topicInfo = result(topicApi.request.brokerClusterKey(brokerClusterInfo.key).create())
    result(topicApi.start(topicInfo.key))
    val data = result(
      inspectApi.topicRequest
        .key(topicInfo.key)
        .query()
    )
    data.messages.size shouldBe 1
    data.messages.head.value should not be None
    val fields = data.messages.head.value.get.asJsObject.fields
    fields.size should not be 0
    var typeCount = 0
    fields.values.foreach {
      case _: JsString  => typeCount += 1
      case _: JsNumber  => typeCount += 1
      case _: JsBoolean => typeCount += 1
      case _: JsArray   => typeCount += 1
      case _: JsObject  => typeCount += 1
      case JsNull       => throw new RuntimeException("JsNull is illegal")
    }
    typeCount shouldBe 5
  }

  @AfterEach
  def tearDown(): Unit = {
    Releasable.close(configurator)
    Releasable.close(db)
  }
}
