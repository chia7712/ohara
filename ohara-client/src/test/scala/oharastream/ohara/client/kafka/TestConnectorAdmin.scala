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

package oharastream.ohara.client.kafka

import java.util.concurrent.TimeUnit

import oharastream.ohara.client.configurator.ConnectorApi.State
import oharastream.ohara.common.data.Serializer
import oharastream.ohara.common.setting.{ConnectorKey, SettingDef, TopicKey, WithDefinitions}
import oharastream.ohara.common.util.{CommonUtils, VersionUtils}
import oharastream.ohara.kafka.Consumer
import oharastream.ohara.kafka.connector.json.{ConnectorDefUtils, ConverterType, StringList}
import oharastream.ohara.testing.With3Brokers3Workers
import org.junit.jupiter.api.Test
import org.scalatest.matchers.should.Matchers._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._
class TestConnectorAdmin extends With3Brokers3Workers {
  private[this] val connectorAdmin = ConnectorAdmin(testUtil().workersConnProps())

  private[this] def result[T](f: Future[T]): T = Await.result(f, Duration(60, TimeUnit.SECONDS))

  private[this] def assertExist(connectorAdmin: ConnectorAdmin, connectorKey: ConnectorKey): Boolean =
    CommonUtils.await(() => result(connectorAdmin.exist(connectorKey)) == true, java.time.Duration.ofSeconds(30))

  private[this] def await(f: () => Boolean): Unit = CommonUtils.await(() => f(), java.time.Duration.ofSeconds(300))

  @Test
  def testExist(): Unit = {
    val topicKey     = TopicKey.of(CommonUtils.randomString(10), CommonUtils.randomString(10))
    val connectorKey = ConnectorKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))
    result(connectorAdmin.exist(connectorKey)) shouldBe false

    result(
      connectorAdmin
        .connectorCreator()
        .topicKey(topicKey)
        .connectorClass(classOf[MyConnector])
        .connectorKey(connectorKey)
        .numberOfTasks(1)
        .create()
    )

    try assertExist(connectorAdmin, connectorKey)
    finally result(connectorAdmin.delete(connectorKey))
  }

  @Test
  def testExistOnUnrunnableConnector(): Unit = {
    val topicKey     = TopicKey.of(CommonUtils.randomString(10), CommonUtils.randomString(10))
    val connectorKey = ConnectorKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))
    result(connectorAdmin.exist(connectorKey)) shouldBe false

    result(
      connectorAdmin
        .connectorCreator()
        .topicKey(topicKey)
        .connectorClass(classOf[BrokenConnector])
        .connectorKey(connectorKey)
        .numberOfTasks(1)
        .create()
    )

    try assertExist(connectorAdmin, connectorKey)
    finally result(connectorAdmin.delete(connectorKey))
  }

  @Test
  def testPauseAndResumeSource(): Unit = {
    val topicKey     = TopicKey.of(CommonUtils.randomString(10), CommonUtils.randomString(10))
    val connectorKey = ConnectorKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))
    result(
      connectorAdmin
        .connectorCreator()
        .topicKey(topicKey)
        .connectorClass(classOf[MyConnector])
        .connectorKey(connectorKey)
        .numberOfTasks(1)
        .create()
    )
    try {
      assertExist(connectorAdmin, connectorKey)
      val consumer =
        Consumer
          .builder()
          .topicKey(topicKey)
          .offsetFromBegin()
          .connectionProps(testUtil.brokersConnProps)
          .keySerializer(Serializer.ROW)
          .valueSerializer(Serializer.BYTES)
          .build()
      try {
        // try to receive some data from topic
        var rows = consumer.poll(java.time.Duration.ofSeconds(10), 1)
        rows.size should not be 0
        rows.asScala.foreach(_.key.get shouldBe MyConnectorTask.ROW)
        // pause connector
        result(connectorAdmin.pause(connectorKey))

        await(() => result(connectorAdmin.status(connectorKey)).connector.state == State.PAUSED.name)

        // try to receive all data from topic...10 seconds should be enough in this case
        rows = consumer.poll(java.time.Duration.ofSeconds(10), Int.MaxValue)
        rows.asScala.foreach(_.key.get shouldBe MyConnectorTask.ROW)

        // connector is paused so there is no data
        rows = consumer.poll(java.time.Duration.ofSeconds(20), 1)
        rows.size shouldBe 0

        // resume connector
        result(connectorAdmin.resume(connectorKey))

        await(() => result(connectorAdmin.status(connectorKey)).connector.state == State.RUNNING.name)

        // since connector is resumed so some data are generated
        rows = consumer.poll(java.time.Duration.ofSeconds(20), 1)
        rows.size should not be 0
      } finally consumer.close()
    } finally result(connectorAdmin.delete(connectorKey))
  }

  @Test
  def testValidate(): Unit = {
    val name          = CommonUtils.randomString(10)
    val topicName     = CommonUtils.randomString(10)
    val numberOfTasks = 1
    val settingInfo = result(
      connectorAdmin
        .connectorValidator()
        .className(classOf[MyConnector].getName)
        .settings(
          Map(
            ConnectorDefUtils.CONNECTOR_NAME_DEFINITION.key() -> name,
            ConnectorDefUtils.TOPIC_NAMES_DEFINITION.key() -> StringList
              .toJsonString(java.util.List.of(topicName)),
            ConnectorDefUtils.NUMBER_OF_TASKS_DEFINITION.key() -> numberOfTasks.toString
          )
        )
        .run()
    )
    settingInfo.className.get shouldBe classOf[MyConnector].getName
    settingInfo.settings.size should not be 0
    settingInfo.topicNamesOnKafka.asScala shouldBe Seq(topicName)
    settingInfo.numberOfTasks.get shouldBe numberOfTasks
  }

  @Test
  def ignoreTopicNames(): Unit =
    an[NoSuchElementException] should be thrownBy result(
      connectorAdmin.connectorValidator().className(classOf[MyConnector].getName).run()
    )

  @Test
  def ignoreClassName(): Unit =
    an[NoSuchElementException] should be thrownBy result(connectorAdmin.connectorValidator().run())

  @Test
  def testValidateWithoutValue(): Unit = {
    val topicKey = TopicKey.of(CommonUtils.randomString(10), CommonUtils.randomString(10))
    val settingInfo = result(
      connectorAdmin.connectorValidator().className(classOf[MyConnector].getName).topicKey(topicKey).run()
    )
    settingInfo.className.get shouldBe classOf[MyConnector].getName
    settingInfo.settings.size should not be 0
    settingInfo.topicNamesOnKafka.isEmpty shouldBe false
    settingInfo.numberOfTasks.isPresent shouldBe true
  }

  @Test
  def testColumnsDefinition(): Unit =
    result(connectorAdmin.connectorDefinitions())
      .map(_._2.settingDefinitions.filter(_.key() == ConnectorDefUtils.COLUMNS_DEFINITION.key()).head)
      .foreach { definition =>
        definition.tableKeys().size() should not be 0
      }

  @Test
  def testAllPluginDefinitions(): Unit = {
    val plugins = result(connectorAdmin.connectorDefinitions())
    plugins.size should not be 0
    plugins.foreach(plugin => check(plugin._2.settingDefinitions))
  }

  @Test
  def testListDefinitions(): Unit = {
    check(result(connectorAdmin.connectorDefinition(classOf[MyConnector].getName)).settingDefinitions)
  }

  private[this] def check(settingDefinitionS: Seq[SettingDef]): Unit = {
    settingDefinitionS.size should not be 0

    settingDefinitionS.exists(_.key() == ConnectorDefUtils.CONNECTOR_CLASS_DEFINITION.key()) shouldBe true
    settingDefinitionS
      .find(_.key() == ConnectorDefUtils.CONNECTOR_CLASS_DEFINITION.key())
      .head
      .group() shouldBe ConnectorDefUtils.CORE_GROUP
    settingDefinitionS
      .find(_.key() == ConnectorDefUtils.CONNECTOR_CLASS_DEFINITION.key())
      .head
      .internal() shouldBe false
    settingDefinitionS
      .find(_.key() == ConnectorDefUtils.CONNECTOR_CLASS_DEFINITION.key())
      .head
      .permission() shouldBe SettingDef.Permission.CREATE_ONLY
    settingDefinitionS
      .find(_.key() == ConnectorDefUtils.CONNECTOR_CLASS_DEFINITION.key())
      .head
      .hasDefault shouldBe false

    settingDefinitionS.exists(_.key() == ConnectorDefUtils.COLUMNS_DEFINITION.key()) shouldBe true
    settingDefinitionS
      .find(_.key() == ConnectorDefUtils.COLUMNS_DEFINITION.key())
      .head
      .group() shouldBe ConnectorDefUtils.CORE_GROUP
    settingDefinitionS.find(_.key() == ConnectorDefUtils.COLUMNS_DEFINITION.key()).head.internal() shouldBe false
    settingDefinitionS
      .find(_.key() == ConnectorDefUtils.COLUMNS_DEFINITION.key())
      .head
      .permission() shouldBe SettingDef.Permission.EDITABLE
    settingDefinitionS.find(_.key() == ConnectorDefUtils.COLUMNS_DEFINITION.key()).head.hasDefault shouldBe false

    settingDefinitionS.exists(_.key() == ConnectorDefUtils.NUMBER_OF_TASKS_DEFINITION.key()) shouldBe true
    settingDefinitionS
      .find(_.key() == ConnectorDefUtils.NUMBER_OF_TASKS_DEFINITION.key())
      .head
      .group() shouldBe ConnectorDefUtils.CORE_GROUP
    settingDefinitionS
      .find(_.key() == ConnectorDefUtils.NUMBER_OF_TASKS_DEFINITION.key())
      .head
      .internal() shouldBe false
    settingDefinitionS
      .find(_.key() == ConnectorDefUtils.NUMBER_OF_TASKS_DEFINITION.key())
      .head
      .permission() shouldBe SettingDef.Permission.EDITABLE
    settingDefinitionS.find(_.key() == ConnectorDefUtils.NUMBER_OF_TASKS_DEFINITION.key()).head.defaultInt() shouldBe 1

    settingDefinitionS.exists(_.key() == ConnectorDefUtils.TOPIC_NAMES_DEFINITION.key()) shouldBe true
    settingDefinitionS
      .find(_.key() == ConnectorDefUtils.TOPIC_NAMES_DEFINITION.key())
      .head
      .group() shouldBe ConnectorDefUtils.CORE_GROUP
    settingDefinitionS.find(_.key() == ConnectorDefUtils.TOPIC_NAMES_DEFINITION.key()).head.internal() shouldBe true
    settingDefinitionS
      .find(_.key() == ConnectorDefUtils.TOPIC_NAMES_DEFINITION.key())
      .head
      .permission() shouldBe SettingDef.Permission.EDITABLE
    settingDefinitionS.find(_.key() == ConnectorDefUtils.TOPIC_NAMES_DEFINITION.key()).head.hasDefault shouldBe false

    settingDefinitionS.exists(_.key() == ConnectorDefUtils.WORKER_CLUSTER_KEY_DEFINITION.key()) shouldBe true
    settingDefinitionS
      .find(_.key() == ConnectorDefUtils.WORKER_CLUSTER_KEY_DEFINITION.key())
      .head
      .group() shouldBe ConnectorDefUtils.CORE_GROUP
    settingDefinitionS
      .find(_.key() == ConnectorDefUtils.WORKER_CLUSTER_KEY_DEFINITION.key())
      .head
      .internal() shouldBe false
    settingDefinitionS
      .find(_.key() == ConnectorDefUtils.WORKER_CLUSTER_KEY_DEFINITION.key())
      .head
      .permission() shouldBe SettingDef.Permission.EDITABLE
    settingDefinitionS
      .find(_.key() == ConnectorDefUtils.WORKER_CLUSTER_KEY_DEFINITION.key())
      .head
      .hasDefault shouldBe false

    settingDefinitionS.exists(_.key() == WithDefinitions.AUTHOR_KEY) shouldBe true
    settingDefinitionS
      .find(_.key() == WithDefinitions.AUTHOR_KEY)
      .head
      .group() should not be ConnectorDefUtils.CORE_GROUP
    settingDefinitionS.find(_.key() == WithDefinitions.AUTHOR_KEY).head.internal() shouldBe false
    settingDefinitionS
      .find(_.key() == WithDefinitions.AUTHOR_KEY)
      .head
      .permission() shouldBe SettingDef.Permission.READ_ONLY
    settingDefinitionS.find(_.key() == WithDefinitions.AUTHOR_KEY).head.defaultString shouldBe VersionUtils.USER

    settingDefinitionS.exists(_.key() == WithDefinitions.VERSION_KEY) shouldBe true
    settingDefinitionS
      .find(_.key() == WithDefinitions.VERSION_KEY)
      .head
      .group() should not be ConnectorDefUtils.CORE_GROUP
    settingDefinitionS.find(_.key() == WithDefinitions.VERSION_KEY).head.internal() shouldBe false
    settingDefinitionS
      .find(_.key() == WithDefinitions.VERSION_KEY)
      .head
      .permission() shouldBe SettingDef.Permission.READ_ONLY
    settingDefinitionS
      .find(_.key() == WithDefinitions.VERSION_KEY)
      .head
      .defaultString shouldBe VersionUtils.VERSION

    settingDefinitionS.exists(_.key() == WithDefinitions.REVISION_KEY) shouldBe true
    settingDefinitionS
      .find(_.key() == WithDefinitions.REVISION_KEY)
      .head
      .group() shouldBe "meta"
    settingDefinitionS.find(_.key() == WithDefinitions.REVISION_KEY).head.internal() shouldBe false
    settingDefinitionS.find(_.key() == WithDefinitions.REVISION_KEY).head.defaultString shouldBe VersionUtils.REVISION

    settingDefinitionS.exists(_.key() == WithDefinitions.KIND_KEY) shouldBe true
    settingDefinitionS.find(_.key() == WithDefinitions.KIND_KEY).head.group() shouldBe WithDefinitions.META_GROUP
    settingDefinitionS.find(_.key() == WithDefinitions.KIND_KEY).head.internal() shouldBe false
    (settingDefinitionS.find(_.key() == WithDefinitions.KIND_KEY).head.defaultString == "source"
    || settingDefinitionS.find(_.key() == WithDefinitions.KIND_KEY).head.defaultString == "sink") shouldBe true

    settingDefinitionS.exists(_.key() == ConnectorDefUtils.KEY_CONVERTER_DEFINITION.key()) shouldBe true
    settingDefinitionS
      .find(_.key() == ConnectorDefUtils.KEY_CONVERTER_DEFINITION.key())
      .head
      .group() shouldBe ConnectorDefUtils.CORE_GROUP
    settingDefinitionS.find(_.key() == ConnectorDefUtils.KEY_CONVERTER_DEFINITION.key()).head.internal() shouldBe true
    settingDefinitionS
      .find(_.key() == ConnectorDefUtils.KEY_CONVERTER_DEFINITION.key())
      .head
      .permission() shouldBe SettingDef.Permission.EDITABLE
    settingDefinitionS
      .find(_.key() == ConnectorDefUtils.KEY_CONVERTER_DEFINITION.key())
      .head
      .defaultString shouldBe ConverterType.NONE.clz.getName

    settingDefinitionS.exists(_.key() == ConnectorDefUtils.VALUE_CONVERTER_DEFINITION.key()) shouldBe true
    settingDefinitionS
      .find(_.key() == ConnectorDefUtils.VALUE_CONVERTER_DEFINITION.key())
      .head
      .group() shouldBe ConnectorDefUtils.CORE_GROUP
    settingDefinitionS.find(_.key() == ConnectorDefUtils.VALUE_CONVERTER_DEFINITION.key()).head.internal() shouldBe true
    settingDefinitionS
      .find(_.key() == ConnectorDefUtils.VALUE_CONVERTER_DEFINITION.key())
      .head
      .permission() shouldBe SettingDef.Permission.EDITABLE
    settingDefinitionS
      .find(_.key() == ConnectorDefUtils.VALUE_CONVERTER_DEFINITION.key())
      .head
      .defaultString shouldBe ConverterType.NONE.clz.getName
  }

  @Test
  def passIncorrectColumns(): Unit = {
    val topicKey = TopicKey.of(CommonUtils.randomString(10), CommonUtils.randomString(10))
    val e = intercept[IllegalArgumentException] {
      result(
        connectorAdmin
          .connectorCreator()
          .topicKey(topicKey)
          .connectorClass(classOf[MyConnector])
          .connectorKey(ConnectorKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5)))
          .numberOfTasks(1)
          .settings(Map(ConnectorDefUtils.COLUMNS_DEFINITION.key() -> "Asdasdasd"))
          .create()
      )
    }
    //see SettingDef.checker
    e.getMessage.contains("can't be converted to PropGroup type") shouldBe true
  }

  @Test
  def passIncorrectDuration(): Unit = {
    val topicKey = TopicKey.of(CommonUtils.randomString(10), CommonUtils.randomString(10))
    val e = intercept[IllegalArgumentException] {
      result(
        connectorAdmin
          .connectorCreator()
          .topicKey(topicKey)
          .connectorClass(classOf[MyConnector])
          .connectorKey(ConnectorKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5)))
          .numberOfTasks(1)
          .settings(Map(MyConnector.DURATION_KEY -> "Asdasdasd"))
          .create()
      )
    }
    //see ConnectorDefinitions.validator
    e.getMessage.contains("can't be converted to Duration type") shouldBe true
  }

  @Test
  def pass1Second(): Unit = {
    val topicKey = TopicKey.of(CommonUtils.randomString(10), CommonUtils.randomString(10))
    result(
      connectorAdmin
        .connectorCreator()
        .topicKey(topicKey)
        .connectorClass(classOf[MyConnector])
        .connectorKey(ConnectorKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5)))
        .numberOfTasks(1)
        .settings(Map(MyConnector.DURATION_KEY -> "PT1S"))
        .create()
    )
  }
  @Test
  def pass1Minute1Second(): Unit = {
    val topicKey = TopicKey.of(CommonUtils.randomString(10), CommonUtils.randomString(10))
    result(
      connectorAdmin
        .connectorCreator()
        .topicKey(topicKey)
        .connectorClass(classOf[MyConnector])
        .connectorKey(ConnectorKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5)))
        .numberOfTasks(1)
        .settings(Map(MyConnector.DURATION_KEY -> "PT1M1S"))
        .create()
    )
  }

  @Test
  def nullConnectionProps(): Unit =
    an[NullPointerException] should be thrownBy ConnectorAdmin.builder.connectionProps(null)

  @Test
  def emptyConnectionProps(): Unit =
    an[IllegalArgumentException] should be thrownBy ConnectorAdmin.builder.connectionProps("")

  @Test
  def nullRetryLimit(): Unit = {
    an[IllegalArgumentException] should be thrownBy ConnectorAdmin.builder.retryLimit(0)
    an[IllegalArgumentException] should be thrownBy ConnectorAdmin.builder.retryLimit(-1)
  }

  @Test
  def nullRetryInterval(): Unit = an[NullPointerException] should be thrownBy ConnectorAdmin.builder.retryInternal(null)
}
