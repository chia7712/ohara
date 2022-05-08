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

package oharastream.ohara.client.configurator

import oharastream.ohara.client.configurator.ConnectorApi.State._
import oharastream.ohara.client.configurator.ConnectorApi._
import oharastream.ohara.common.data.{Column, DataType}
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.setting.SettingDef.Permission
import oharastream.ohara.common.setting.{ObjectKey, PropGroup, SettingDef, TopicKey}
import oharastream.ohara.common.util.CommonUtils
import oharastream.ohara.kafka.connector.json.ConnectorDefUtils
import org.junit.jupiter.api.Test
import org.scalatest.matchers.should.Matchers._
import spray.json.DefaultJsonProtocol._
import spray.json.{JsString, _}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.jdk.CollectionConverters._
import scala.util.Random
class TestConnectorApi extends OharaTest {
  @Test
  def nullKeyInGet(): Unit =
    an[NullPointerException] should be thrownBy ConnectorApi.access.get(null)

  @Test
  def nullKeyInDelete(): Unit =
    an[NullPointerException] should be thrownBy ConnectorApi.access.delete(null)

  @Test
  def testParseCreation(): Unit = {
    val workerClusterName = CommonUtils.randomString()
    val className         = CommonUtils.randomString()
    val topicKeys         = Set(TopicKey.of(CommonUtils.randomString(), CommonUtils.randomString()))
    val numberOfTasks     = 10
    val tags              = Map("a" -> JsString("b"), "b" -> JsNumber(1))
    val anotherKey        = CommonUtils.randomString()
    val anotherValue      = CommonUtils.randomString()

    val creation = CREATION_FORMAT.read(s"""
       |{
       |  "workerClusterKey": ${JsString(workerClusterName).toString()},
       |  "connector.class": ${JsString(className).toString()},
       |  "numberOfTasks": ${JsNumber(numberOfTasks).toString()},
       |  "topicKeys": ${TopicKey.toJsonString(topicKeys.asJava)},
       |  "tags": ${JsObject(tags)},
       |  "$anotherKey": "$anotherValue"
       |}
      """.stripMargin.parseJson)

    creation.group shouldBe GROUP_DEFAULT
    creation.name.length shouldBe SettingDef.STRING_LENGTH_LIMIT
    creation.workerClusterKey.name() shouldBe workerClusterName
    creation.className shouldBe className
    creation.columns shouldBe Seq.empty
    creation.topicKeys shouldBe topicKeys
    creation.numberOfTasks shouldBe 1
    creation.tags shouldBe tags
    // this key is deprecated so json converter will replace it by new one
    creation.raw.contains("className") shouldBe false
    creation.raw.contains("aaa") shouldBe false
    creation.raw(anotherKey).convertTo[String] shouldBe anotherValue
    CREATION_FORMAT.read(CREATION_FORMAT.write(creation)).raw shouldBe creation.raw

    val group = CommonUtils.randomString(10)
    val name  = CommonUtils.randomString(10)
    val column = Column
      .builder()
      .name(CommonUtils.randomString(10))
      .newName(CommonUtils.randomString(10))
      .dataType(DataType.DOUBLE)
      .build()
    val creation2 = CREATION_FORMAT.read(s"""
       |{
       |  "group": "$group",
       |  "name": ${JsString(name).toString()},
       |  "workerClusterKey": ${JsString(workerClusterName).toString()},
       |  "connector.class": ${JsString(className).toString()},
       |  "$COLUMNS_KEY": ${PropGroup.ofColumn(column).toJsonString},
       |  "topicKeys": ${TopicKey.toJsonString(topicKeys.asJava)},
       |  "numberOfTasks": ${JsNumber(numberOfTasks).toString()},
       |  "$anotherKey": "$anotherValue"
       |}""".stripMargin.parseJson)
    creation2.group shouldBe group
    creation2.name shouldBe name
    creation2.workerClusterKey.name() shouldBe workerClusterName
    creation2.className shouldBe className
    creation2.columns shouldBe Seq(column)
    creation.topicKeys shouldBe topicKeys
    creation2.numberOfTasks shouldBe 1
    // this key is deprecated so json converter will replace it by new one
    creation2.raw.contains("className") shouldBe false
    creation2.raw.contains("aaa") shouldBe false
    creation2.raw(anotherKey).convertTo[String] shouldBe anotherValue
    CREATION_FORMAT.read(CREATION_FORMAT.write(creation2)).raw shouldBe creation2.raw
  }

  @Test
  def testState(): Unit = {
    State.all shouldBe Seq(
      UNASSIGNED,
      RUNNING,
      PAUSED,
      FAILED,
      DESTROYED
    ).sortBy(_.name)
  }

  @Test
  def testStateJson(): Unit = {
    State.all.foreach(
      state => ConnectorApi.CONNECTOR_STATE_FORMAT.read(ConnectorApi.CONNECTOR_STATE_FORMAT.write(state)) shouldBe state
    )
  }

  @Test
  def renderJsonWithoutAnyRequiredFields(): Unit = {
    val response = ConnectorInfo(
      settings = Map(
        CommonUtils.randomString() -> JsString(CommonUtils.randomString()),
        GROUP_KEY                  -> JsString(CommonUtils.randomString()),
        NAME_KEY                   -> JsString(CommonUtils.randomString())
      ),
      state = None,
      aliveNodes = Set.empty,
      error = None,
      tasksStatus = Seq.empty,
      nodeMetrics = Map.empty,
      lastModified = CommonUtils.current()
    )
    // pass
    ConnectorApi.CONNECTOR_INFO_FORMAT.write(response)
  }

  @Test
  def renderJsonWithConnectorClass(): Unit = {
    val className = CommonUtils.randomString()
    val response = ConnectorInfo(
      settings = access.request
        .className(className)
        .workerClusterKey(ObjectKey.of("g", "m"))
        .topicKey(TopicKey.of("g", "n"))
        .creation
        .raw,
      state = None,
      aliveNodes = Set.empty,
      error = None,
      tasksStatus = Seq.empty,
      nodeMetrics = Map.empty,
      lastModified = CommonUtils.current()
    )
    ConnectorApi.CONNECTOR_INFO_FORMAT
      .write(response)
      .asInstanceOf[JsObject]
      // previous name
      .fields
      .contains("className") shouldBe false
  }

  @Test
  def parsePropGroup(): Unit = {
    val creationRequest = ConnectorApi.CREATION_FORMAT.read(s"""
      |  {
      |    "$WORKER_CLUSTER_KEY_KEY": {
      |      "group": "g",
      |      "name": "n"
      |    },
      |    "topicKeys": [
      |      {
      |        "group": "g",
      |        "name": "n"
      |      }
      |    ],
      |    "$CONNECTOR_CLASS_KEY": "${CommonUtils.randomString()}",
      |    "columns": [
      |      {
      |       "order": 1,
      |       "name": "abc",
      |       "newName": "ccc",
      |       "dataType": "STRING"
      |      }
      |    ]
      |  }
      | """.stripMargin.parseJson)
    val column          = PropGroup.ofJson(creationRequest.raw("columns").toString()).toColumns.get(0)
    column.order() shouldBe 1
    column.name() shouldBe "abc"
    column.newName() shouldBe "ccc"
    column.dataType().name() shouldBe "STRING"
  }

  @Test
  def ignoreClassNameOnCreation(): Unit =
    intercept[DeserializationException] {
      ConnectorApi.access
        .hostname(CommonUtils.randomString(10))
        .port(CommonUtils.availablePort())
        .request
        .workerClusterKey(ObjectKey.of("default", "name"))
        .topicKey(TopicKey.of("g", "n"))
        .creation
    }.getMessage should include(CONNECTOR_CLASS_KEY)

  @Test
  def ignoreNameOnCreation(): Unit =
    ConnectorApi.access
      .hostname(CommonUtils.randomString(10))
      .port(CommonUtils.availablePort())
      .request
      .className(CommonUtils.randomString(10))
      .workerClusterKey(ObjectKey.of("default", "name"))
      .topicKey(TopicKey.of("g", "n"))
      .creation
      .name
      .length should not be 0

  @Test
  def ignoreNameOnUpdate(): Unit =
    an[NoSuchElementException] should be thrownBy ConnectorApi.access
      .hostname(CommonUtils.randomString(10))
      .port(CommonUtils.availablePort())
      .request
      .update()

  @Test
  def emptyGroup(): Unit = an[IllegalArgumentException] should be thrownBy ConnectorApi.access.request.group("")

  @Test
  def nullGroup(): Unit = an[NullPointerException] should be thrownBy ConnectorApi.access.request.group(null)

  @Test
  def emptyName(): Unit = an[IllegalArgumentException] should be thrownBy ConnectorApi.access.request.name("")

  @Test
  def nullName(): Unit = an[NullPointerException] should be thrownBy ConnectorApi.access.request.name(null)

  @Test
  def emptyClassName(): Unit =
    an[IllegalArgumentException] should be thrownBy ConnectorApi.access.request.className("")

  @Test
  def nullClassName(): Unit =
    an[NullPointerException] should be thrownBy ConnectorApi.access.request.className(null)

  @Test
  def emptyColumns(): Unit = ConnectorApi.access.request.columns(Seq.empty)

  @Test
  def nullColumns(): Unit = an[NullPointerException] should be thrownBy ConnectorApi.access.request.columns(null)

  @Test
  def nullWorkerClusterKey(): Unit =
    an[NullPointerException] should be thrownBy ConnectorApi.access.request.workerClusterKey(null)

  @Test
  def emptyTopicKeys(): Unit = ConnectorApi.access.request.topicKeys(Set.empty)

  @Test
  def nullTopicKeys(): Unit =
    an[NullPointerException] should be thrownBy ConnectorApi.access.request.topicKeys(null)

  @Test
  def emptySettings(): Unit = ConnectorApi.access.request.settings(Map.empty)

  @Test
  def nullSettings(): Unit = an[NullPointerException] should be thrownBy ConnectorApi.access.request.settings(null)

  @Test
  def testCreation(): Unit = {
    val name      = CommonUtils.randomString(10)
    val className = CommonUtils.randomString(10)
    val topicKeys = Set(TopicKey.of(CommonUtils.randomString(10), CommonUtils.randomString(10)))
    val map = Map(
      CommonUtils.randomString(10) -> JsString(CommonUtils.randomString(10)),
      CommonUtils.randomString(10) -> JsString(CommonUtils.randomString(10)),
      CommonUtils.randomString(10) -> JsString(CommonUtils.randomString(10))
    )
    val creation =
      ConnectorApi.access.request
        .name(name)
        .className(className)
        .topicKeys(topicKeys)
        .settings(map)
        .workerClusterKey(ObjectKey.of("g", "n"))
        .creation
    creation.name shouldBe name
    creation.className shouldBe className
    creation.topicKeys shouldBe topicKeys
    map.foreach {
      case (k, v) => creation.plain(k) shouldBe v.convertTo[String]
    }
  }

  @Test
  def testDefaultNumberOfTasks(): Unit =
    ConnectorApi.CREATION_FORMAT.read(s"""
      |  {
      |    "$WORKER_CLUSTER_KEY_KEY": {
      |      "group": "g",
      |      "name": "n"
      |    },
      |    "topicKeys": [
      |      {
      |        "group": "g",
      |        "name": "n"
      |      }
      |    ],
      |    "name": "ftpsource",
      |    "$CONNECTOR_CLASS_KEY": "oharastream.ohara.connector.ftp.FtpSource"
      |  }
      |     """.stripMargin.parseJson).numberOfTasks shouldBe 1

  @Test
  def parseColumn(): Unit = {
    val name     = CommonUtils.randomString()
    val newName  = CommonUtils.randomString()
    val dataType = DataType.BOOLEAN
    val order    = 1
    val creation = ConnectorApi.CREATION_FORMAT.read(s"""
                                            |  {
                                            |    "$WORKER_CLUSTER_KEY_KEY": {
                                            |      "group": "g",
                                            |      "name": "n"
                                            |    },
                                            |    "topicKeys": [
                                            |      {
                                            |        "group": "g",
                                            |        "name": "n"
                                            |      }
                                            |    ],
                                            |    "$CONNECTOR_CLASS_KEY": "${CommonUtils.randomString()}",
                                            |    "$COLUMNS_KEY": [
                                            |      {
                                            |        "name": "$name",
                                            |        "newName": "$newName",
                                            |        "dataType": "${dataType.name}",
                                            |        "order": $order
                                            |      }
                                            |    ]
                                            |  }
                                            |""".stripMargin.parseJson)
    creation.columns.size shouldBe 1
    val column = creation.columns.head
    column.name shouldBe name
    column.newName shouldBe newName
    column.dataType shouldBe dataType
    column.order shouldBe order

    val creation2 = ConnectorApi.CREATION_FORMAT.read(s"""
                                                                       |  {
                                                                       |    "$WORKER_CLUSTER_KEY_KEY": {
                                                                       |      "group": "g",
                                                                       |      "name": "n"
                                                                       |    },
                                                                       |    "$CONNECTOR_CLASS_KEY": "oharastream.ohara.connector.ftp.FtpSource",
                                                                       |    "topicKeys": [
                                                                       |      {
                                                                       |        "group": "g",
                                                                       |        "name": "n"
                                                                       |      }
                                                                       |    ],
                                                                       |    "$COLUMNS_KEY": [
                                                                       |      {
                                                                       |        "name": "$name",
                                                                       |        "dataType": "${dataType.name}",
                                                                       |        "order": $order
                                                                       |      }
                                                                       |    ]
                                                                       |  }
                                                                       |""".stripMargin.parseJson)
    creation2.columns.size shouldBe 1
    val column2 = creation2.columns.head
    column2.name shouldBe name
    column2.newName shouldBe name
    column2.dataType shouldBe dataType
    column2.order shouldBe order
  }

  @Test
  def emptyNameForCreatingColumn(): Unit =
    an[DeserializationException] should be thrownBy ConnectorApi.CREATION_FORMAT.read(
      s"""
                                                                                                   |  {
                                                                                                   |    "$WORKER_CLUSTER_KEY_KEY": {
                                                                                                   |      "group": "g",
                                                                                                   |      "name": "n"
                                                                                                   |    },
                                                                                                   |    "$CONNECTOR_CLASS_KEY": "oharastream.ohara.connector.ftp.FtpSource",
                                                                                                   |    "$COLUMNS_KEY": [
                                                                                                   |      {
                                                                                                   |        "name": "",
                                                                                                   |        "dataType": "Boolean",
                                                                                                   |        "order": 1
                                                                                                   |      }
                                                                                                   |    ]
                                                                                                   |  }
                                                                                                        |""".stripMargin.parseJson
    )

  @Test
  def emptyNewNameForCreatingColumn(): Unit =
    an[DeserializationException] should be thrownBy ConnectorApi.CREATION_FORMAT.read(
      s"""
                                                                                                   |  {
                                                                                                   |    "$WORKER_CLUSTER_KEY_KEY": {
                                                                                                   |      "group": "g",
                                                                                                   |      "name": "n"
                                                                                                   |    },
                                                                                                   |    "$CONNECTOR_CLASS_KEY": "oharastream.ohara.connector.ftp.FtpSource",
                                                                                                   |    "$COLUMNS_KEY": [
                                                                                                   |      {
                                                                                                   |        "name": "AA",
                                                                                                   |        "newName": "",
                                                                                                   |        "dataType": "Boolean",
                                                                                                   |        "order": 1
                                                                                                   |      }
                                                                                                   |    ]
                                                                                                   |  }
                                                                                                        |""".stripMargin.parseJson
    )

  @Test
  def negativeOrderForCreatingColumn(): Unit =
    an[DeserializationException] should be thrownBy ConnectorApi.CREATION_FORMAT.read(s"""
                                                        |  {
                                                        |    "$WORKER_CLUSTER_KEY_KEY": {
                                                        |      "group": "g",
                                                        |      "name": "n"
                                                        |    },
                                                        |    "$CONNECTOR_CLASS_KEY": "oharastream.ohara.connector.ftp.FtpSource",
                                                        |    "$COLUMNS_KEY": [
                                                        |      {
                                                        |        "name": "AA",
                                                        |        "newName": "cc",
                                                        |        "dataType": "Boolean",
                                                        |        "order": -1
                                                        |      }
                                                        |    ]
                                                        |  }
                                                        |""".stripMargin.parseJson)

  @Test
  def duplicateOrderForCreatingColumns(): Unit =
    an[DeserializationException] should be thrownBy ConnectorApi.CREATION_FORMAT.read(
      s"""
                                                                                                   |  {
                                                                                                   |    "$WORKER_CLUSTER_KEY_KEY": {
                                                                                                   |      "group": "g",
                                                                                                   |      "name": "n"
                                                                                                   |    },
                                                                                                   |    "$CONNECTOR_CLASS_KEY": "oharastream.ohara.connector.ftp.FtpSource",
                                                                                                   |    "$COLUMNS_KEY": [
                                                                                                   |      {
                                                                                                   |        "name": "AA",
                                                                                                   |        "newName": "",
                                                                                                   |        "dataType": "Boolean",
                                                                                                   |        "order": 1
                                                                                                   |      },
                                                                                                   |      {
                                                                                                   |        "name": "AA",
                                                                                                   |        "newName": "",
                                                                                                   |        "dataType": "Boolean",
                                                                                                   |        "order": 1
                                                                                                   |      }
                                                                                                   |    ]
                                                                                                   |  }
                                                                                                   |""".stripMargin.parseJson
    )

  @Test
  def emptyNameForUpdatingColumn(): Unit =
    an[DeserializationException] should be thrownBy ConnectorApi.UPDATING_FORMAT.read(
      s"""
                                                                                                        |  {
                                                                                                        |    "$WORKER_CLUSTER_KEY_KEY": {
                                                                                                        |      "group": "g",
                                                                                                        |      "name": "n"
                                                                                                        |    },
                                                                                                        |    "$CONNECTOR_CLASS_KEY": "oharastream.ohara.connector.ftp.FtpSource",
                                                                                                        |    "$COLUMNS_KEY": [
                                                                                                        |      {
                                                                                                        |        "name": "",
                                                                                                        |         "dataType": "Boolean",
                                                                                                        |         "order": 1
                                                                                                        |       }
                                                                                                        |    ]
                                                                                                        |  }
                                                                                                        |""".stripMargin.parseJson
    )

  @Test
  def emptyNewNameForUpdatingColumn(): Unit =
    an[DeserializationException] should be thrownBy ConnectorApi.UPDATING_FORMAT.read(
      s"""
                                                                                                   |  {
                                                                                                   |    "$WORKER_CLUSTER_KEY_KEY": {
                                                                                                   |      "group": "g",
                                                                                                   |      "name": "n"
                                                                                                   |    },
                                                                                                   |    "$CONNECTOR_CLASS_KEY": "oharastream.ohara.connector.ftp.FtpSource",
                                                                                                   |    "$COLUMNS_KEY": [
                                                                                                   |      {
                                                                                                   |        "name": "AA",
                                                                                                   |        "newName": "",
                                                                                                   |        "dataType": "Boolean",
                                                                                                   |        "order": 1
                                                                                                   |      }
                                                                                                   |    ]
                                                                                                   |  }
                                                                                                   |""".stripMargin.parseJson
    )

  @Test
  def negativeOrderForUpdatingColumn(): Unit =
    an[DeserializationException] should be thrownBy ConnectorApi.UPDATING_FORMAT.read(
      s"""
                                                                                                 |  {
                                                                                                 |    "$WORKER_CLUSTER_KEY_KEY": {
                                                                                                 |      "group": "g",
                                                                                                 |      "name": "n"
                                                                                                 |    },
                                                                                                 |    "$CONNECTOR_CLASS_KEY": "oharastream.ohara.connector.ftp.FtpSource",
                                                                                                 |    "$COLUMNS_KEY": [
                                                                                                 |      {
                                                                                                 |        "name": "AA",
                                                                                                 |        "newName": "cc",
                                                                                                 |        "dataType": "Boolean",
                                                                                                 |        "order": -1
                                                                                                 |      }
                                                                                                 |    ]
                                                                                                 |  }
                                                                                                 |""".stripMargin.parseJson
    )

  @Test
  def duplicateOrderForUpdatingColumns(): Unit =
    an[DeserializationException] should be thrownBy ConnectorApi.UPDATING_FORMAT.read(
      s"""
                                                                                                   |  {
                                                                                                   |    "$WORKER_CLUSTER_KEY_KEY": {
                                                                                                   |      "group": "g",
                                                                                                   |      "name": "n"
                                                                                                   |    },
                                                                                                   |    "$CONNECTOR_CLASS_KEY": "oharastream.ohara.connector.ftp.FtpSource",
                                                                                                   |    "$COLUMNS_KEY": [
                                                                                                   |      {
                                                                                                   |        "name": "AA",
                                                                                                   |        "newName": "",
                                                                                                   |        "dataType": "Boolean",
                                                                                                   |        "order": 1
                                                                                                   |      },
                                                                                                   |      {
                                                                                                   |        "name": "AA",
                                                                                                   |        "newName": "",
                                                                                                   |        "dataType": "Boolean",
                                                                                                   |        "order": 1
                                                                                                   |      }
                                                                                                   |    ]
                                                                                                   |  }
                                                                                                   |""".stripMargin.parseJson
    )

  @Test
  def nullTags(): Unit = an[NullPointerException] should be thrownBy ConnectorApi.access.request.tags(null)

  @Test
  def emptyTags(): Unit = ConnectorApi.access.request.tags(Map.empty)
  @Test
  def parseTags(): Unit =
    ConnectorApi.CREATION_FORMAT.read(s"""
      |  {
      |    "$WORKER_CLUSTER_KEY_KEY": {
      |      "group": "g",
      |      "name": "n"
      |    },
      |    "topicKeys": [
      |      {
      |        "group": "g",
      |        "name": "n"
      |      }
      |    ],
      |    "$CONNECTOR_CLASS_KEY": "oharastream.ohara.connector.ftp.FtpSource",
      |    "tags": {
      |      "a": "bb",
      |      "b": 123
      |    }
      |  }
      |     """.stripMargin.parseJson).tags shouldBe Map(
      "a" -> JsString("bb"),
      "b" -> JsNumber(123)
    )

  @Test
  def parseNullTags(): Unit =
    ConnectorApi.CREATION_FORMAT.read(s"""
      |  {
      |    "$WORKER_CLUSTER_KEY_KEY": {
      |      "group": "g",
      |      "name": "n"
      |    },
      |    "topicKeys": [
      |      {
      |        "group": "g",
      |        "name": "n"
      |      }
      |    ],
      |    "$CONNECTOR_CLASS_KEY": "oharastream.ohara.connector.ftp.FtpSource"
      |  }
      |     """.stripMargin.parseJson).tags shouldBe Map.empty

  @Test
  def defaultNumberOfTasks(): Unit =
    ConnectorApi.CREATION_FORMAT.read(s"""
       |  {
       |    "$WORKER_CLUSTER_KEY_KEY": {
       |      "group": "g",
       |      "name": "n"
       |    },
       |    "topicKeys": [
       |      {
       |        "group": "g",
       |        "name": "n"
       |      }
       |    ],
       |    "$CONNECTOR_CLASS_KEY": "oharastream.ohara.connector.ftp.FtpSource"
       |  }
       |     """.stripMargin.parseJson).numberOfTasks shouldBe ConnectorDefUtils.NUMBER_OF_TASKS_DEFINITION.defaultInt

  @Test
  def testCustomGroup(): Unit =
    ConnectorApi.access
      .hostname(CommonUtils.randomString(10))
      .port(CommonUtils.availablePort())
      .request
      .group("abc")
      .className(CommonUtils.randomString(10))
      .workerClusterKey(ObjectKey.of("g", "n"))
      .topicKey(TopicKey.of("g", "n"))
      .creation
      .group shouldBe "abc"

  @Test
  def testDefaultGroup(): Unit =
    ConnectorApi.access
      .hostname(CommonUtils.randomString(10))
      .port(CommonUtils.availablePort())
      .request
      .className(CommonUtils.randomString(10))
      .workerClusterKey(ObjectKey.of("g", "n"))
      .topicKey(TopicKey.of("g", "n"))
      .creation
      .group shouldBe GROUP_DEFAULT

  @Test
  def nameDefinitionShouldBeNonUpdatable(): Unit =
    ConnectorApi.DEFINITIONS.find(_.key() == NAME_KEY).get.permission() shouldBe Permission.CREATE_ONLY

  @Test
  def groupDefinitionShouldBeNonUpdatable(): Unit =
    ConnectorApi.DEFINITIONS.find(_.key() == GROUP_KEY).get.permission() shouldBe Permission.CREATE_ONLY

  @Test
  def settingsDisappearFromJson(): Unit = {
    val cluster = ConnectorInfo(
      settings = ConnectorApi.access.request.className("aa").workerClusterKey(ObjectKey.of("a", "b")).creation.raw,
      state = None,
      error = None,
      aliveNodes = Set.empty,
      tasksStatus = Seq.empty,
      nodeMetrics = Map.empty,
      lastModified = CommonUtils.current()
    )
    ConnectorApi.CONNECTOR_INFO_FORMAT.write(cluster).asJsObject.fields.keySet should not contain ("settings")
  }

  @Test
  def testInfoJsonRepresentation(): Unit = {
    val cluster = ConnectorInfo(
      settings = ConnectorApi.access.request.className("aa").workerClusterKey(ObjectKey.of("a", "b")).creation.raw,
      state = None,
      error = None,
      aliveNodes = Set.empty,
      tasksStatus = Seq.empty,
      nodeMetrics = Map.empty,
      lastModified = CommonUtils.current()
    )
    ConnectorApi.CONNECTOR_INFO_FORMAT.read(ConnectorApi.CONNECTOR_INFO_FORMAT.write(cluster)) shouldBe cluster
  }

  @Test
  def testDataTypeInColumns(): Unit = {
    DataType.all.asScala.foreach { dataType =>
      val order    = Math.abs(Random.nextInt())
      val name     = CommonUtils.randomString()
      val newName  = CommonUtils.randomString()
      val creation = CREATION_FORMAT.read(s"""
                                               |{
                                               |  "workerClusterKey": "wk00",
                                               |  "connector.class": "myClass",
                                               |  "topicKeys": ["tp00"],
                                               |  "columns": [
                                               |    {
                                               |      "order":$order,
                                               |      "dataType":"$dataType",
                                               |      "name":"$name",
                                               |      "newName":"$newName"
                                               |    }
                                               |  ]
                                               |}
      """.stripMargin.parseJson)
      creation.columns.size shouldBe 1
      creation.columns.head.order() shouldBe order
      creation.columns.head.dataType() shouldBe dataType
      creation.columns.head.name() shouldBe name
      creation.columns.head.newName() shouldBe newName
    }
  }

  @Test
  def userDefinedStateShouldBeRemoveFromCreation(): Unit =
    ConnectorApi.CREATION_FORMAT.read(s"""
                                         |{
                                         |  "workerClusterKey": "wk00",
                                         |  "connector.class": "myClass",
                                         |  "topicKeys": ["tp00"],
                                         |  "state": "RUNNING"
                                         |}
      """.stripMargin.parseJson).raw.get("state") shouldBe None

  @Test
  def userDefinedStateShouldBeRemoveFromUpdating(): Unit =
    ConnectorApi.UPDATING_FORMAT.read(s"""
                                         |  {
                                         |  "workerClusterKey": "wk00",
                                         |  "connector.class": "myClass",
                                         |  "topicKeys": ["tp00"],
                                         |  "state": "RUNNING"
                                         |  }
      """.stripMargin.parseJson).raw.get("state") shouldBe None
}
