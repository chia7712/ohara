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

import oharastream.ohara.client.configurator.WorkerApi._
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.setting.SettingDef.Permission
import oharastream.ohara.common.setting.{ObjectKey, SettingDef}
import oharastream.ohara.common.util.CommonUtils
import org.apache.kafka.common.record.CompressionType
import org.junit.jupiter.api.Test
import org.scalatest.matchers.should.Matchers._
import spray.json.DefaultJsonProtocol._
import spray.json.{DeserializationException, _}

import scala.jdk.CollectionConverters._
class TestWorkerApi extends OharaTest {
  private[this] final val accessApi =
    WorkerApi.access.hostname(CommonUtils.randomString(5)).port(CommonUtils.availablePort()).request

  @Test
  def testClone(): Unit = {
    val nodeNames = Set(CommonUtils.randomString())
    val workerClusterInfo = WorkerClusterInfo(
      settings = WorkerApi.access.request
        .nodeNames(Set(CommonUtils.randomString()))
        .brokerClusterKey(ObjectKey.of("g", "n"))
        .creation
        .raw,
      aliveNodes = Set.empty,
      state = None,
      error = None,
      lastModified = CommonUtils.current()
    )
    workerClusterInfo.newNodeNames(nodeNames).nodeNames shouldBe nodeNames
  }

  @Test
  def ignoreNameOnCreation(): Unit =
    accessApi
      .nodeName(CommonUtils.randomString(10))
      .brokerClusterKey(ObjectKey.of("g", "n"))
      .creation
      .name
      .length should not be 0

  @Test
  def testTags(): Unit =
    accessApi
      .nodeName(CommonUtils.randomString(10))
      .tags(Map("a" -> JsNumber(1), "b" -> JsString("2")))
      .brokerClusterKey(ObjectKey.of("g", "n"))
      .creation
      .tags
      .size shouldBe 2

  @Test
  def ignoreNodeNamesOnCreation(): Unit =
    an[DeserializationException] should be thrownBy accessApi.name(CommonUtils.randomString(5)).creation

  @Test
  def nullName(): Unit = an[NullPointerException] should be thrownBy accessApi.name(null)

  @Test
  def emptyName(): Unit = an[IllegalArgumentException] should be thrownBy accessApi.name("")

  @Test
  def nullGroup(): Unit = an[NullPointerException] should be thrownBy accessApi.group(null)

  @Test
  def emptyGroup(): Unit = an[IllegalArgumentException] should be thrownBy accessApi.group("")

  @Test
  def nullBrokerClusterKey(): Unit = an[NullPointerException] should be thrownBy accessApi.brokerClusterKey(null)

  @Test
  def nullNodeNames(): Unit = an[NullPointerException] should be thrownBy accessApi.nodeNames(null)

  @Test
  def emptyNodeNames(): Unit = an[IllegalArgumentException] should be thrownBy accessApi.nodeNames(Set.empty)

  @Test
  def negativeClientPort(): Unit = an[IllegalArgumentException] should be thrownBy accessApi.clientPort(-1)

  @Test
  def negativeJmxPort(): Unit = an[IllegalArgumentException] should be thrownBy accessApi.jmxPort(-1)

  @Test
  def nullConfigTopicName(): Unit = an[NullPointerException] should be thrownBy accessApi.configTopicName(null)

  @Test
  def emptyConfigTopicName(): Unit = an[IllegalArgumentException] should be thrownBy accessApi.configTopicName("")

  @Test
  def negativeNumberOfConfigTopicReplication(): Unit =
    an[IllegalArgumentException] should be thrownBy accessApi.configTopicReplications(-1)

  @Test
  def nullOffsetTopicName(): Unit = an[NullPointerException] should be thrownBy accessApi.offsetTopicName(null)

  @Test
  def emptyOffsetTopicName(): Unit = an[IllegalArgumentException] should be thrownBy accessApi.offsetTopicName("")

  @Test
  def negativeNumberOfOffsetTopicPartitions(): Unit =
    an[IllegalArgumentException] should be thrownBy accessApi.offsetTopicPartitions(-1)

  @Test
  def negativeNumberOfOffsetTopicReplication(): Unit =
    an[IllegalArgumentException] should be thrownBy accessApi.offsetTopicReplications(-1)

  @Test
  def nullStatusTopicName(): Unit = an[NullPointerException] should be thrownBy accessApi.statusTopicName(null)

  @Test
  def emptyStatusTopicName(): Unit = an[IllegalArgumentException] should be thrownBy accessApi.statusTopicName("")

  @Test
  def negativeNumberOfStatusTopicPartitions(): Unit =
    an[IllegalArgumentException] should be thrownBy accessApi.statusTopicPartitions(-1)

  @Test
  def negativeNumberOfStatusTopicReplication(): Unit =
    an[IllegalArgumentException] should be thrownBy accessApi.statusTopicReplications(-1)

  @Test
  def testCreation(): Unit = {
    val name                           = CommonUtils.randomString(5)
    val group                          = CommonUtils.randomString(10)
    val clientPort                     = CommonUtils.availablePort()
    val jmxPort                        = CommonUtils.availablePort()
    val brokerClusterKey               = ObjectKey.of("default", CommonUtils.randomString())
    val configTopicName                = CommonUtils.randomString(10)
    val configTopicReplications: Short = 2
    val offsetTopicName                = CommonUtils.randomString(10)
    val offsetTopicPartitions: Int     = 2
    val offsetTopicReplications: Short = 2
    val statusTopicName                = CommonUtils.randomString(10)
    val statusTopicPartitions: Int     = 2
    val statusTopicReplications: Short = 2
    val nodeName                       = CommonUtils.randomString()
    val creation = WorkerApi.access
      .hostname(CommonUtils.randomString(10))
      .port(CommonUtils.availablePort())
      .request
      .name(name)
      .group(group)
      .brokerClusterKey(brokerClusterKey)
      .configTopicName(configTopicName)
      .configTopicReplications(configTopicReplications)
      .offsetTopicName(offsetTopicName)
      .offsetTopicPartitions(offsetTopicPartitions)
      .offsetTopicReplications(offsetTopicReplications)
      .statusTopicName(statusTopicName)
      .statusTopicPartitions(statusTopicPartitions)
      .statusTopicReplications(statusTopicReplications)
      .clientPort(clientPort)
      .jmxPort(jmxPort)
      .nodeName(nodeName)
      .creation
    creation.name shouldBe name
    creation.group shouldBe group
    creation.clientPort shouldBe clientPort
    creation.jmxPort shouldBe jmxPort
    creation.brokerClusterKey shouldBe brokerClusterKey
    creation.configTopicName shouldBe configTopicName
    creation.configTopicReplications shouldBe configTopicReplications
    creation.offsetTopicName shouldBe offsetTopicName
    creation.offsetTopicPartitions shouldBe offsetTopicPartitions
    creation.offsetTopicReplications shouldBe offsetTopicReplications
    creation.statusTopicName shouldBe statusTopicName
    creation.statusTopicPartitions shouldBe statusTopicPartitions
    creation.statusTopicReplications shouldBe statusTopicReplications
    creation.nodeNames.head shouldBe nodeName
  }

  @Test
  def parseCreation(): Unit = {
    val nodeName = CommonUtils.randomString()
    val creation = WorkerApi.CREATION_FORMAT.read(s"""
      |  {
      |    "nodeNames": ["$nodeName"],
      |    "brokerClusterKey": {
      |      "group": "g",
      |      "name": "n"
      |    }
      |  }
      |  """.stripMargin.parseJson)
    creation.name.length shouldBe SettingDef.STRING_LENGTH_LIMIT
    creation.imageName shouldBe WorkerApi.IMAGE_NAME_DEFAULT
    creation.brokerClusterKey shouldBe ObjectKey.of("g", "n")
    creation.configTopicReplications shouldBe 1
    creation.offsetTopicReplications shouldBe 1
    creation.offsetTopicPartitions shouldBe 1
    creation.statusTopicReplications shouldBe 1
    creation.statusTopicPartitions shouldBe 1
    creation.nodeNames.size shouldBe 1
    creation.nodeNames.head shouldBe nodeName
    creation.pluginKeys.size shouldBe 0

    val name      = CommonUtils.randomString(10)
    val group     = CommonUtils.randomString(10)
    val creation2 = WorkerApi.CREATION_FORMAT.read(s"""
      |  {
      |    "name": "$name",
      |    "group": "$group",
      |    "brokerClusterKey": {
      |      "group": "g",
      |      "name": "n"
      |    },
      |    "nodeNames": ["$nodeName"]
      |  }
      |  """.stripMargin.parseJson)
    // group is support in create cluster
    creation2.name shouldBe name
    creation2.group shouldBe group
    creation2.imageName shouldBe WorkerApi.IMAGE_NAME_DEFAULT
    creation2.brokerClusterKey shouldBe ObjectKey.of("g", "n")
    creation2.configTopicReplications shouldBe 1
    creation2.offsetTopicReplications shouldBe 1
    creation2.offsetTopicPartitions shouldBe 1
    creation2.statusTopicReplications shouldBe 1
    creation2.statusTopicPartitions shouldBe 1
    creation2.nodeNames.size shouldBe 1
    creation2.nodeNames.head shouldBe nodeName
    creation2.pluginKeys.size shouldBe 0
    creation2.sharedJarKeys.size shouldBe 0
  }

  @Test
  def testUpdate(): Unit = {
    val name             = CommonUtils.randomString(10)
    val group            = CommonUtils.randomString(10)
    val clientPort       = CommonUtils.availablePort()
    val nodeName         = CommonUtils.randomString()
    val brokerClusterKey = ObjectKey.of("g", "n")

    val creation = accessApi.name(name).nodeName(nodeName).brokerClusterKey(brokerClusterKey).creation
    creation.name shouldBe name
    // use default values if absent
    creation.group shouldBe GROUP_DEFAULT
    creation.imageName shouldBe WorkerApi.IMAGE_NAME_DEFAULT
    creation.nodeNames shouldBe Set(nodeName)
    creation.brokerClusterKey shouldBe brokerClusterKey

    // initial a new update request
    val updateAsCreation = WorkerApi.access.request
      .name(name)
      // the group here is not as same as before
      // here we use update as creation
      .group(group)
      .clientPort(clientPort)
      .updating
    updateAsCreation.clientPort shouldBe Some(clientPort)
    updateAsCreation.nodeNames should not be Some(Set(nodeName))
  }

  @Test
  def parseEmptyNodeNames(): Unit =
    an[DeserializationException] should be thrownBy WorkerApi.CREATION_FORMAT.read(s"""
      |  {
      |    "name": "asdasd",
      |    "brokerClusterKey": {
      |      "group": "g",
      |      "name": "n"
      |    },
      |    "nodeNames": []
      |  }
      |  """.stripMargin.parseJson)

  @Test
  def parseNodeNamesOnUpdate(): Unit =
    intercept[DeserializationException] {
      WorkerApi.CREATION_FORMAT.read(s"""
           |  {
           |    "brokerClusterKey": {
           |      "group": "g",
           |      "name": "n"
           |    },
           |    "nodeNames": ""
           |  }
           |  """.stripMargin.parseJson)
    }.getMessage should include("the value of \"nodeNames\" can't be empty string")
  @Test
  def parseZeroClientPort(): Unit =
    an[DeserializationException] should be thrownBy WorkerApi.CREATION_FORMAT.read(s"""
      |  {
      |    "name": "name",
      |    "clientPort": 0,
      |    "brokerClusterKey": {
      |      "group": "g",
      |      "name": "n"
      |    },
      |    "nodeNames": ["n"]
      |  }
      |  """.stripMargin.parseJson)

  @Test
  def parseNegativeClientPort(): Unit =
    an[DeserializationException] should be thrownBy WorkerApi.CREATION_FORMAT.read(s"""
      |  {
      |    "name": "name",
      |    "clientPort": -1,
      |    "brokerClusterKey": {
      |      "group": "g",
      |      "name": "n"
      |    },
      |    "nodeNames": ["n"]
      |  }
      |  """.stripMargin.parseJson)

  @Test
  def parseLargeClientPort(): Unit =
    an[DeserializationException] should be thrownBy WorkerApi.CREATION_FORMAT.read(s"""
      |  {
      |    "name": "name",
      |    "clientPort": 999999,
      |    "brokerClusterKey": {
      |      "group": "g",
      |      "name": "n"
      |    },
      |    "nodeNames": ["n"]
      |  }
      |  """.stripMargin.parseJson)

  @Test
  def parseClientPortOnUpdate(): Unit = {
    intercept[DeserializationException] {
      WorkerApi.CREATION_FORMAT.read(s"""
           |  {
           |    "nodeNames": [
           |      "node"
           |    ],
           |    "brokerClusterKey": {
           |      "group": "g",
           |      "name": "n"
           |    },
           |    "clientPort": 0
           |  }
           |  """.stripMargin.parseJson)
    }.getMessage should include("the number must be")

    intercept[DeserializationException] {
      WorkerApi.CREATION_FORMAT.read(s"""
           |  {
           |    "nodeNames": [
           |      "node"
           |    ],
           |    "brokerClusterKey": {
           |      "group": "g",
           |      "name": "n"
           |    },
           |    "clientPort": -9
           |  }
           |  """.stripMargin.parseJson)
    }.getMessage should include("the number must be")

    intercept[DeserializationException] {
      WorkerApi.CREATION_FORMAT.read(s"""
           |  {
           |    "nodeNames": [
           |      "node"
           |    ],
           |    "brokerClusterKey": {
           |      "group": "g",
           |      "name": "n"
           |    },
           |    "clientPort": 99999
           |  }
           |  """.stripMargin.parseJson)
    }.getMessage should include("the number must be")
  }

  @Test
  def parseZeroJmxPort(): Unit =
    an[DeserializationException] should be thrownBy WorkerApi.CREATION_FORMAT.read(s"""
      |  {
      |    "name": "name",
      |    "jmxPort": 0,
      |    "brokerClusterKey": {
      |      "group": "g",
      |      "name": "n"
      |    },
      |    "nodeNames": ["n"]
      |  }
      |  """.stripMargin.parseJson)

  @Test
  def parseNegativeJmxPort(): Unit =
    an[DeserializationException] should be thrownBy WorkerApi.CREATION_FORMAT.read(s"""
      |  {
      |    "name": "name",
      |    "jmxPort": -1,
      |    "brokerClusterKey": {
      |      "group": "g",
      |      "name": "n"
      |    },
      |    "nodeNames": ["n"]
      |  }
      |  """.stripMargin.parseJson)

  @Test
  def parseLargeJmxPort(): Unit =
    an[DeserializationException] should be thrownBy WorkerApi.CREATION_FORMAT.read(s"""
      |  {
      |    "name": "name",
      |    "jmxPort": 999999,
      |    "brokerClusterKey": {
      |      "group": "g",
      |      "name": "n"
      |    },
      |    "nodeNames": ["n"]
      |  }
      |  """.stripMargin.parseJson)

  @Test
  def parseJmxPortOnCreation(): Unit = {
    intercept[DeserializationException] {
      WorkerApi.CREATION_FORMAT.read(s"""
           |  {
           |    "nodeNames": [
           |      "node"
           |    ],
           |    "brokerClusterKey": {
           |      "group": "g",
           |      "name": "n"
           |    },
           |    "jmxPort": 0
           |  }
           |  """.stripMargin.parseJson)
    }.getMessage should include("the number must be")

    intercept[DeserializationException] {
      WorkerApi.CREATION_FORMAT.read(s"""
           |  {
           |    "nodeNames": [
           |      "node"
           |    ],
           |    "brokerClusterKey": {
           |      "group": "g",
           |      "name": "n"
           |    },
           |    "jmxPort": -9
           |  }
           |  """.stripMargin.parseJson)
    }.getMessage should include("the number must be")

    intercept[DeserializationException] {
      WorkerApi.CREATION_FORMAT.read(s"""
           |  {
           |    "nodeNames": [
           |      "node"
           |    ],
           |    "brokerClusterKey": {
           |      "group": "g",
           |      "name": "n"
           |    },
           |    "jmxPort": 99999
           |  }
           |  """.stripMargin.parseJson)
    }.getMessage should include("the number must be")
  }

  /**
    * CONFIG_TOPIC_PARTITIONS_DEFINITION is readonly so the value is immutable
    */
  @Test
  def zeroNumberOfPartitionsForConfigTopic(): Unit =
    WorkerApi.CREATION_FORMAT
      .read(
        s"""
         |  {
         |    "nodeNames": [
         |      "node"
         |    ],
         |    "brokerClusterKey": {
         |      "group": "g",
         |      "name": "n"
         |    },
         |    "${CONFIG_TOPIC_PARTITIONS_KEY}": 0
         |  }
         |  """.stripMargin.parseJson
      )
      .raw(CONFIG_TOPIC_PARTITIONS_KEY)
      .convertTo[Int] shouldBe 1

  /**
    * CONFIG_TOPIC_PARTITIONS_DEFINITION is readonly so the value is immutable
    */
  @Test
  def negativeNumberOfPartitionsForConfigTopic(): Unit =
    WorkerApi.CREATION_FORMAT
      .read(
        s"""
                           |  {
                           |    "nodeNames": [
                           |      "node"
                           |    ],
                           |    "brokerClusterKey": {
                           |      "group": "g",
                           |      "name": "n"
                           |    },
                           |    "${CONFIG_TOPIC_PARTITIONS_KEY}": -1
                           |  }
                           |  """.stripMargin.parseJson
      )
      .raw(CONFIG_TOPIC_PARTITIONS_KEY)
      .convertTo[Int] shouldBe 1

  @Test
  def zeroNumberOfReplicationForConfigTopic(): Unit =
    intercept[DeserializationException] {
      WorkerApi.CREATION_FORMAT.read(s"""
                                                    |  {
                                                    |    "nodeNames": [
                                                    |      "node"
                                                    |    ],
                                                    |    "brokerClusterKey": {
                                                    |      "group": "g",
                                                    |      "name": "n"
                                                    |    },
                                                    |    "${CONFIG_TOPIC_REPLICATIONS_KEY}": 0
                                                    |  }
                                                    |  """.stripMargin.parseJson)
    }.getMessage should include("the number must be")

  @Test
  def negativeNumberOfReplicationForConfigTopic(): Unit =
    intercept[DeserializationException] {
      WorkerApi.CREATION_FORMAT.read(s"""
                                                    |  {
                                                    |    "nodeNames": [
                                                    |      "node"
                                                    |    ],
                                                    |    "brokerClusterKey": {
                                                    |      "group": "g",
                                                    |      "name": "n"
                                                    |    },
                                                    |    "${CONFIG_TOPIC_REPLICATIONS_KEY}": -1
                                                    |  }
                                                    |  """.stripMargin.parseJson)
    }.getMessage should include("the number must be")

  @Test
  def zeroNumberOfPartitionsForOffsetTopic(): Unit =
    intercept[DeserializationException] {
      WorkerApi.CREATION_FORMAT.read(s"""
                                                    |  {
                                                    |    "nodeNames": [
                                                    |      "node"
                                                    |    ],
                                                    |    "brokerClusterKey": {
                                                    |      "group": "g",
                                                    |      "name": "n"
                                                    |    },
                                                    |    "${OFFSET_TOPIC_PARTITIONS_KEY}": 0
                                                    |  }
                                                    |  """.stripMargin.parseJson)
    }.getMessage should include("the number must be")

  @Test
  def negativeNumberOfPartitionsForOffsetTopic(): Unit =
    intercept[DeserializationException] {
      WorkerApi.CREATION_FORMAT.read(s"""
                                                    |  {
                                                    |    "nodeNames": [
                                                    |      "node"
                                                    |    ],
                                                    |    "brokerClusterKey": {
                                                    |      "group": "g",
                                                    |      "name": "n"
                                                    |    },
                                                    |    "${OFFSET_TOPIC_PARTITIONS_KEY}": -1
                                                    |  }
                                                    |  """.stripMargin.parseJson)
    }.getMessage should include("the number must be")

  @Test
  def zeroNumberOfReplicationForOffsetTopic(): Unit =
    intercept[DeserializationException] {
      WorkerApi.CREATION_FORMAT.read(s"""
                                                    |  {
                                                    |    "nodeNames": [
                                                    |      "node"
                                                    |    ],
                                                    |    "brokerClusterKey": {
                                                    |      "group": "g",
                                                    |      "name": "n"
                                                    |    },
                                                    |    "${OFFSET_TOPIC_REPLICATIONS_KEY}": 0
                                                    |  }
                                                    |  """.stripMargin.parseJson)
    }.getMessage should include("the number must be")

  @Test
  def negativeNumberOfReplicationForOffsetTopic(): Unit =
    intercept[DeserializationException] {
      WorkerApi.CREATION_FORMAT.read(s"""
                                                    |  {
                                                    |    "nodeNames": [
                                                    |      "node"
                                                    |    ],
                                                    |    "brokerClusterKey": {
                                                    |      "group": "g",
                                                    |      "name": "n"
                                                    |    },
                                                    |    "${OFFSET_TOPIC_REPLICATIONS_KEY}": -1
                                                    |  }
                                                    |  """.stripMargin.parseJson)
    }.getMessage should include("the number must be")

  @Test
  def zeroNumberOfPartitionsForStatusTopic(): Unit =
    intercept[DeserializationException] {
      WorkerApi.CREATION_FORMAT.read(s"""
                                                    |  {
                                                    |    "nodeNames": [
                                                    |      "node"
                                                    |    ],
                                                    |    "brokerClusterKey": {
                                                    |      "group": "g",
                                                    |      "name": "n"
                                                    |    },
                                                    |    "${STATUS_TOPIC_PARTITIONS_KEY}": 0
                                                    |  }
                                                    |  """.stripMargin.parseJson)
    }.getMessage should include("the number must be")

  @Test
  def negativeNumberOfPartitionsForStatusTopic(): Unit =
    intercept[DeserializationException] {
      WorkerApi.CREATION_FORMAT.read(s"""
                                                    |  {
                                                    |    "nodeNames": [
                                                    |      "node"
                                                    |    ],
                                                    |    "brokerClusterKey": {
                                                    |      "group": "g",
                                                    |      "name": "n"
                                                    |    },
                                                    |    "${STATUS_TOPIC_PARTITIONS_KEY}": -1
                                                    |  }
                                                    |  """.stripMargin.parseJson)
    }.getMessage should include("the number must be")

  @Test
  def zeroNumberOfReplicationForStatusTopic(): Unit =
    intercept[DeserializationException] {
      WorkerApi.CREATION_FORMAT.read(s"""
                                                    |  {
                                                    |    "nodeNames": [
                                                    |      "node"
                                                    |    ],
                                                    |    "brokerClusterKey": {
                                                    |      "group": "g",
                                                    |      "name": "n"
                                                    |    },
                                                    |    "${STATUS_TOPIC_REPLICATIONS_KEY}": 0
                                                    |  }
                                                    |  """.stripMargin.parseJson)
    }.getMessage should include("the number must be")

  @Test
  def negativeNumberOfReplicationForStatusTopic(): Unit =
    intercept[DeserializationException] {
      WorkerApi.CREATION_FORMAT.read(s"""
                                                    |  {
                                                    |    "nodeNames": [
                                                    |      "node"
                                                    |    ],
                                                    |    "brokerClusterKey": {
                                                    |      "group": "g",
                                                    |      "name": "n"
                                                    |    },
                                                    |    "${STATUS_TOPIC_REPLICATIONS_KEY}": -1
                                                    |  }
                                                    |  """.stripMargin.parseJson)
    }.getMessage should include("the number must be")

  @Test
  def testDeadNodes(): Unit = {
    val cluster = WorkerClusterInfo(
      settings =
        WorkerApi.access.request.nodeNames(Set("n0", "n1")).brokerClusterKey(ObjectKey.of("g", "n")).creation.raw,
      aliveNodes = Set("n0"),
      state = Some(ClusterState.RUNNING),
      error = None,
      lastModified = CommonUtils.current()
    )
    cluster.nodeNames shouldBe Set("n0", "n1")
    cluster.deadNodes shouldBe Set("n1")
    cluster.copy(state = None).deadNodes shouldBe Set.empty
  }

  @Test
  def testFreePorts(): Unit = {
    WorkerApi.access.request
      .nodeName(CommonUtils.randomString(10))
      .brokerClusterKey(ObjectKey.of("g", "n"))
      .creation
      .freePorts shouldBe Set.empty

    val freePorts = Set(CommonUtils.availablePort(), CommonUtils.availablePort())
    WorkerApi.access.request
      .nodeName(CommonUtils.randomString(10))
      .freePorts(freePorts)
      .brokerClusterKey(ObjectKey.of("g", "n"))
      .creation
      .freePorts shouldBe freePorts
  }

  @Test
  def stringArrayToPluginKeys(): Unit = {
    val key      = CommonUtils.randomString()
    val updating = WorkerApi.UPDATING_FORMAT.read(s"""
                                                  |  {
                                                  |    "pluginKeys": ["$key"]
                                                  |  }
                                                  |  """.stripMargin.parseJson)
    updating.pluginKeys.get.head shouldBe ObjectKey.of(GROUP_DEFAULT, key)
  }

  @Test
  def stringArrayToSharedJarKeys(): Unit = {
    val key      = CommonUtils.randomString()
    val updating = WorkerApi.UPDATING_FORMAT.read(s"""
                                                                 |  {
                                                                 |    "sharedJarKeys": ["$key"]
                                                                 |  }
                                                                 |  """.stripMargin.parseJson)
    updating.sharedJarKeys.get.head shouldBe ObjectKey.of(GROUP_DEFAULT, key)
  }

  @Test
  def emptyNodeNamesShouldPassInUpdating(): Unit = {
    WorkerApi.UPDATING_FORMAT.read(s"""
                                           |  {
                                           |    "nodeNames": []
                                           |  }
                                           |  """.stripMargin.parseJson).nodeNames shouldBe Some(Set.empty)
  }

  @Test
  def groupShouldAppearInResponse(): Unit = {
    val name = CommonUtils.randomString(5)
    val res = WorkerApi.WORKER_CLUSTER_INFO_FORMAT.write(
      WorkerClusterInfo(
        settings =
          accessApi.name(name).brokerClusterKey(ObjectKey.of("default", "bk1")).nodeNames(Set("n1")).creation.raw,
        aliveNodes = Set.empty,
        state = None,
        error = None,
        lastModified = CommonUtils.current()
      )
    )

    // serialize to json should see the object key (group, name)
    res.asJsObject.fields(NAME_KEY).convertTo[String] shouldBe name
    res.asJsObject.fields(GROUP_KEY).convertTo[String] shouldBe GROUP_DEFAULT
  }

  @Test
  def testConnectionProps(): Unit = {
    val cluster = WorkerClusterInfo(
      settings =
        WorkerApi.access.request.nodeNames(Set("n0", "m1")).brokerClusterKey(ObjectKey.of("g", "n")).creation.raw,
      aliveNodes = Set("nn"),
      state = None,
      error = None,
      lastModified = CommonUtils.current()
    )
    cluster.connectionProps should not include "nn"
  }

  @Test
  def testBrokerClusterKey(): Unit = {
    val bkKey = ObjectKey.of(CommonUtils.randomString(10), CommonUtils.randomString(10))
    accessApi.nodeName("n").brokerClusterKey(bkKey).creation.brokerClusterKey shouldBe bkKey
  }

  @Test
  def defaultValueShouldBeAppendedToResponse(): Unit = {
    val cluster = WorkerClusterInfo(
      settings =
        WorkerApi.access.request.nodeNames(Set("n0", "n1")).brokerClusterKey(ObjectKey.of("g", "n")).creation.raw,
      aliveNodes = Set("n0"),
      state = Some(ClusterState.RUNNING),
      error = None,
      lastModified = CommonUtils.current()
    )

    val string = WorkerApi.WORKER_CLUSTER_INFO_FORMAT.write(cluster).toString()

    WorkerApi.DEFINITIONS
      .filter(_.hasDefault)
      // the immutable setting is not in custom field
      .filter(_.permission() == Permission.EDITABLE)
      .foreach { definition =>
        string should include(definition.key())
        string should include(definition.defaultValue().get().toString)
      }
  }

  @Test
  def checkNameDefinition(): Unit = WorkerApi.DEFINITIONS.find(_.key() == NAME_KEY) should not be None

  @Test
  def checkImageNameDefinition(): Unit = WorkerApi.DEFINITIONS.find(_.key() == IMAGE_NAME_KEY) should not be None

  @Test
  def checkGroupDefinition(): Unit = WorkerApi.DEFINITIONS.find(_.key() == GROUP_KEY) should not be None

  @Test
  def checkNodeNamesDefinition(): Unit = WorkerApi.DEFINITIONS.find(_.key() == NODE_NAMES_KEY) should not be None

  @Test
  def checkTagDefinition(): Unit = WorkerApi.DEFINITIONS.find(_.key() == TAGS_KEY) should not be None

  @Test
  def checkClientPortDefinition(): Unit = WorkerApi.DEFINITIONS.find(_.key() == CLIENT_PORT_KEY) should not be None

  @Test
  def nameDefinitionShouldBeNonUpdatable(): Unit =
    WorkerApi.DEFINITIONS.find(_.key() == NAME_KEY).get.permission() shouldBe Permission.CREATE_ONLY

  @Test
  def groupDefinitionShouldBeNonUpdatable(): Unit =
    WorkerApi.DEFINITIONS.find(_.key() == GROUP_KEY).get.permission() shouldBe Permission.CREATE_ONLY

  @Test
  def testMaxHeap(): Unit = WorkerApi.CREATION_FORMAT.read(s"""
                                                                                |  {
                                                                                |    "brokerClusterKey": "bk",
                                                                                |    "nodeNames": ["node00"],
                                                                                |    "xmx": 123
                                                                                |  }
      """.stripMargin.parseJson).maxHeap shouldBe 123

  @Test
  def testInitHeap(): Unit = WorkerApi.CREATION_FORMAT.read(s"""
                                                                                 |  {
                                                                                 |    "brokerClusterKey": "bk",
                                                                                 |    "nodeNames": ["node00"],
                                                                                 |    "xms": 123
                                                                                 |  }
      """.stripMargin.parseJson).initHeap shouldBe 123

  @Test
  def testNegativeMaxHeap(): Unit =
    an[DeserializationException] should be thrownBy WorkerApi.CREATION_FORMAT.read(s"""
                                                                                    |  {
                                                                                    |    "brokerClusterKey": "bk",
                                                                                    |    "nodeNames": ["node00"],
                                                                                    |    "xmx": -123
                                                                                    |  }
      """.stripMargin.parseJson)

  @Test
  def testNegativeInitHeap(): Unit =
    an[DeserializationException] should be thrownBy WorkerApi.CREATION_FORMAT.read(s"""
                                                                                         |  {
                                                                                         |    "brokerClusterKey": "bk",
                                                                                         |    "nodeNames": ["node00"],
                                                                                         |    "xms": -123
                                                                                         |  }
      """.stripMargin.parseJson)

  @Test
  def settingsDisappearFromJson(): Unit = {
    val cluster = WorkerClusterInfo(
      settings =
        WorkerApi.access.request.brokerClusterKey(ObjectKey.of("a", "b")).nodeNames(Set("n0", "n1")).creation.raw,
      aliveNodes = Set("n0"),
      state = Some(ClusterState.RUNNING),
      error = None,
      lastModified = CommonUtils.current()
    )
    WorkerApi.WORKER_CLUSTER_INFO_FORMAT.write(cluster).asJsObject.fields.keySet should not contain ("settings")
  }

  @Test
  def testInfoJsonRepresentation(): Unit = {
    val cluster = WorkerClusterInfo(
      settings =
        WorkerApi.access.request.brokerClusterKey(ObjectKey.of("a", "b")).nodeNames(Set("n0", "n1")).creation.raw,
      aliveNodes = Set("n0"),
      state = Some(ClusterState.RUNNING),
      error = None,
      lastModified = CommonUtils.current()
    )
    WorkerApi.WORKER_CLUSTER_INFO_FORMAT.read(WorkerApi.WORKER_CLUSTER_INFO_FORMAT.write(cluster)) shouldBe cluster
  }

  @Test
  def testDataDir(): Unit = {
    val creation = WorkerApi.CREATION_FORMAT.read(s"""
                                                          |  {
                                                          |    "brokerClusterKey": "bk",
                                                          |    "nodeNames": ["node00"]
                                                          |  }
      """.stripMargin.parseJson)
    creation.volumeMaps.size shouldBe 0
  }

  @Test
  def userDefinedStateShouldBeRemoveFromCreation(): Unit =
    WorkerApi.CREATION_FORMAT.read(s"""
                                     |  {
                                     |    "nodeNames": ["node00"],
                                     |    "brokerClusterKey": "bk",
                                     |    "state": "RUNNING"
                                     |  }
      """.stripMargin.parseJson).raw.get("state") shouldBe None

  @Test
  def userDefinedStateShouldBeRemoveFromUpdating(): Unit =
    WorkerApi.UPDATING_FORMAT.read(s"""
                                     |  {
                                     |    "brokerClusterKey": "bk",
                                     |    "state": "RUNNING"
                                     |  }
      """.stripMargin.parseJson).raw.get("state") shouldBe None

  @Test
  def testCompressionType(): Unit =
    WorkerApi.DEFINITIONS
      .find(_.key() == WorkerApi.COMPRESSION_TYPE_KEY)
      .get
      .recommendedValues()
      .asScala shouldBe CompressionType.values().map(_.name).toSet
}
