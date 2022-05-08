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

import oharastream.ohara.client.configurator.BrokerApi._
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.setting.SettingDef.Permission
import oharastream.ohara.common.setting.{ObjectKey, SettingDef}
import oharastream.ohara.common.util.CommonUtils
import org.junit.jupiter.api.Test
import org.scalatest.matchers.should.Matchers._
import spray.json.DefaultJsonProtocol._
import spray.json.{DeserializationException, _}

class TestBrokerApi extends OharaTest {
  private[this] final val access =
    BrokerApi.access.hostname(CommonUtils.randomString(5)).port(CommonUtils.availablePort()).request

  @Test
  def testClone(): Unit = {
    val nodeNames = Set(CommonUtils.randomString())
    val brokerClusterInfo = BrokerClusterInfo(
      settings =
        access.nodeNames(Set(CommonUtils.randomString())).zookeeperClusterKey(ObjectKey.of("g", "n")).creation.raw,
      aliveNodes = Set.empty,
      state = None,
      error = None,
      lastModified = CommonUtils.current()
    )
    brokerClusterInfo.newNodeNames(nodeNames).nodeNames shouldBe nodeNames
  }

  @Test
  def ignoreNameOnCreation(): Unit =
    access
      .nodeName(CommonUtils.randomString(10))
      .zookeeperClusterKey(ObjectKey.of("g", "n"))
      .creation
      .name
      .length should not be 0

  @Test
  def testTags(): Unit =
    access
      .nodeName(CommonUtils.randomString(10))
      .tags(Map("a" -> JsNumber(1), "b" -> JsString("2")))
      .zookeeperClusterKey(ObjectKey.of("g", "n"))
      .creation
      .tags
      .size shouldBe 2

  @Test
  def ignoreNodeNamesOnCreation(): Unit =
    an[DeserializationException] should be thrownBy access.name(CommonUtils.randomString(10)).creation

  @Test
  def nullName(): Unit = an[NullPointerException] should be thrownBy access.name(null)

  @Test
  def emptyName(): Unit = an[IllegalArgumentException] should be thrownBy access.name("")

  @Test
  def nullGroup(): Unit = an[NullPointerException] should be thrownBy access.group(null)

  @Test
  def emptyGroup(): Unit = an[IllegalArgumentException] should be thrownBy access.group("")

  @Test
  def nullZookeeperClusterKey(): Unit = an[NullPointerException] should be thrownBy access.zookeeperClusterKey(null)

  @Test
  def nullNodeNames(): Unit = an[NullPointerException] should be thrownBy access.nodeNames(null)

  @Test
  def emptyNodeNames(): Unit = an[IllegalArgumentException] should be thrownBy access.nodeNames(Set.empty)

  @Test
  def negativeClientPort(): Unit = an[IllegalArgumentException] should be thrownBy access.clientPort(-1)

  @Test
  def negativeJmxPort(): Unit = an[IllegalArgumentException] should be thrownBy access.jmxPort(-1)

  @Test
  def testCreation(): Unit = {
    val name       = CommonUtils.randomString(10)
    val group      = CommonUtils.randomString(10)
    val clientPort = CommonUtils.availablePort()
    val jmxPort    = CommonUtils.availablePort()
    val zkKey      = ObjectKey.of(CommonUtils.randomString(), CommonUtils.randomString())
    val nodeName   = CommonUtils.randomString()
    val creation = access
      .name(name)
      .group(group)
      .zookeeperClusterKey(zkKey)
      .clientPort(clientPort)
      .jmxPort(jmxPort)
      .nodeName(nodeName)
      .creation
    creation.name shouldBe name
    creation.group shouldBe group
    creation.clientPort shouldBe clientPort
    creation.jmxPort shouldBe jmxPort
    creation.zookeeperClusterKey shouldBe zkKey
    creation.nodeNames.head shouldBe nodeName
  }

  @Test
  def testExtraSettingInCreation(): Unit = {
    val name  = CommonUtils.randomString(10)
    val name2 = JsString(CommonUtils.randomString(10))
    val creation = access
      .name(name)
      .nodeNames(Set("n1"))
      .settings(Map("name" -> name2))
      .zookeeperClusterKey(ObjectKey.of("g", "n"))
      .creation

    // settings() has higher priority than name()
    creation.name shouldBe name2.value
  }

  @Test
  def parseCreation(): Unit = {
    val nodeName = CommonUtils.randomString()
    val creation = BrokerApi.CREATION_FORMAT.read(s"""
      |  {
      |    "$ZOOKEEPER_CLUSTER_KEY_KEY": {
      |      "group": "g",
      |      "name": "n"
      |    },
      |    "nodeNames": ["$nodeName"]
      |  }
      """.stripMargin.parseJson)
    creation.group shouldBe GROUP_DEFAULT
    creation.name.length shouldBe SettingDef.STRING_LENGTH_LIMIT
    creation.imageName shouldBe BrokerApi.IMAGE_NAME_DEFAULT
    creation.zookeeperClusterKey should not be None
    creation.nodeNames.size shouldBe 1
    creation.nodeNames.head shouldBe nodeName
    creation.clientPort should not be 0
    creation.jmxPort should not be 0
    creation.ports.size shouldBe 2

    val name       = CommonUtils.randomString(10)
    val group      = CommonUtils.randomString(10)
    val zkKey      = ObjectKey.of(CommonUtils.randomString(10), CommonUtils.randomString(10))
    val clientPort = CommonUtils.availablePort()
    val jmxPort    = CommonUtils.availablePort()
    val creation2  = BrokerApi.CREATION_FORMAT.read(s"""
      |  {
      |    "name": "$name",
      |    "group": "$group",
      |    "clientPort": $clientPort,
      |    "jmxPort": $jmxPort,
      |    "zookeeperClusterKey": ${zkKey.toString},
      |    "nodeNames": ["$nodeName"]
      |  }
      """.stripMargin.parseJson)
    // group is support in create cluster
    creation2.group shouldBe group
    creation2.name shouldBe name
    creation2.imageName shouldBe BrokerApi.IMAGE_NAME_DEFAULT
    creation2.nodeNames.size shouldBe 1
    creation2.nodeNames.head shouldBe nodeName
    creation2.zookeeperClusterKey.name() shouldBe zkKey.name()
    creation2.clientPort shouldBe clientPort
    creation2.jmxPort shouldBe jmxPort

    val creation3 = BrokerApi.CREATION_FORMAT.read(s"""
      |  {
      |    "zookeeperClusterKey": ${zkKey.toString},
      |    "name": "$name",
      |    "nodeNames": ["$nodeName"]
      |  }
      """.stripMargin.parseJson)

    creation3.name shouldBe name
    creation3.nodeNames.size shouldBe 1
    creation3.nodeNames.head shouldBe nodeName
    creation3.imageName shouldBe BrokerApi.IMAGE_NAME_DEFAULT
    creation3.clientPort should not be 0
    creation3.jmxPort should not be 0
  }

  @Test
  def testUpdate(): Unit = {
    val name       = CommonUtils.randomString(10)
    val group      = CommonUtils.randomString(10)
    val clientPort = CommonUtils.availablePort()
    val nodeName   = CommonUtils.randomString()

    val creation = access.name(name).nodeName(nodeName).zookeeperClusterKey(ObjectKey.of("g", "n")).creation
    creation.name shouldBe name
    // use default values if absent
    creation.group shouldBe GROUP_DEFAULT
    creation.imageName shouldBe BrokerApi.IMAGE_NAME_DEFAULT
    creation.nodeNames shouldBe Set(nodeName)

    // initial a new update request
    val updateAsCreation = BrokerApi.access.request
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
  def testDefaultName(): Unit = BrokerApi.CREATION_FORMAT.read(s"""
      |  {
      |    "$ZOOKEEPER_CLUSTER_KEY_KEY": {
      |      "group": "g",
      |      "name": "n"
      |    },
      |    "nodeNames": ["n1"]
      |  }
      """.stripMargin.parseJson).name.nonEmpty shouldBe true

  @Test
  def parseNameField(): Unit =
    intercept[DeserializationException] {
      BrokerApi.CREATION_FORMAT.read(s"""
           |  {
           |    "$ZOOKEEPER_CLUSTER_KEY_KEY": {
           |      "group": "g",
           |      "name": "n"
           |    },
           |    "nodeNames": ["n"],
           |    "name": ""
           |  }
           |  """.stripMargin.parseJson)
    }.getMessage should include("the value of \"name\" can't be empty string")

  @Test
  def parseImageNameField(): Unit =
    BrokerApi.CREATION_FORMAT
      .read(s"""
                                                  |  {
                                                  |    "$ZOOKEEPER_CLUSTER_KEY_KEY": {
                                                  |      "group": "g",
                                                  |      "name": "n"
                                                  |    },
                                                  |    "nodeNames": ["n"],
                                                  |    "imageName": "${BrokerApi.IMAGE_NAME_DEFAULT}"
                                                  |  }
                                                  |  """.stripMargin.parseJson)
      .raw(IMAGE_NAME_KEY)
      .convertTo[String] shouldBe BrokerApi.IMAGE_NAME_DEFAULT

  @Test
  def emptyNodeNamesShouldPassInUpdating(): Unit = {
    BrokerApi.UPDATING_FORMAT.read(s"""
                                              |  {
                                              |    "nodeNames": []
                                              |  }
                                              |  """.stripMargin.parseJson).nodeNames shouldBe Some(Set.empty)
  }
  @Test
  def parseImageNameOnUpdate(): Unit =
    intercept[DeserializationException] {
      BrokerApi.UPDATING_FORMAT.read(s"""
           |  {
           |    "imageName": ""
           |  }
      """.stripMargin.parseJson)
    }.getMessage should include("the value of \"imageName\" can't be empty string")

  @Test
  def testEmptyNodeNames(): Unit =
    an[DeserializationException] should be thrownBy BrokerApi.CREATION_FORMAT.read(s"""
      |  {
      |    "$ZOOKEEPER_CLUSTER_KEY_KEY": {
      |      "group": "g",
      |      "name": "n"
      |    },
      |    "name": "name",
      |    "nodeNames": []
      |  }
      """.stripMargin.parseJson)

  @Test
  def parseNodeNamesOnUpdate(): Unit =
    intercept[DeserializationException] {
      BrokerApi.UPDATING_FORMAT.read(s"""
           |  {
           |    "nodeNames": ""
           |  }
      """.stripMargin.parseJson)
    }.getMessage should include("the value of \"nodeNames\" can't be empty string")

  @Test
  def parseZeroClientPort(): Unit =
    an[DeserializationException] should be thrownBy BrokerApi.CREATION_FORMAT.read(s"""
      |  {
      |    "$ZOOKEEPER_CLUSTER_KEY_KEY": {
      |      "group": "g",
      |      "name": "n"
      |    },
      |    "name": "name",
      |    "clientPort": 0,
      |    "nodeNames": ["n"]
      |  }
      """.stripMargin.parseJson)

  @Test
  def parseNegativeClientPort(): Unit =
    an[DeserializationException] should be thrownBy BrokerApi.CREATION_FORMAT.read(s"""
      |  {
      |    "$ZOOKEEPER_CLUSTER_KEY_KEY": {
      |      "group": "g",
      |      "name": "n"
      |    },
      |    "name": "name",
      |    "clientPort": -1,
      |    "nodeNames": ["n"]
      |  }
      """.stripMargin.parseJson)

  @Test
  def parseLargeClientPort(): Unit =
    an[DeserializationException] should be thrownBy BrokerApi.CREATION_FORMAT.read(s"""
      |  {
      |    "$ZOOKEEPER_CLUSTER_KEY_KEY": {
      |      "group": "g",
      |      "name": "n"
      |    },
      |    "name": "name",
      |    "clientPort": 999999,
      |    "nodeNames": ["n"]
      |  }
      """.stripMargin.parseJson)

  @Test
  def parseClientPortOnUpdate(): Unit = {
    intercept[DeserializationException] {
      BrokerApi.CREATION_FORMAT.read(s"""
           |  {
           |    "$ZOOKEEPER_CLUSTER_KEY_KEY": {
           |      "group": "g",
           |      "name": "n"
           |    },
           |    "nodeNames": [
           |      "node"
           |    ],
           |    "clientPort": 0
           |  }
      """.stripMargin.parseJson)
    }.getMessage should include("the number must be")

    intercept[DeserializationException] {
      BrokerApi.CREATION_FORMAT.read(s"""
           |  {
           |    "$ZOOKEEPER_CLUSTER_KEY_KEY": {
           |      "group": "g",
           |      "name": "n"
           |    },
           |    "nodeNames": [
           |      "node"
           |    ],
           |    "clientPort": -9
           |  }
      """.stripMargin.parseJson)
    }.getMessage should include("the number must be")

    intercept[DeserializationException] {
      BrokerApi.CREATION_FORMAT.read(s"""
           |  {
           |    "$ZOOKEEPER_CLUSTER_KEY_KEY": {
           |      "group": "g",
           |      "name": "n"
           |    },
           |    "nodeNames": [
           |      "node"
           |    ],
           |    "clientPort": 99999
           |  }
      """.stripMargin.parseJson)
    }.getMessage should include("the number must be")
  }

  @Test
  def parseZeroJmxPort(): Unit =
    an[DeserializationException] should be thrownBy BrokerApi.CREATION_FORMAT.read(s"""
      |  {
      |    "$ZOOKEEPER_CLUSTER_KEY_KEY": {
      |      "group": "g",
      |      "name": "n"
      |    },
      |    "name": "name",
      |    "jmxPort": 0,
      |    "nodeNames": ["n"]
      |  }
      """.stripMargin.parseJson)

  @Test
  def parseNegativeJmxPort(): Unit =
    an[DeserializationException] should be thrownBy BrokerApi.CREATION_FORMAT.read(s"""
      |  {
      |    "$ZOOKEEPER_CLUSTER_KEY_KEY": {
      |      "group": "g",
      |      "name": "n"
      |    },
      |    "name": "name",
      |    "jmxPort": -1,
      |    "nodeNames": ["n"]
      |  }
      """.stripMargin.parseJson)

  @Test
  def parseLargeJmxPort(): Unit =
    an[DeserializationException] should be thrownBy BrokerApi.CREATION_FORMAT.read(s"""
      |  {
      |    "name": "name",
      |    "jmxPort": 999999,
      |    "nodeNames": ["n"]
      |  }
      """.stripMargin.parseJson)

  @Test
  def parseJmxPortOnUpdate(): Unit = {
    intercept[DeserializationException] {
      BrokerApi.CREATION_FORMAT.read(s"""
           |  {
           |    "$ZOOKEEPER_CLUSTER_KEY_KEY": {
           |      "group": "g",
           |      "name": "n"
           |    },
           |    "nodeNames": [
           |      "node"
           |    ],
           |    "jmxPort": 0
           |  }
      """.stripMargin.parseJson)
    }.getMessage should include("the number must be")

    intercept[DeserializationException] {
      BrokerApi.CREATION_FORMAT.read(s"""
           |  {
           |    "$ZOOKEEPER_CLUSTER_KEY_KEY": {
           |      "group": "g",
           |      "name": "n"
           |    },
           |    "nodeNames": [
           |      "node"
           |    ],
           |    "jmxPort": -9
           |  }
      """.stripMargin.parseJson)
    }.getMessage should include("the number must be")

    intercept[DeserializationException] {
      BrokerApi.CREATION_FORMAT.read(s"""
           |  {
           |    "$ZOOKEEPER_CLUSTER_KEY_KEY": {
           |      "group": "g",
           |      "name": "n"
           |    },
           |    "nodeNames": [
           |      "node"
           |    ],
           |    "jmxPort": 99999
           |  }
      """.stripMargin.parseJson)
    }.getMessage should include("the number must be")
  }

  @Test
  def testInvalidNodeNames(): Unit = {
    an[DeserializationException] should be thrownBy access.nodeName("start").creation
    an[DeserializationException] should be thrownBy access.nodeName("stop").creation
    an[DeserializationException] should be thrownBy access.nodeName("start").updating
    an[DeserializationException] should be thrownBy access.nodeName("stop").updating

    an[DeserializationException] should be thrownBy BrokerApi.CREATION_FORMAT.read(s"""
      |  {
      |    "$ZOOKEEPER_CLUSTER_KEY_KEY": {
      |      "group": "g",
      |      "name": "n"
      |    },
      |    "nodeNames": ["start", "stop"]
      |  }
      """.stripMargin.parseJson)
  }

  @Test
  def testInvalidBrokerClusterKey(): Unit = {
    an[DeserializationException] should be thrownBy BrokerApi.CREATION_FORMAT.read(s"""
      |  {
      |    "$ZOOKEEPER_CLUSTER_KEY_KEY": {
      |      "group": "g",
      |      "name": "n"
      |    },
      |    "zookeeperClusterKey": "",
      |    "nodeNames": ["n1"]
      |  }
      """.stripMargin.parseJson)
  }

  @Test
  def testBrokerClusterKeyWithDefaultGroup(): Unit = {
    val name = CommonUtils.randomString()
    BrokerApi.CREATION_FORMAT.read(s"""
      |  {
      |    "zookeeperClusterKey": "$name",
      |    "nodeNames": ["n1"]
      |  }
      """.stripMargin.parseJson).zookeeperClusterKey shouldBe ObjectKey.of(GROUP_DEFAULT, name)
  }

  @Test
  def testDefaultUpdate(): Unit = {
    val data = access.name(CommonUtils.randomString(10)).updating
    data.imageName.isEmpty shouldBe true
    data.zookeeperClusterKey.isEmpty shouldBe true
    data.jmxPort.isEmpty shouldBe true
    data.clientPort.isEmpty shouldBe true
    data.nodeNames.isEmpty shouldBe true
  }

  @Test
  def testEmptyString(): Unit = {
    an[DeserializationException] should be thrownBy BrokerApi.CREATION_FORMAT.read(s"""
      |  {
      |    "$ZOOKEEPER_CLUSTER_KEY_KEY": {
      |      "group": "g",
      |      "name": "n"
      |    },
      |    "name": "",
      |    "nodeNames": ["a0"]
      |  }
      """.stripMargin.parseJson)

    an[DeserializationException] should be thrownBy BrokerApi.CREATION_FORMAT.read(s"""
      |  {
      |    "name": "name",
      |    "zookeeperClusterKey": "",
      |    "nodeNames": ["a0"]
      |  }
      """.stripMargin.parseJson)

    an[DeserializationException] should be thrownBy BrokerApi.CREATION_FORMAT.read(s"""
      |  {
      |    "name": "name",
      |    "imageName": "",
      |    "nodeNames": ["a0"]
      |  }
      """.stripMargin.parseJson)
  }

  @Test
  def groupShouldAppearInResponse(): Unit = {
    val name = CommonUtils.randomString(5)
    val res = BrokerApi.BROKER_CLUSTER_INFO_FORMAT.write(
      BrokerClusterInfo(
        settings = BrokerApi.access.request
          .name(name)
          .nodeNames(Set("n1"))
          .zookeeperClusterKey(ObjectKey.of("g", "n"))
          .creation
          .raw,
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
  def testTagsOnUpdate(): Unit = access.updating.tags shouldBe None

  @Test
  def testOverwriteSettings(): Unit = {
    val r1 =
      access
        .nodeName("n1")
        .clientPort(12345)
        .jmxPort(45678)
        .zookeeperClusterKey(ObjectKey.of("g", "n"))
        .creation

    val r2 = access
      .nodeName("n1")
      .clientPort(12345)
      .settings(Map("name" -> JsString("fake")))
      .zookeeperClusterKey(ObjectKey.of("g", "n"))
      .creation

    r1.nodeNames shouldBe r2.nodeNames
    r1.clientPort shouldBe r2.clientPort
    // settings will overwrite default value
    r1.name should not be r2.name
  }

  @Test
  def testDeadNodes(): Unit = {
    val cluster = BrokerClusterInfo(
      settings = BrokerApi.access.request
        .nodeNames(Set("n0", "n1"))
        .zookeeperClusterKey(ObjectKey.of("g", "n"))
        .creation
        .raw,
      aliveNodes = Set("n0"),
      state = Some(ClusterState.RUNNING),
      error = None,
      lastModified = CommonUtils.current()
    )
    cluster.deadNodes shouldBe Set("n1")
    cluster.copy(state = None).deadNodes shouldBe Set.empty
  }

  @Test
  def testConnectionProps(): Unit = {
    val cluster = BrokerClusterInfo(
      settings = BrokerApi.access.request
        .nodeNames(Set("n0", "m1"))
        .zookeeperClusterKey(ObjectKey.of("g", "n"))
        .creation
        .raw,
      aliveNodes = Set("nn"),
      state = Some(ClusterState.RUNNING),
      error = None,
      lastModified = CommonUtils.current()
    )
    cluster.connectionProps should not include "nn"
  }

  @Test
  def testZookeeperClusterKey(): Unit = {
    val zkKey = ObjectKey.of(CommonUtils.randomString(10), CommonUtils.randomString(10))
    access.nodeName("n").zookeeperClusterKey(zkKey).creation.zookeeperClusterKey shouldBe zkKey
  }

  @Test
  def defaultValueShouldBeAppendedToResponse(): Unit = {
    val cluster = BrokerClusterInfo(
      settings = BrokerApi.access.request
        .nodeNames(Set("n0", "n1"))
        .zookeeperClusterKey(ObjectKey.of("g", "n"))
        .creation
        .raw,
      aliveNodes = Set("n0"),
      state = Some(ClusterState.RUNNING),
      error = None,
      lastModified = CommonUtils.current()
    )

    val string = BrokerApi.BROKER_CLUSTER_INFO_FORMAT.write(cluster).toString()

    BrokerApi.DEFINITIONS.filter(_.hasDefault).foreach { definition =>
      string should include(definition.key())
      string should include(definition.defaultValue().get().toString)
    }
  }

  @Test
  def checkNameDefinition(): Unit = BrokerApi.DEFINITIONS.find(_.key() == NAME_KEY) should not be None

  @Test
  def checkImageNameDefinition(): Unit = BrokerApi.DEFINITIONS.find(_.key() == IMAGE_NAME_KEY) should not be None

  @Test
  def checkGroupDefinition(): Unit = BrokerApi.DEFINITIONS.find(_.key() == GROUP_KEY) should not be None

  @Test
  def checkNodeNamesDefinition(): Unit = BrokerApi.DEFINITIONS.find(_.key() == NODE_NAMES_KEY) should not be None

  @Test
  def checkTagDefinition(): Unit = BrokerApi.DEFINITIONS.find(_.key() == TAGS_KEY) should not be None

  @Test
  def checkClientPortDefinition(): Unit = BrokerApi.DEFINITIONS.find(_.key() == CLIENT_PORT_KEY) should not be None

  @Test
  def nameDefinitionShouldBeNonUpdatable(): Unit =
    BrokerApi.DEFINITIONS.find(_.key() == NAME_KEY).get.permission() shouldBe Permission.CREATE_ONLY

  @Test
  def groupDefinitionShouldBeNonUpdatable(): Unit =
    BrokerApi.DEFINITIONS.find(_.key() == GROUP_KEY).get.permission() shouldBe Permission.CREATE_ONLY

  @Test
  def testMaxHeap(): Unit =
    BrokerApi.CREATION_FORMAT
      .read(s"""
             |  {
             |    "$ZOOKEEPER_CLUSTER_KEY_KEY": "zk",
             |    "nodeNames": ["node00"],
             |    "xmx": 123
             |  }
      """.stripMargin.parseJson)
      .maxHeap shouldBe 123

  @Test
  def testInitHeap(): Unit =
    BrokerApi.CREATION_FORMAT
      .read(s"""
              |  {
              |    "$ZOOKEEPER_CLUSTER_KEY_KEY": "zk",
              |    "nodeNames": ["node00"],
              |    "xms": 123
              |  }
      """.stripMargin.parseJson)
      .initHeap shouldBe 123

  @Test
  def testNegativeMaxHeap(): Unit =
    an[DeserializationException] should be thrownBy BrokerApi.CREATION_FORMAT.read(s"""
                                                                                         |  {
                                                                                         |    "$ZOOKEEPER_CLUSTER_KEY_KEY": "zk",
                                                                                         |    "nodeNames": ["node00"],
                                                                                         |    "xmx": -123
                                                                                         |  }
      """.stripMargin.parseJson)

  @Test
  def testNegativeInitHeap(): Unit =
    an[DeserializationException] should be thrownBy BrokerApi.CREATION_FORMAT.read(s"""
                                                                                                                            |  {
                                                                                                                            |    "nodeNames": ["node00"],
                                                                                                                            |    "xms": -123
                                                                                                                            |  }
      """.stripMargin.parseJson)

  @Test
  def settingsDisappearFromJson(): Unit = {
    val cluster = BrokerClusterInfo(
      settings = BrokerApi.access.request.zookeeperClusterKey(ObjectKey.of("a", "b")).nodeName("aa").creation.raw,
      aliveNodes = Set.empty,
      state = None,
      error = None,
      lastModified = CommonUtils.current()
    )
    BrokerApi.BROKER_CLUSTER_INFO_FORMAT.write(cluster).asJsObject.fields.keySet should not contain ("settings")
  }

  @Test
  def testInfoJsonRepresentation(): Unit = {
    val cluster = BrokerClusterInfo(
      settings = BrokerApi.access.request.zookeeperClusterKey(ObjectKey.of("a", "b")).nodeName("aa").creation.raw,
      aliveNodes = Set.empty,
      state = None,
      error = None,
      lastModified = CommonUtils.current()
    )
    BrokerApi.BROKER_CLUSTER_INFO_FORMAT.read(BrokerApi.BROKER_CLUSTER_INFO_FORMAT.write(cluster)) shouldBe cluster
  }

  @Test
  def testDataDir(): Unit = {
    val creation = BrokerApi.CREATION_FORMAT.read(s"""
                                                             |  {
                                                             |    "$ZOOKEEPER_CLUSTER_KEY_KEY": "zk",
                                                             |    "nodeNames": ["node00"],
                                                             |    "${BrokerApi.LOG_DIRS_KEY}": [
                                                             |      {
                                                             |        "group": "g",
                                                             |        "name": "n"
                                                             |      },
                                                             |      {
                                                             |        "group": "g2",
                                                             |        "name": "n"
                                                             |      }
                                                             |    ]
                                                             |  }
      """.stripMargin.parseJson)
    creation.volumeMaps.size shouldBe 2
    creation.dataFolders.contains(creation.volumeMaps(ObjectKey.of("g", "n")))
    creation.dataFolders.contains(creation.volumeMaps(ObjectKey.of("g2", "n")))
  }

  @Test
  def userDefinedStateShouldBeRemoveFromCreation(): Unit =
    BrokerApi.CREATION_FORMAT.read(s"""
                                      |  {
                                      |    "$ZOOKEEPER_CLUSTER_KEY_KEY": "zk",
                                      |    "nodeNames": ["node00"],
                                      |    "state": "RUNNING"
                                      |  }
      """.stripMargin.parseJson).raw.get("state") shouldBe None

  @Test
  def userDefinedStateShouldBeRemoveFromUpdating(): Unit =
    BrokerApi.UPDATING_FORMAT.read(s"""
                                      |  {
                                      |    "$ZOOKEEPER_CLUSTER_KEY_KEY": "zk",
                                      |    "nodeNames": ["node00"],
                                      |    "state": "RUNNING"
                                      |  }
      """.stripMargin.parseJson).raw.get("state") shouldBe None
}
