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

import oharastream.ohara.client.configurator.ZookeeperApi.ZookeeperClusterInfo
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.setting.{ObjectKey, SettingDef}
import oharastream.ohara.common.setting.SettingDef.Permission
import oharastream.ohara.common.util.CommonUtils
import org.junit.jupiter.api.Test
import org.scalatest.matchers.should.Matchers._
import spray.json.DefaultJsonProtocol._
import spray.json._
class TestZookeeperApi extends OharaTest {
  private[this] final val access =
    ZookeeperApi.access.hostname(CommonUtils.randomString(5)).port(CommonUtils.availablePort()).request

  @Test
  def testClone(): Unit = {
    val nodeNames = Set(CommonUtils.randomString())
    val zookeeperClusterInfo = ZookeeperClusterInfo(
      settings = access.nodeNames(Set(CommonUtils.randomString())).creation.raw,
      aliveNodes = Set.empty,
      state = None,
      error = None,
      lastModified = CommonUtils.current()
    )
    zookeeperClusterInfo.newNodeNames(nodeNames).nodeNames shouldBe nodeNames
  }

  @Test
  def ignoreNameOnCreation(): Unit = access.nodeName(CommonUtils.randomString(10)).creation.name.length should not be 0

  @Test
  def testTags(): Unit =
    access
      .nodeName(CommonUtils.randomString(10))
      .tags(Map("a" -> JsNumber(1), "b" -> JsString("2")))
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
  def nullNodeNames(): Unit = {
    an[NullPointerException] should be thrownBy access.nodeNames(null)
    an[IllegalArgumentException] should be thrownBy access.nodeNames(Set.empty)
  }

  @Test
  def emptyNodeNames(): Unit = an[IllegalArgumentException] should be thrownBy access.nodeNames(Set.empty)

  @Test
  def negativeClientPort(): Unit = an[IllegalArgumentException] should be thrownBy access.clientPort(-1)

  @Test
  def negativeElectionPort(): Unit = an[IllegalArgumentException] should be thrownBy access.electionPort(-1)

  @Test
  def negativePeerPort(): Unit = an[IllegalArgumentException] should be thrownBy access.peerPort(-1)

  @Test
  def testCreation(): Unit = {
    val name         = CommonUtils.randomString(10)
    val group        = CommonUtils.randomString(10)
    val jmxPort      = CommonUtils.availablePort()
    val clientPort   = CommonUtils.availablePort()
    val peerPort     = CommonUtils.availablePort()
    val electionPort = CommonUtils.availablePort()
    val nodeName     = CommonUtils.randomString()
    val creation = access
      .name(name)
      .group(group)
      .jmxPort(jmxPort)
      .clientPort(clientPort)
      .peerPort(peerPort)
      .electionPort(electionPort)
      .nodeName(nodeName)
      .creation
    creation.name shouldBe name
    creation.group shouldBe group
    creation.jmxPort shouldBe jmxPort
    creation.clientPort shouldBe clientPort
    creation.peerPort shouldBe peerPort
    creation.electionPort shouldBe electionPort
    creation.nodeNames.head shouldBe nodeName
  }

  @Test
  def testExtraSettingInCreation(): Unit = {
    val name     = CommonUtils.randomString(10)
    val name2    = JsString(CommonUtils.randomString(10))
    val creation = access.name(name).nodeNames(Set("n1")).settings(Map("name" -> name2)).creation

    // settings() has higher priority than name()
    creation.name shouldBe name2.value
  }

  @Test
  def parseCreation(): Unit = {
    val nodeName = "n1"
    val creation = ZookeeperApi.CREATION_FORMAT.read(s"""
      |  {
      |    "nodeNames": ["$nodeName"]
      |  }
      """.stripMargin.parseJson)

    creation.group shouldBe GROUP_DEFAULT
    creation.name.length shouldBe SettingDef.STRING_LENGTH_LIMIT
    creation.nodeNames.size shouldBe 1
    creation.nodeNames.head shouldBe nodeName
    creation.imageName shouldBe ZookeeperApi.IMAGE_NAME_DEFAULT
    creation.clientPort should not be 0
    creation.electionPort should not be 0
    creation.peerPort should not be 0

    val name      = CommonUtils.randomString(10)
    val group     = CommonUtils.randomString(10)
    val creation2 = ZookeeperApi.CREATION_FORMAT.read(s"""
      |  {
      |    "group": "$group",
      |    "name": "$name",
      |    "nodeNames": ["$nodeName"]
      |  }
      """.stripMargin.parseJson)
    // group is support in create cluster
    creation2.group shouldBe group
    creation2.name shouldBe name
    creation2.nodeNames.size shouldBe 1
    creation2.nodeNames.head shouldBe nodeName
    creation2.imageName shouldBe ZookeeperApi.IMAGE_NAME_DEFAULT
    creation2.clientPort should not be 0
    creation2.electionPort should not be 0
    creation2.peerPort should not be 0
  }

  @Test
  def testUpdate(): Unit = {
    val name       = CommonUtils.randomString(10)
    val group      = CommonUtils.randomString(10)
    val clientPort = CommonUtils.availablePort()
    val nodeName   = CommonUtils.randomString()

    val creation = access.name(name).nodeName(nodeName).creation
    creation.name shouldBe name
    // use default values if absent
    creation.group shouldBe GROUP_DEFAULT
    creation.imageName shouldBe ZookeeperApi.IMAGE_NAME_DEFAULT
    creation.nodeNames shouldBe Set(nodeName)

    // initial a new update request
    val updateAsCreation = ZookeeperApi.access.request
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
  def testDefaultName(): Unit = ZookeeperApi.CREATION_FORMAT.read(s"""
      |  {
      |    "nodeNames": ["n1"]
      |  }
      """.stripMargin.parseJson).name.nonEmpty shouldBe true

  @Test
  def parseNameField(): Unit =
    intercept[DeserializationException] {
      ZookeeperApi.CREATION_FORMAT.read(s"""
           |  {
           |    "nodeNames": [
           |      "node"
           |    ],
           |    "name": ""
           |  }
           |  """.stripMargin.parseJson)
    }.getMessage should include("the value of \"name\" can't be empty string")

  @Test
  def parseImageNameField(): Unit =
    ZookeeperApi.CREATION_FORMAT
      .read(s"""
              |  {
              |    "nodeNames": [
              |      "node"
              |    ],
              |    "imageName": "${ZookeeperApi.IMAGE_NAME_DEFAULT}"
              |  }
              |  """.stripMargin.parseJson)
      .raw(IMAGE_NAME_KEY)
      .convertTo[String] shouldBe ZookeeperApi.IMAGE_NAME_DEFAULT

  @Test
  def emptyNodeNamesShouldPassInUpdating(): Unit = {
    ZookeeperApi.UPDATING_FORMAT.read(s"""
                                           |  {
                                           |    "nodeNames": []
                                           |  }
                                           |  """.stripMargin.parseJson).nodeNames shouldBe Some(Set.empty)
  }

  @Test
  def parseImageNameOnUpdate(): Unit =
    intercept[DeserializationException] {
      ZookeeperApi.UPDATING_FORMAT.read(s"""
           |  {
           |    "imageName": ""
           |  }
           """.stripMargin.parseJson)
    }.getMessage should include("the value of \"imageName\" can't be empty string")

  @Test
  def parseEmptyNodeNames(): Unit =
    an[DeserializationException] should be thrownBy ZookeeperApi.CREATION_FORMAT.read(s"""
         |  {
         |    "name": "name",
         |    "nodeNames": []
         |  }
           """.stripMargin.parseJson)

  @Test
  def parseNodeNamesOnUpdate(): Unit =
    intercept[DeserializationException] {
      ZookeeperApi.UPDATING_FORMAT.read(s"""
           |  {
           |    "nodeNames": ""
           |  }
           """.stripMargin.parseJson)
    }.getMessage should include("the value of \"nodeNames\" can't be empty string")

  @Test
  def parseZeroClientPort(): Unit =
    an[DeserializationException] should be thrownBy ZookeeperApi.CREATION_FORMAT.read(s"""
         |  {
         |    "name": "name",
         |    "clientPort": 0,
         |    "nodeNames": ["n"]
         |  }
           """.stripMargin.parseJson)

  @Test
  def parseNegativeClientPort(): Unit =
    an[DeserializationException] should be thrownBy ZookeeperApi.CREATION_FORMAT.read(s"""
         |  {
         |    "name": "name",
         |    "clientPort": -1,
         |    "nodeNames": ["n"]
         |  }
           """.stripMargin.parseJson)

  @Test
  def parseLargeClientPort(): Unit =
    an[DeserializationException] should be thrownBy ZookeeperApi.CREATION_FORMAT.read(s"""
         |  {
         |    "name": "name",
         |    "clientPort": 999999,
         |    "nodeNames": ["n"]
         |  }
           """.stripMargin.parseJson)

  @Test
  def parseZeroElectionPort(): Unit =
    an[DeserializationException] should be thrownBy ZookeeperApi.CREATION_FORMAT.read(s"""
         |  {
         |    "name": "name",
         |    "electionPort": 0,
         |    "nodeNames": ["n"]
         |  }
           """.stripMargin.parseJson)

  @Test
  def parseNegativeElectionPort(): Unit =
    an[DeserializationException] should be thrownBy ZookeeperApi.CREATION_FORMAT.read(s"""
         |  {
         |    "name": "name",
         |    "electionPort": -1,
         |    "nodeNames": ["n"]
         |  }
           """.stripMargin.parseJson)

  @Test
  def parseLargeElectionPort(): Unit =
    an[DeserializationException] should be thrownBy ZookeeperApi.CREATION_FORMAT.read(s"""
         |  {
         |    "name": "name",
         |    "electionPort": 999999,
         |    "nodeNames": ["n"]
         |  }
           """.stripMargin.parseJson)

  @Test
  def parseZeroPeerPort(): Unit =
    an[DeserializationException] should be thrownBy ZookeeperApi.CREATION_FORMAT.read(s"""
         |  {
         |    "name": "name",
         |    "peerPort": 0,
         |    "nodeNames": ["n"]
         |  }
           """.stripMargin.parseJson)

  @Test
  def parseNegativePeerPort(): Unit =
    an[DeserializationException] should be thrownBy ZookeeperApi.CREATION_FORMAT.read(s"""
         |  {
         |    "name": "name",
         |    "peerPort": -1,
         |    "nodeNames": ["n"]
         |  }
           """.stripMargin.parseJson)

  @Test
  def parseLargePeerPort(): Unit =
    an[DeserializationException] should be thrownBy ZookeeperApi.CREATION_FORMAT.read(s"""
         |  {
         |    "name": "name",
         |    "peerPort": 999999,
         |    "nodeNames": ["n"]
         |  }
           """.stripMargin.parseJson)

  @Test
  def testInvalidNodeNames(): Unit = {
    an[DeserializationException] should be thrownBy access.nodeName("start").creation
    an[DeserializationException] should be thrownBy access.nodeName("stop").creation
    an[DeserializationException] should be thrownBy access.nodeName("start").updating
    an[DeserializationException] should be thrownBy access.nodeName("stop").updating

    an[DeserializationException] should be thrownBy ZookeeperApi.CREATION_FORMAT.read(s"""
      |  {
      |    "nodeNames": ["start", "stop"]
      |  }
      """.stripMargin.parseJson)
  }

  @Test
  def testDefaultUpdate(): Unit = {
    val name = CommonUtils.randomString(10)
    val data = access.name(name).updating
    data.imageName.isEmpty shouldBe true
    data.peerPort.isEmpty shouldBe true
    data.electionPort.isEmpty shouldBe true
    data.clientPort.isEmpty shouldBe true
    data.nodeNames.isEmpty shouldBe true
  }

  @Test
  def groupShouldAppearInResponse(): Unit = {
    val name = CommonUtils.randomString(5)
    val res = ZookeeperApi.ZOOKEEPER_CLUSTER_INFO_FORMAT.write(
      ZookeeperClusterInfo(
        settings = ZookeeperApi.access.request.name(name).nodeNames(Set("n1")).creation.raw,
        aliveNodes = Set.empty,
        state = None,
        error = None,
        lastModified = CommonUtils.current()
      )
    )
    // serialize to json should see the object key (group, name) in "settings"
    res.asJsObject.fields(NAME_KEY).convertTo[String] shouldBe name
    res.asJsObject.fields(GROUP_KEY).convertTo[String] shouldBe GROUP_DEFAULT
  }

  @Test
  def testTagsOnUpdate(): Unit = access.updating.tags shouldBe None

  @Test
  def testOverwriteSettings(): Unit = {
    val r1 =
      access.nodeName("n1").clientPort(12345).peerPort(45678).creation

    val r2 = access.nodeName("n1").clientPort(12345).settings(Map("name" -> JsString("fake"))).creation

    r1.nodeNames shouldBe r2.nodeNames
    r1.clientPort shouldBe r2.clientPort
    // settings will overwrite default value
    r1.name should not be r2.name
  }

  @Test
  def testDeadNodes(): Unit = {
    val cluster = ZookeeperClusterInfo(
      settings = ZookeeperApi.access.request.nodeNames(Set("n0", "n1")).creation.raw,
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
  def defaultValueShouldBeAppendedToResponse(): Unit = {
    val cluster = ZookeeperClusterInfo(
      settings = ZookeeperApi.access.request.nodeNames(Set("n0", "n1")).creation.raw,
      aliveNodes = Set("n0"),
      state = Some(ClusterState.RUNNING),
      error = None,
      lastModified = CommonUtils.current()
    )

    val string = ZookeeperApi.ZOOKEEPER_CLUSTER_INFO_FORMAT.write(cluster).toString()

    ZookeeperApi.DEFINITIONS.filter(_.hasDefault).foreach { definition =>
      string should include(definition.key())
      string should include(definition.defaultValue().get().toString)
    }
  }

  @Test
  def checkNameDefinition(): Unit = ZookeeperApi.DEFINITIONS.find(_.key() == NAME_KEY) should not be None

  @Test
  def nameDefinitionShouldBeNonUpdatable(): Unit =
    ZookeeperApi.DEFINITIONS.find(_.key() == NAME_KEY).get.permission() shouldBe Permission.CREATE_ONLY

  @Test
  def groupDefinitionShouldBeNonUpdatable(): Unit =
    ZookeeperApi.DEFINITIONS.find(_.key() == GROUP_KEY).get.permission() shouldBe Permission.CREATE_ONLY

  @Test
  def checkImageNameDefinition(): Unit = ZookeeperApi.DEFINITIONS.find(_.key() == IMAGE_NAME_KEY) should not be None

  @Test
  def checkGroupDefinition(): Unit = ZookeeperApi.DEFINITIONS.find(_.key() == GROUP_KEY) should not be None

  @Test
  def checkNodeNamesDefinition(): Unit = ZookeeperApi.DEFINITIONS.find(_.key() == NODE_NAMES_KEY) should not be None

  @Test
  def checkTagDefinition(): Unit = ZookeeperApi.DEFINITIONS.find(_.key() == TAGS_KEY) should not be None

  @Test
  def checkClientPortDefinition(): Unit = ZookeeperApi.DEFINITIONS.find(_.key() == CLIENT_PORT_KEY) should not be None

  @Test
  def testMaxHeap(): Unit = ZookeeperApi.CREATION_FORMAT.read(s"""
                                                                |  {
                                                                |    "nodeNames": ["node00"],
                                                                |    "xmx": 123
                                                                |  }
      """.stripMargin.parseJson).maxHeap shouldBe 123

  @Test
  def testInitHeap(): Unit = ZookeeperApi.CREATION_FORMAT.read(s"""
                                                                |  {
                                                                |    "nodeNames": ["node00"],
                                                                |    "xms": 123
                                                                |  }
      """.stripMargin.parseJson).initHeap shouldBe 123

  @Test
  def testNegativeMaxHeap(): Unit =
    an[DeserializationException] should be thrownBy ZookeeperApi.CREATION_FORMAT.read(s"""
                                                                                            |  {
                                                                                            |    "nodeNames": ["node00"],
                                                                                            |    "xmx": -123
                                                                                            |  }
      """.stripMargin.parseJson)

  @Test
  def testNegativeInitHeap(): Unit =
    an[DeserializationException] should be thrownBy ZookeeperApi.CREATION_FORMAT.read(s"""
                                                                                            |  {
                                                                                            |    "nodeNames": ["node00"],
                                                                                            |    "xms": -123
                                                                                            |  }
      """.stripMargin.parseJson)

  @Test
  def settingsDisappearFromJson(): Unit = {
    val cluster = ZookeeperClusterInfo(
      settings = ZookeeperApi.access.request.nodeNames(Set("n0", "n1")).creation.raw,
      aliveNodes = Set("n0"),
      state = Some(ClusterState.RUNNING),
      error = None,
      lastModified = CommonUtils.current()
    )
    ZookeeperApi.ZOOKEEPER_CLUSTER_INFO_FORMAT.write(cluster).asJsObject.fields.keySet should not contain "settings"
  }

  @Test
  def testInfoJsonRepresentation(): Unit = {
    val cluster = ZookeeperClusterInfo(
      settings = ZookeeperApi.access.request.nodeNames(Set("n0", "n1")).creation.raw,
      aliveNodes = Set("n0"),
      state = Some(ClusterState.RUNNING),
      error = None,
      lastModified = CommonUtils.current()
    )
    ZookeeperApi.ZOOKEEPER_CLUSTER_INFO_FORMAT.read(ZookeeperApi.ZOOKEEPER_CLUSTER_INFO_FORMAT.write(cluster)) shouldBe cluster
  }

  @Test
  def testDataDir(): Unit = {
    val creation = ZookeeperApi.CREATION_FORMAT.read(s"""
                                                       |  {
                                                       |    "nodeNames": ["node00"],
                                                       |    "${ZookeeperApi.DATA_DIR_KEY}": {
                                                       |      "group": "g",
                                                       |      "name": "n"
                                                       |    }
                                                       |  }
      """.stripMargin.parseJson)

    creation.volumeMaps.size shouldBe 1
    creation.volumeMaps.head._1 shouldBe ObjectKey.of("g", "n")
    creation.volumeMaps.head._2 shouldBe creation.dataFolder
  }

  @Test
  def testConnectionTimeout(): Unit =
    ZookeeperApi.CREATION_FORMAT.read(s"""
                                        |  {
                                        |    "nodeNames": ["node00"],
                                        |    "${ZookeeperApi.DATA_DIR_KEY}": {
                                        |      "group": "g",
                                        |      "name": "n"
                                        |    }
                                        |  }
      """.stripMargin.parseJson).connectionTimeout.toMillis should not be 0

  @Test
  def userDefinedStateShouldBeRemoveFromCreation(): Unit =
    ZookeeperApi.CREATION_FORMAT.read(s"""
                                         |  {
                                         |    "nodeNames": ["node00"],
                                         |    "${ZookeeperApi.DATA_DIR_KEY}": {
                                         |      "group": "g",
                                         |      "name": "n"
                                         |    },
                                         |    "state": "RUNNING"
                                         |  }
      """.stripMargin.parseJson).raw.get("state") shouldBe None

  @Test
  def userDefinedStateShouldBeRemoveFromUpdating(): Unit =
    ZookeeperApi.UPDATING_FORMAT.read(s"""
                                         |  {
                                         |    "nodeNames": ["node00"],
                                         |    "${ZookeeperApi.DATA_DIR_KEY}": {
                                         |      "group": "g",
                                         |      "name": "n"
                                         |    },
                                         |    "state": "RUNNING"
                                         |  }
      """.stripMargin.parseJson).raw.get("state") shouldBe None
}
