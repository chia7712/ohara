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

import oharastream.ohara.client.configurator.TopicApi._
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.setting.{ObjectKey, SettingDef}
import oharastream.ohara.common.util.CommonUtils
import org.junit.jupiter.api.Test
import org.scalatest.matchers.should.Matchers._
import spray.json._

import scala.concurrent.ExecutionContext.Implicits.global
class TestTopicApi extends OharaTest {
  @Test
  def nullKeyInGet(): Unit =
    an[NullPointerException] should be thrownBy TopicApi.access.get(null)

  @Test
  def nullKeyInDelete(): Unit =
    an[NullPointerException] should be thrownBy TopicApi.access.delete(null)

  @Test
  def emptyGroup(): Unit = an[IllegalArgumentException] should be thrownBy TopicApi.access.request.group("")

  @Test
  def nullGroup(): Unit = an[NullPointerException] should be thrownBy TopicApi.access.request.group(null)

  @Test
  def brokerClusterKeyShouldBeRequired(): Unit =
    intercept[DeserializationException] {
      TopicApi.access.hostname(CommonUtils.randomString(10)).port(CommonUtils.availablePort()).request.creation
    }.getMessage should include(TopicApi.BROKER_CLUSTER_KEY_KEY)

  @Test
  def ignoreNameOnCreation(): Unit =
    TopicApi.access
      .hostname(CommonUtils.randomString(10))
      .port(CommonUtils.availablePort())
      .request
      .brokerClusterKey(ObjectKey.of("fake", "fake"))
      .creation
      .name
      .length should not be 0

  @Test
  def ignoreNameOnUpdate(): Unit =
    an[NoSuchElementException] should be thrownBy TopicApi.access
      .hostname(CommonUtils.randomString(10))
      .port(CommonUtils.availablePort())
      .request
      .update()

  @Test
  def emptyName(): Unit = an[IllegalArgumentException] should be thrownBy TopicApi.access.request.name("")

  @Test
  def nullName(): Unit = an[NullPointerException] should be thrownBy TopicApi.access.request.name(null)

  @Test
  def nullBrokerClusterKey(): Unit =
    an[NullPointerException] should be thrownBy TopicApi.access.request.brokerClusterKey(null)

  @Test
  def negativeNumberOfPartitions(): Unit =
    an[IllegalArgumentException] should be thrownBy TopicApi.access.request.numberOfPartitions(-1)

  @Test
  def negativeNumberOfReplications(): Unit =
    an[IllegalArgumentException] should be thrownBy TopicApi.access.request.numberOfReplications(-1)

  @Test
  def parseCreation(): Unit = {
    val brokerClusterName    = CommonUtils.randomString(10)
    val numberOfPartitions   = 100
    val numberOfReplications = 10
    val group                = CommonUtils.randomString(10)
    val name                 = CommonUtils.randomString(10)
    val creation             = TopicApi.CREATION_FORMAT.read(s"""
         |{
         | "$GROUP_KEY": "$group",
         | "$NAME_KEY": "$name",
         | "$BROKER_CLUSTER_KEY_KEY": "$brokerClusterName",
         | "${NUMBER_OF_PARTITIONS_KEY}": $numberOfPartitions,
         | "${NUMBER_OF_REPLICATIONS_KEY}": $numberOfReplications
         |}
       """.stripMargin.parseJson)

    creation.group shouldBe group
    creation.name shouldBe name
    creation.brokerClusterKey.name() shouldBe brokerClusterName
    creation.numberOfPartitions shouldBe numberOfPartitions
    creation.numberOfReplications shouldBe numberOfReplications
  }

  @Test
  def parseJsonForUpdate(): Unit = {
    val name                 = CommonUtils.randomString(10)
    val brokerClusterName    = CommonUtils.randomString(10)
    val numberOfPartitions   = 100
    val numberOfReplications = 10
    val update               = TopicApi.UPDATING_FORMAT.read(s"""
      |{
      | "$NAME_KEY": "$name",
      | "$BROKER_CLUSTER_KEY_KEY": "$brokerClusterName",
      | "${NUMBER_OF_PARTITIONS_KEY}": $numberOfPartitions,
      | "${NUMBER_OF_REPLICATIONS_KEY}": $numberOfReplications
      |}
       """.stripMargin.parseJson)

    update.brokerClusterKey.get.name() shouldBe brokerClusterName
    update.raw(NUMBER_OF_PARTITIONS_KEY) shouldBe JsNumber(numberOfPartitions)
    update.raw(NUMBER_OF_REPLICATIONS_KEY) shouldBe JsNumber(numberOfReplications)

    val update2 = TopicApi.UPDATING_FORMAT.read(s"""
         |{
         |}
       """.stripMargin.parseJson)

    update2.brokerClusterKey shouldBe None
    update2.raw should not contain NUMBER_OF_PARTITIONS_KEY
    update2.raw should not contain NUMBER_OF_REPLICATIONS_KEY
  }

  @Test
  def nullTags(): Unit = an[NullPointerException] should be thrownBy TopicApi.access.request.tags(null)

  @Test
  def emptyTags(): Unit = TopicApi.access.request.tags(Map.empty)

  @Test
  def testNameLimit(): Unit =
    an[DeserializationException] should be thrownBy
      TopicApi.access
        .hostname(CommonUtils.randomString(10))
        .port(CommonUtils.availablePort())
        .request
        .name(CommonUtils.randomString(SettingDef.STRING_LENGTH_LIMIT))
        .group(CommonUtils.randomString(SettingDef.STRING_LENGTH_LIMIT))
        .creation

  @Test
  def negativeReplicationsIsIllegalInCreation(): Unit =
    intercept[DeserializationException] {
      TopicApi.CREATION_FORMAT.read(s"""
                                     |  {
                                     |    "$BROKER_CLUSTER_KEY_KEY": {
                                     |      "group": "g",
                                     |      "name": "n"
                                     |    },
                                     |    "${NUMBER_OF_REPLICATIONS_KEY}": -1
                                     |  }
       """.stripMargin.parseJson)
    }.getMessage should include("the number must")

  @Test
  def negativePartitionsIsIllegalInCreation(): Unit =
    intercept[DeserializationException] {
      TopicApi.CREATION_FORMAT.read(s"""
                                           |  {
                                           |    "$BROKER_CLUSTER_KEY_KEY": {
                                           |      "group": "g",
                                           |      "name": "n"
                                           |    },
                                           |    "${NUMBER_OF_PARTITIONS_KEY}": -1
                                           |  }
       """.stripMargin.parseJson)
    }.getMessage should include("the number must")

  @Test
  def zeroReplicationsIsIllegalInCreation(): Unit =
    intercept[DeserializationException] {
      TopicApi.CREATION_FORMAT.read(s"""
                                           |  {
                                           |    "$BROKER_CLUSTER_KEY_KEY": {
                                           |      "group": "g",
                                           |      "name": "n"
                                           |    },
                                           |    "${NUMBER_OF_REPLICATIONS_KEY}": 0
                                           |  }
       """.stripMargin.parseJson)
    }.getMessage should include("the number must")

  @Test
  def zeroPartitionsIsIllegalInCreation(): Unit =
    intercept[DeserializationException] {
      TopicApi.CREATION_FORMAT.read(s"""
                                           |  {
                                           |    "$BROKER_CLUSTER_KEY_KEY": {
                                           |      "group": "g",
                                           |      "name": "n"
                                           |    },
                                           |    "${NUMBER_OF_PARTITIONS_KEY}": 0
                                           |  }
       """.stripMargin.parseJson)
    }.getMessage should include("the number must")

  @Test
  def settingsDisappearFromJson(): Unit = {
    val cluster = TopicInfo(
      settings = TopicApi.access.request.brokerClusterKey(ObjectKey.of("a", "b")).creation.raw,
      partitionInfos = Seq.empty,
      state = None,
      nodeMetrics = Map.empty,
      lastModified = CommonUtils.current()
    )
    TopicApi.TOPIC_INFO_FORMAT.write(cluster).asJsObject.fields.keySet should not contain ("settings")
  }

  @Test
  def testInfoJsonRepresentation(): Unit = {
    val cluster = TopicInfo(
      settings = TopicApi.access.request.brokerClusterKey(ObjectKey.of("a", "b")).creation.raw,
      partitionInfos = Seq.empty,
      state = None,
      nodeMetrics = Map.empty,
      lastModified = CommonUtils.current()
    )
    TopicApi.TOPIC_INFO_FORMAT.read(TopicApi.TOPIC_INFO_FORMAT.write(cluster)) shouldBe cluster
  }

  @Test
  def userDefinedStateShouldBeRemoveFromCreation(): Unit =
    TopicApi.CREATION_FORMAT.read(s"""
                                     |  {
                                     |    "$BROKER_CLUSTER_KEY_KEY": {
                                     |      "group": "g",
                                     |      "name": "n"
                                     |    },
                                     |    "state": "RUNNING"
                                     |  }
      """.stripMargin.parseJson).raw.get("state") shouldBe None

  @Test
  def userDefinedStateShouldBeRemoveFromUpdating(): Unit =
    TopicApi.UPDATING_FORMAT.read(s"""
                                     |  {
                                     |    "$BROKER_CLUSTER_KEY_KEY": {
                                     |      "group": "g",
                                     |      "name": "n"
                                     |    },
                                     |    "state": "RUNNING"
                                     |  }
      """.stripMargin.parseJson).raw.get("state") shouldBe None
}
