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

package oharastream.ohara.agent

import java.util.concurrent.TimeUnit

import oharastream.ohara.client.configurator.WorkerApi
import oharastream.ohara.client.configurator.WorkerApi.WorkerClusterInfo
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.setting.{ObjectKey, SettingDef}
import oharastream.ohara.common.util.CommonUtils
import org.junit.jupiter.api.Test
import org.scalatest.matchers.should.Matchers._
import spray.json.DeserializationException

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
class TestWorkerCreator extends OharaTest {
  private[this] def wkCreator(): WorkerCollie.ClusterCreator = (executionContext, creation) => {
    // the inputs have been checked (NullPointerException). Hence, we throw another exception here.
    if (executionContext == null) throw new AssertionError()
    Future.successful(
      WorkerClusterInfo(
        settings = creation.raw,
        aliveNodes = Set.empty,
        state = None,
        error = None,
        lastModified = 0
      )
    )
  }

  @Test
  def nullClusterName(): Unit = {
    an[NullPointerException] should be thrownBy wkCreator().name(null)
  }

  @Test
  def emptyClusterName(): Unit = {
    an[IllegalArgumentException] should be thrownBy wkCreator().name("")
  }

  @Test
  def nullGroup(): Unit = {
    an[NullPointerException] should be thrownBy wkCreator().group(null)
  }

  @Test
  def emptyGroup(): Unit = {
    an[IllegalArgumentException] should be thrownBy wkCreator().group("")
  }

  @Test
  def negativeClientPort(): Unit = {
    an[IllegalArgumentException] should be thrownBy wkCreator().clientPort(-1)
  }

  @Test
  def negativeJmxPort(): Unit = {
    an[IllegalArgumentException] should be thrownBy wkCreator().jmxPort(-1)
  }

  @Test
  def nullBkClusterKey(): Unit = {
    an[NullPointerException] should be thrownBy wkCreator().brokerClusterKey(null)
  }

  @Test
  def nullGroupId(): Unit = {
    an[NullPointerException] should be thrownBy wkCreator().groupId(null)
  }

  @Test
  def emptyGroupId(): Unit = {
    an[IllegalArgumentException] should be thrownBy wkCreator().groupId("")
  }

  @Test
  def nullConfigTopicName(): Unit = {
    an[NullPointerException] should be thrownBy wkCreator().configTopicName(null)
  }

  @Test
  def emptyConfigTopicName(): Unit = {
    an[IllegalArgumentException] should be thrownBy wkCreator().configTopicName("")
  }

  @Test
  def negativeConfigTopicReplications(): Unit = {
    an[IllegalArgumentException] should be thrownBy wkCreator().configTopicReplications(-1)
  }

  @Test
  def nullStatusTopicName(): Unit = {
    an[NullPointerException] should be thrownBy wkCreator().statusTopicName(null)
  }

  @Test
  def emptyStatusTopicName(): Unit = {
    an[IllegalArgumentException] should be thrownBy wkCreator().statusTopicName("")
  }
  @Test
  def negativeStatusTopicPartitions(): Unit = {
    an[IllegalArgumentException] should be thrownBy wkCreator().statusTopicPartitions(-1)
  }
  @Test
  def negativeStatusTopicReplications(): Unit = {
    an[IllegalArgumentException] should be thrownBy wkCreator().statusTopicReplications(-1)
  }

  @Test
  def nullOffsetTopicName(): Unit = {
    an[NullPointerException] should be thrownBy wkCreator().offsetTopicName(null)
  }

  @Test
  def emptyOffsetTopicName(): Unit = {
    an[IllegalArgumentException] should be thrownBy wkCreator().offsetTopicName("")
  }
  @Test
  def negativeOffsetTopicPartitions(): Unit = {
    an[IllegalArgumentException] should be thrownBy wkCreator().offsetTopicPartitions(-1)
  }
  @Test
  def negativeOffsetTopicReplications(): Unit = {
    an[IllegalArgumentException] should be thrownBy wkCreator().offsetTopicReplications(-1)
  }

  @Test
  def nullNodes(): Unit = {
    an[NullPointerException] should be thrownBy wkCreator().nodeNames(null)
  }

  @Test
  def emptyNodes(): Unit = {
    an[IllegalArgumentException] should be thrownBy wkCreator().nodeNames(Set.empty)
  }

  @Test
  def testNameLength(): Unit =
    wkCreator()
      .name(CommonUtils.randomString(10))
      .group(CommonUtils.randomString(10))
      .brokerClusterKey(ObjectKey.of("default", "bk"))
      .clientPort(CommonUtils.availablePort())
      .jmxPort(8084)
      .groupId(CommonUtils.randomString(10))
      .configTopicName(CommonUtils.randomString(10))
      .configTopicReplications(1)
      .statusTopicName(CommonUtils.randomString(10))
      .statusTopicPartitions(1)
      .statusTopicReplications(1)
      .offsetTopicName(CommonUtils.randomString(10))
      .offsetTopicPartitions(1)
      .offsetTopicReplications(1)
      .nodeName(CommonUtils.randomString(10))
      .create()

  @Test
  def testInvalidName(): Unit =
    an[DeserializationException] should be thrownBy wkCreator()
      .name(CommonUtils.randomString(SettingDef.STRING_LENGTH_LIMIT))
      .group(CommonUtils.randomString(10))
      .nodeName(CommonUtils.randomString(10))
      .create()

  @Test
  def testMinimumCreator(): Unit = Await.result(
    wkCreator()
      .name(CommonUtils.randomString(10))
      .group(CommonUtils.randomString(10))
      .nodeName(CommonUtils.randomString)
      .brokerClusterKey(ObjectKey.of("g", "n"))
      .create(),
    Duration(5, TimeUnit.SECONDS)
  )

  @Test
  def testCopy(): Unit = {
    val nodeNames = Set(CommonUtils.randomString())
    val workerClusterInfo = WorkerClusterInfo(
      settings =
        WorkerApi.access.request.brokerClusterKey(ObjectKey.of("default", "bk")).nodeNames(nodeNames).creation.raw,
      aliveNodes = nodeNames,
      state = None,
      error = None,
      lastModified = 0
    )

    // pass
    Await.result(wkCreator().settings(workerClusterInfo.settings).create(), Duration(30, TimeUnit.SECONDS))
  }
}
