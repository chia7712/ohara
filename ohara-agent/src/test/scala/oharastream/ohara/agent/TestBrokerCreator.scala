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

import oharastream.ohara.client.configurator.BrokerApi
import oharastream.ohara.client.configurator.BrokerApi.BrokerClusterInfo
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.setting.{ObjectKey, SettingDef}
import oharastream.ohara.common.util.CommonUtils
import org.junit.jupiter.api.Test
import org.scalatest.matchers.should.Matchers._
import spray.json.DeserializationException

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class TestBrokerCreator extends OharaTest {
  private[this] val zkKey: ObjectKey = ObjectKey.of(CommonUtils.randomString(), CommonUtils.randomString())

  private[this] def bkCreator(): BrokerCollie.ClusterCreator =
    (executionContext, creation) => {
      // the inputs have been checked (NullPointerException). Hence, we throw another exception here.
      if (executionContext == null) throw new AssertionError()
      Future.successful(
        BrokerClusterInfo(
          settings = BrokerApi.access.request.settings(creation.raw).creation.raw,
          aliveNodes = creation.nodeNames,
          state = None,
          error = None,
          lastModified = 0
        )
      )
    }

  @Test
  def nullClusterName(): Unit = {
    an[NullPointerException] should be thrownBy bkCreator().name(null)
  }

  @Test
  def emptyClusterName(): Unit = {
    an[IllegalArgumentException] should be thrownBy bkCreator().name("")
  }

  @Test
  def nullGroup(): Unit = {
    an[NullPointerException] should be thrownBy bkCreator().group(null)
  }

  @Test
  def emptyGroup(): Unit = {
    an[IllegalArgumentException] should be thrownBy bkCreator().group("")
  }

  @Test
  def nullZkClusterName(): Unit = {
    an[NullPointerException] should be thrownBy bkCreator().zookeeperClusterKey(null)
  }

  @Test
  def negativeClientPort(): Unit = {
    an[IllegalArgumentException] should be thrownBy bkCreator().clientPort(-1)
  }

  @Test
  def negativeJmxPort(): Unit = {
    an[IllegalArgumentException] should be thrownBy bkCreator().jmxPort(-1)
  }

  @Test
  def nullNodes(): Unit = {
    an[NullPointerException] should be thrownBy bkCreator().nodeNames(null)
  }

  @Test
  def emptyNodes(): Unit = {
    an[IllegalArgumentException] should be thrownBy bkCreator().nodeNames(Set.empty)
  }

  @Test
  def testNameLength(): Unit =
    bkCreator()
      .name(CommonUtils.randomString(10))
      .group(CommonUtils.randomString(10))
      .zookeeperClusterKey(zkKey)
      .clientPort(CommonUtils.availablePort())
      .nodeName(CommonUtils.randomString)
      .create()

  @Test
  def testInvalidName(): Unit =
    an[DeserializationException] should be thrownBy bkCreator()
      .name(CommonUtils.randomString(SettingDef.STRING_LENGTH_LIMIT))
      .group(CommonUtils.randomString(10))
      .nodeName(CommonUtils.randomString)
      .create()

  @Test
  def testMinimumCreator(): Unit = Await.result(
    bkCreator()
      .name(CommonUtils.randomString(10))
      .group(CommonUtils.randomString(10))
      .nodeName(CommonUtils.randomString)
      .zookeeperClusterKey(ObjectKey.of("default", "name"))
      .create(),
    Duration(5, TimeUnit.SECONDS)
  )

  @Test
  def testCopy(): Unit = {
    val nodeNames = Set(CommonUtils.randomString())
    val brokerClusterInfo = BrokerClusterInfo(
      settings = BrokerApi.access.request
        .name(CommonUtils.randomString(10))
        .zookeeperClusterKey(zkKey)
        .nodeNames(nodeNames)
        .creation
        .raw,
      aliveNodes = nodeNames,
      state = None,
      error = None,
      lastModified = 0
    )

    // pass
    Await.result(bkCreator().settings(brokerClusterInfo.settings).create(), Duration(30, TimeUnit.SECONDS))
  }
}
