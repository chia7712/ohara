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

import oharastream.ohara.client.configurator.ZookeeperApi
import oharastream.ohara.client.configurator.ZookeeperApi.ZookeeperClusterInfo
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.setting.SettingDef
import oharastream.ohara.common.util.CommonUtils
import org.junit.jupiter.api.Test
import org.scalatest.matchers.should.Matchers._
import spray.json.DeserializationException

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
class TestZookeeperCreator extends OharaTest {
  private[this] def zkCreator(): ZookeeperCollie.ClusterCreator =
    (executionContext, creation) => {
      if (executionContext == null) throw new AssertionError()
      Future.successful(
        ZookeeperClusterInfo(
          settings = ZookeeperApi.access.request.settings(creation.raw).creation.raw,
          aliveNodes = Set.empty,
          state = None,
          error = None,
          lastModified = 0
        )
      )
    }

  @Test
  def nullClusterName(): Unit = {
    an[NullPointerException] should be thrownBy zkCreator().name(null)
  }

  @Test
  def emptyClusterName(): Unit = {
    an[IllegalArgumentException] should be thrownBy zkCreator().name("")
  }

  @Test
  def nullGroup(): Unit = {
    an[NullPointerException] should be thrownBy zkCreator().group(null)
  }

  @Test
  def emptyGroup(): Unit = {
    an[IllegalArgumentException] should be thrownBy zkCreator().group("")
  }

  @Test
  def negativeClientPort(): Unit = {
    an[IllegalArgumentException] should be thrownBy zkCreator().clientPort(-1)
  }

  @Test
  def negativePeerPort(): Unit = {
    an[IllegalArgumentException] should be thrownBy zkCreator().peerPort(-1)
  }

  @Test
  def negativeElectionPort(): Unit = {
    an[IllegalArgumentException] should be thrownBy zkCreator().electionPort(-1)
  }

  @Test
  def nullNodes(): Unit = {
    an[NullPointerException] should be thrownBy zkCreator().nodeNames(null)
  }

  @Test
  def emptyNodes(): Unit = {
    an[IllegalArgumentException] should be thrownBy zkCreator().nodeNames(Set.empty)
  }

  @Test
  def testNameLength(): Unit =
    zkCreator()
      .name(CommonUtils.randomString(10))
      .group(CommonUtils.randomString(10))
      .peerPort(CommonUtils.availablePort())
      .clientPort(CommonUtils.availablePort())
      .electionPort(CommonUtils.availablePort())
      .nodeName(CommonUtils.randomString(10))
      .create()

  @Test
  def testInvalidName(): Unit =
    an[DeserializationException] should be thrownBy zkCreator()
      .name(CommonUtils.randomString(SettingDef.STRING_LENGTH_LIMIT + 1))
      .group(CommonUtils.randomString(10))
      .nodeName(CommonUtils.randomString(10))
      .create()

  @Test
  def testInvalidGroup(): Unit =
    an[DeserializationException] should be thrownBy zkCreator()
      .name(CommonUtils.randomString(10))
      .group(CommonUtils.randomString(SettingDef.STRING_LENGTH_LIMIT + 1))
      .nodeName(CommonUtils.randomString(10))
      .create()

  @Test
  def testCopy(): Unit = {
    val nodeNames = Set(CommonUtils.randomString())
    val zookeeperClusterInfo = ZookeeperClusterInfo(
      settings = ZookeeperApi.access.request.name(CommonUtils.randomString(10)).nodeNames(nodeNames).creation.raw,
      aliveNodes = nodeNames,
      state = None,
      error = None,
      lastModified = 0
    )

    // pass
    Await.result(zkCreator().settings(zookeeperClusterInfo.settings).create(), Duration(30, TimeUnit.SECONDS))
  }

  @Test
  def testMinimumCreator(): Unit = Await.result(
    zkCreator()
      .name(CommonUtils.randomString(10))
      .group(CommonUtils.randomString(10))
      .nodeName(CommonUtils.randomString)
      .create(),
    Duration(5, TimeUnit.SECONDS)
  )
}
