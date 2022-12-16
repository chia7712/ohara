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

package oharastream.ohara.client

import oharastream.ohara.client.configurator.{BrokerApi, WorkerApi, ZookeeperApi}
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.util.VersionUtils
import org.junit.jupiter.api.Test
import org.scalatest.matchers.should.Matchers._

class TestVersion extends OharaTest {
  @Test
  def testZookeeper(): Unit = {
    ZookeeperApi.IMAGE_NAME_DEFAULT shouldBe s"ghcr.io/chia7712/ohara/zookeeper:${VersionUtils.VERSION}"
  }

  @Test
  def testBroker(): Unit = {
    BrokerApi.IMAGE_NAME_DEFAULT shouldBe s"ghcr.io/chia7712/ohara/broker:${VersionUtils.VERSION}"
  }

  @Test
  def testWorker(): Unit = {
    WorkerApi.IMAGE_NAME_DEFAULT shouldBe s"ghcr.io/chia7712/ohara/connect-worker:${VersionUtils.VERSION}"
  }
}
