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

package oharastream.ohara.configurator.route

import oharastream.ohara.client.configurator.{NodeApi, ZookeeperApi}
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.configurator.Configurator
import org.junit.jupiter.api.{AfterEach, Test}
import org.scalatest.matchers.should.Matchers._
import spray.json.DeserializationException

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class TestClusterNameUpperCaseRoute extends OharaTest {
  private[this] val numberOfCluster = 1
  private[this] val configurator =
    Configurator.builder.fake(numberOfCluster, numberOfCluster, "zk").build()
  private[this] val nodeApi      = NodeApi.access.hostname(configurator.hostname).port(configurator.port)
  private[this] val zookeeperApi = ZookeeperApi.access.hostname(configurator.hostname).port(configurator.port)

  private[this] def result[T](f: Future[T]): T = Await.result(f, Duration("20 seconds"))
  @Test
  def testAddZookeeper(): Unit = {
    result(nodeApi.request.nodeName("host1").port(22).user("b").password("c").create())

    an[DeserializationException] should be thrownBy result(
      zookeeperApi.request.name(s"ZK-${CommonUtils.randomString(10)}").nodeName("host1").create()
    )
  }

  @AfterEach
  def tearDown(): Unit = Releasable.close(configurator)
}
