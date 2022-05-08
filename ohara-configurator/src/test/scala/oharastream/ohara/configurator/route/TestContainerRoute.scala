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

import oharastream.ohara.client.configurator._
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.setting.ObjectKey
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.configurator.Configurator
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.scalatest.matchers.should.Matchers._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

class TestContainerRoute extends OharaTest {
  private[this] val configurator = Configurator.builder.fake(0, 0).build()
  private[this] val containerApi = ContainerApi.access.hostname(configurator.hostname).port(configurator.port)
  private[this] val brokerApi    = BrokerApi.access.hostname(configurator.hostname).port(configurator.port)
  private[this] val workerApi    = WorkerApi.access.hostname(configurator.hostname).port(configurator.port)

  private[this] val zkClusterKey = ObjectKey.of("default", CommonUtils.randomString(10))
  private[this] val bkClusterKey = ObjectKey.of("default", CommonUtils.randomString(10))
  private[this] val wkClusterKey = ObjectKey.of("default", CommonUtils.randomString(10))

  private[this] val nodeNames: Set[String] = Set("n0", "n1")

  private[this] def result[T](f: Future[T]): T = Await.result(f, Duration("20 seconds"))
  @BeforeEach
  def setup(): Unit = {
    val nodeApi = NodeApi.access.hostname(configurator.hostname).port(configurator.port)

    nodeNames.isEmpty shouldBe false
    nodeNames.foreach { n =>
      result(nodeApi.request.nodeName(n).port(22).user("user").password("pwd").create())
    }

    val zk = result(
      ZookeeperApi.access
        .hostname(configurator.hostname)
        .port(configurator.port)
        .request
        .key(zkClusterKey)
        .nodeNames(nodeNames)
        .create()
    )
    zk.key shouldBe zkClusterKey
    result(ZookeeperApi.access.hostname(configurator.hostname).port(configurator.port).start(zk.key))

    val bk = result(brokerApi.request.key(bkClusterKey).zookeeperClusterKey(zkClusterKey).nodeNames(nodeNames).create())
    result(brokerApi.start(bk.key))

    val wk = result(workerApi.request.key(wkClusterKey).brokerClusterKey(bkClusterKey).nodeNames(nodeNames).create())
    result(workerApi.start(wk.key))
  }

  @Test
  def testGetContainersOfBrokerCluster(): Unit = {
    val containerGroups = result(containerApi.get(bkClusterKey))
    containerGroups.size should not be 0
    containerGroups.foreach(group => {
      group.clusterKey shouldBe bkClusterKey
      group.clusterType shouldBe "broker"
      group.containers.size should not be 0
    })
  }

  @Test
  def testGetContainersOfWorkerCluster(): Unit = {
    val containerGroups = result(containerApi.get(wkClusterKey))
    containerGroups.size should not be 0
    containerGroups.foreach(group => {
      group.clusterKey shouldBe wkClusterKey
      group.clusterType shouldBe "worker"
      group.containers.size should not be 0
    })
  }

  @AfterEach
  def tearDown(): Unit = Releasable.close(configurator)
}
