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

package oharastream.ohara.configurator

import java.util.concurrent.TimeUnit

import oharastream.ohara.client.configurator.{BrokerApi, NodeApi, WorkerApi, ZookeeperApi}
import oharastream.ohara.common.util.CommonUtils
import oharastream.ohara.testing.WithBrokerWorker
import org.junit.jupiter.api.Test
import org.scalatest.matchers.should.Matchers._

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

/**
  * Embedded mode with fake cluster.
  */
class TestMixedFakeCollie extends WithBrokerWorker {
  @Test
  def test(): Unit = {
    val configurator = Configurator.builder.fake(testUtil().brokersConnProps(), testUtil().workersConnProps()).build()

    try {
      Await
        .result(
          BrokerApi.access.hostname(configurator.hostname).port(configurator.port).list(),
          Duration(20, TimeUnit.SECONDS)
        )
        .size shouldBe 1

      Await
        .result(
          WorkerApi.access.hostname(configurator.hostname).port(configurator.port).list(),
          Duration(20, TimeUnit.SECONDS)
        )
        .size shouldBe 1

      val nodes =
        Await.result(
          NodeApi.access.hostname(configurator.hostname).port(configurator.port).list(),
          Duration(20, TimeUnit.SECONDS)
        )

      // embedded mode always add single node since embedded mode assign different client port to each thread and
      // our collie demands that all "processes" should use same port.
      nodes.size shouldBe 1

      val zk = Await.result(
        ZookeeperApi.access
          .hostname(configurator.hostname)
          .port(configurator.port)
          .request
          .name(CommonUtils.randomString(10))
          .nodeNames(nodes.map(_.name).toSet)
          .create(),
        Duration(20, TimeUnit.SECONDS)
      )
      Await
        .result(
          ZookeeperApi.access.hostname(configurator.hostname).port(configurator.port).start(zk.key),
          Duration(20, TimeUnit.SECONDS)
        )

      val bk = Await.result(
        BrokerApi.access
          .hostname(configurator.hostname)
          .port(configurator.port)
          .request
          .name(CommonUtils.randomString(10))
          .zookeeperClusterKey(zk.key)
          .nodeNames(nodes.map(_.name).toSet)
          .create(),
        Duration(20, TimeUnit.SECONDS)
      )
      Await.result(
        BrokerApi.access.hostname(configurator.hostname).port(configurator.port).start(bk.key),
        Duration(20, TimeUnit.SECONDS)
      )

      Await
        .result(
          BrokerApi.access.hostname(configurator.hostname).port(configurator.port).list(),
          Duration(20, TimeUnit.SECONDS)
        )
        .size shouldBe 2

      Await.result(
        WorkerApi.access
          .hostname(configurator.hostname)
          .port(configurator.port)
          .request
          .name(CommonUtils.randomString(10))
          .brokerClusterKey(bk.key)
          .nodeNames(nodes.map(_.name).toSet)
          .create(),
        Duration(20, TimeUnit.SECONDS)
      )

      Await
        .result(
          WorkerApi.access.hostname(configurator.hostname).port(configurator.port).list(),
          Duration(20, TimeUnit.SECONDS)
        )
        .size shouldBe 2
    } finally configurator.close()
  }
}
