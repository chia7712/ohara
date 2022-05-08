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

import oharastream.ohara.client.configurator.{BrokerApi, ConnectorApi, PipelineApi, TopicApi, WorkerApi}
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.testing.WithBrokerWorker
import org.junit.jupiter.api.{AfterEach, Test}
import org.scalatest.matchers.should.Matchers._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
class TestListManyPipelines extends WithBrokerWorker {
  private[this] val configurator =
    Configurator.builder.fake(testUtil().brokersConnProps(), testUtil().workersConnProps()).build()

  private[this] val workerClusterInfo = result(
    WorkerApi.access.hostname(configurator.hostname).port(configurator.port).list()
  ).head

  private[this] def result[T](f: Future[T]): T = Await.result(f, Duration(20, TimeUnit.SECONDS))

  private[this] val numberOfPipelines = 30
  @Test
  def test(): Unit = {
    val topic = result(
      TopicApi.access
        .hostname(configurator.hostname)
        .port(configurator.port)
        .request
        .name(CommonUtils.randomString(10))
        .brokerClusterKey(
          result(BrokerApi.access.hostname(configurator.hostname).port(configurator.port).list()).head.key
        )
        .create()
    )

    val connector = result(
      ConnectorApi.access
        .hostname(configurator.hostname)
        .port(configurator.port)
        .request
        .name(CommonUtils.randomString(10))
        .className("oharastream.ohara.connector.perf.PerfSource")
        .topicKey(topic.key)
        .numberOfTasks(1)
        .workerClusterKey(workerClusterInfo.key)
        .create()
    )

    val pipelines = (0 until numberOfPipelines).map { _ =>
      result(
        PipelineApi.access
          .hostname(configurator.hostname)
          .port(configurator.port)
          .request
          .name(CommonUtils.randomString(10))
          .endpoint(connector)
          .endpoint(topic)
          .create()
      )
    }

    val listPipeline =
      Await.result(
        PipelineApi.access.hostname(configurator.hostname).port(configurator.port).list(),
        Duration(20, TimeUnit.SECONDS)
      )
    pipelines.size shouldBe listPipeline.size
    pipelines.foreach(p => listPipeline.exists(_.name == p.name) shouldBe true)
  }

  @AfterEach
  def tearDown(): Unit = Releasable.close(configurator)
}
