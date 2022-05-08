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

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import oharastream.ohara.client.configurator.{ConnectorApi, WorkerApi}
import oharastream.ohara.testing.WithBrokerWorker
import org.junit.jupiter.api.Test
import org.scalatest.matchers.should.Matchers._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class TestDefaultValuesAutoComplete extends WithBrokerWorker {
  private[this] def result[T](f: Future[T]): T = Await.result(f, Duration(20, TimeUnit.SECONDS))

  private[this] val configurator =
    Configurator.builder.fake(testUtil().brokersConnProps(), testUtil().workersConnProps()).build()

  private[this] val workerClusterInfo = result(
    WorkerApi.access.hostname(configurator.hostname).port(configurator.port).list()
  ).head

  private[this] val connectorApi = ConnectorApi.access.hostname(configurator.hostname).port(configurator.port)

  @Test
  def testDefaultValuesForPerfSource(): Unit = {
    val connector = result(
      connectorApi.request
        .workerClusterKey(workerClusterInfo.key)
        .className("oharastream.ohara.connector.perf.PerfSource")
        .create()
    )
    connector.settings.keySet should contain("perf.batch")
    connector.settings.keySet should contain("perf.frequency")
    connector.settings.keySet should contain("perf.cell.length")
  }

  @Test
  def testDefaultValuesForConsoleSink(): Unit = {
    val connector = result(
      connectorApi.request
        .workerClusterKey(workerClusterInfo.key)
        .className("oharastream.ohara.connector.console.ConsoleSink")
        .create()
    )
    connector.settings.keySet should contain("console.sink.frequence")
    connector.settings.keySet should contain("console.sink.row.divider")

    val a = new ConcurrentHashMap[String, String]()
    import scala.jdk.CollectionConverters._
    a.elements().asScala.toSeq
  }
}
