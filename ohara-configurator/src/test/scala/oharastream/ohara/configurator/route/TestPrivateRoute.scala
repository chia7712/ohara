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

import java.util.concurrent.TimeUnit

import oharastream.ohara.client.configurator.PrivateApi.Deletion
import oharastream.ohara.client.configurator.{PrivateApi, WorkerApi}
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.configurator.Configurator
import org.junit.jupiter.api.Test
import org.scalatest.matchers.should.Matchers._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class TestPrivateRoute extends OharaTest {
  private[this] val workerCount  = 2
  private[this] val configurator = Configurator.builder.fake(1, workerCount).build()

  private[this] def result[T](f: Future[T]): T = Await.result(f, Duration(20, TimeUnit.SECONDS))

  private[this] val workerApi = WorkerApi.access.hostname(configurator.hostname).port(configurator.port)

  @Test
  def testDeletion(): Unit = {
    val workers = result(workerApi.list())
    val group   = workers.head.group
    val kind    = workers.head.kind
    workers.size shouldBe workerCount
    result(
      workerApi.request
        .group(group)
        .nodeNames(workers.head.nodeNames)
        .brokerClusterKey(workers.head.brokerClusterKey)
        .create()
    )

    result(workerApi.list()).size shouldBe workers.size + 1

    // we use same group to create an new worker cluster
    result(workerApi.list()).groupBy(_.group).size shouldBe workerCount

    result(
      PrivateApi.delete(
        hostname = configurator.hostname,
        port = configurator.port,
        deletion = Deletion(groups = Set(group), kinds = Set(kind))
      )
    )

    val latestWorkers = result(workerApi.list())
    latestWorkers.size shouldBe workers.size - 1

    // delete again
    result(
      PrivateApi.delete(
        hostname = configurator.hostname,
        port = configurator.port,
        deletion = Deletion(groups = Set(group), kinds = Set(kind))
      )
    )
    result(workerApi.list()).size shouldBe latestWorkers.size

    // delete group without kind
    result(
      PrivateApi.delete(
        hostname = configurator.hostname,
        port = configurator.port,
        deletion = Deletion(groups = Set(latestWorkers.head.group), kinds = Set.empty)
      )
    )
    result(workerApi.list()).size shouldBe latestWorkers.size
  }
}
