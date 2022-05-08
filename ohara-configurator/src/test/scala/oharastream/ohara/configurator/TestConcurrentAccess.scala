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

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, TimeUnit}

import oharastream.ohara.client.configurator.NodeApi
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import org.junit.jupiter.api.{AfterEach, Test}
import org.scalatest.matchers.should.Matchers._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}

class TestConcurrentAccess extends OharaTest {
  private[this] val configurator = Configurator.builder.fake().build()

  private[this] val nodeApi = NodeApi.access.hostname(configurator.hostname).port(configurator.port)

  private[this] def result[T](f: Future[T]): T = Await.result(f, Duration(10, TimeUnit.SECONDS))

  /**
    * the get/list happens after delete should NOT see the deleted objects
    */
  @Test
  def deletedObjectShouldDisappearFromGet(): Unit = {
    val threadCount                                         = 10
    val threadsPool                                         = Executors.newFixedThreadPool(threadCount)
    val unmatchedCount                                      = new AtomicInteger()
    implicit val executionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(threadsPool)
    (0 until threadCount).foreach { _ =>
      threadsPool.execute { () =>
        val nodeName = CommonUtils.randomString(10)
        val nodes = result(
          nodeApi.request
            .nodeName(nodeName)
            .user(CommonUtils.randomString(10))
            .password(CommonUtils.randomString(10))
            .create()
            .flatMap(node => nodeApi.delete(node.key))
            .flatMap(_ => nodeApi.list())
        )
        if (nodes.exists(_.hostname == nodeName)) unmatchedCount.incrementAndGet()
      }
    }
    threadsPool.shutdown()
    threadsPool.awaitTermination(60, TimeUnit.SECONDS) shouldBe true
    unmatchedCount.get() shouldBe 0
  }

  @AfterEach
  def tearDown(): Unit = Releasable.close(configurator)
}
