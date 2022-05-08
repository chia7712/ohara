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

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{ArrayBlockingQueue, Executors, LinkedBlockingDeque, TimeUnit}

import oharastream.ohara.client.configurator.{BrokerApi, TopicApi}
import oharastream.ohara.common.setting.TopicKey
import oharastream.ohara.common.util.Releasable
import oharastream.ohara.testing.WithBrokerWorker
import org.junit.jupiter.api.{AfterEach, Test}
import org.scalatest.matchers.should.Matchers._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._

class TestDeleteTopicsConcurrently extends WithBrokerWorker {
  private[this] val configurator =
    Configurator.builder.fake(testUtil.brokersConnProps, testUtil().workersConnProps()).build()

  private[this] val topicApi = TopicApi.access.hostname(configurator.hostname).port(configurator.port)

  private[this] def result[T](f: Future[T]): T = Await.result(f, Duration(20, TimeUnit.SECONDS))

  private[this] val brokerClusterInfo = result(
    BrokerApi.access.hostname(configurator.hostname).port(configurator.port).list()
  ).head

  @Test
  def test(): Unit = {
    val loopMax       = 10
    val count         = 3
    val topicKeyQueue = new ArrayBlockingQueue[TopicKey](count)
    (0 until count).foreach(i => topicKeyQueue.put(TopicKey.of("test", i.toString)))
    val executors      = Executors.newFixedThreadPool(count)
    val exceptionQueue = new LinkedBlockingDeque[Throwable]()
    val closed         = new AtomicBoolean(false)
    val loopCount      = new AtomicInteger(0)
    (0 until count).foreach(
      _ =>
        executors.execute { () =>
          while (!closed.get() && loopCount.getAndIncrement() <= loopMax) try {
            val topicKey = topicKeyQueue.take()
            try result(
              topicApi.request
                .group(topicKey.group())
                .name(topicKey.name())
                .brokerClusterKey(brokerClusterInfo.key)
                .numberOfPartitions(1)
                .numberOfReplications(1)
                .create()
                .flatMap(_ => topicApi.start(topicKey))
                .flatMap { _ =>
                  TimeUnit.SECONDS.sleep(1)
                  topicApi.stop(topicKey)
                }
                .flatMap { _ =>
                  TimeUnit.SECONDS.sleep(1)
                  topicApi.delete(topicKey)
                }
            )
            finally topicKeyQueue.put(topicKey)
          } catch {
            case t: Throwable =>
              exceptionQueue.put(t)
              closed.set(true)
          }
        }
    )
    executors.shutdown()
    withClue(s"${exceptionQueue.asScala.map(_.getMessage).mkString(",")}") {
      executors.awaitTermination(60, TimeUnit.SECONDS) shouldBe true
    }
    withClue(s"exceptions: $exceptionQueue")(exceptionQueue.size() shouldBe 0)
  }
  @AfterEach
  def tearDown(): Unit = Releasable.close(configurator)
}
