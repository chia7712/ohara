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

package oharastream.ohara.shabondi.sink

import java.time.{Duration => JDuration}
import java.util.concurrent.{ExecutorService, TimeUnit}

import akka.http.scaladsl.testkit.RouteTestTimeout
import oharastream.ohara.common.setting.ObjectKey
import oharastream.ohara.common.util.Releasable
import oharastream.ohara.shabondi.{BasicShabondiTest, KafkaSupport}
import org.junit.jupiter.api.{AfterEach, Test}

import scala.compat.java8.DurationConverters
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService}
import org.scalatest.matchers.should.Matchers._

import scala.concurrent.duration.Duration

final class TestSinkDataGroups extends BasicShabondiTest {
  // Extend the timeout to avoid the exception:
  // org.scalatest.exceptions.TestFailedException: Request was neither completed nor rejected within 1 second
  implicit val routeTestTimeout: RouteTestTimeout = RouteTestTimeout(Duration(5, TimeUnit.SECONDS))

  @AfterEach
  override def tearDown(): Unit = {
    super.tearDown()
  }

  @Test
  def testSingleGroup(): Unit = {
    val threadPool: ExecutorService                  = newThreadPool()
    implicit val ec: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(threadPool)
    val objectKey                                    = ObjectKey.of("g", "n")
    val topicKey1                                    = createTopicKey
    val rowCount                                     = 999
    val dataGroups =
      new SinkDataGroups(
        objectKey,
        brokerProps,
        Set(topicKey1),
        DurationConverters.toJava(Duration(10, TimeUnit.SECONDS))
      )
    try {
      KafkaSupport.prepareBulkOfRow(brokerProps, topicKey1, rowCount)

      val queue  = dataGroups.createIfAbsent("group0").queue
      val queue1 = dataGroups.createIfAbsent("group0").queue
      queue should ===(queue1)
      dataGroups.size should ===(1)

      val rows = countRows(queue, 10 * 1000, ec)

      Await.result(rows, Duration(30, TimeUnit.SECONDS)) should ===(rowCount)
    } finally {
      Releasable.close(dataGroups)
      threadPool.shutdown()
    }
  }

  @Test
  def testMultipleGroup(): Unit = {
    val threadPool: ExecutorService                  = newThreadPool()
    implicit val ec: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(threadPool)
    val objectKey                                    = ObjectKey.of("g", "n")
    val topicKey1                                    = createTopicKey
    val rowCount                                     = 999
    val dataGroups =
      new SinkDataGroups(
        objectKey,
        brokerProps,
        Set(topicKey1),
        DurationConverters.toJava(Duration(10, TimeUnit.SECONDS))
      )
    try {
      KafkaSupport.prepareBulkOfRow(brokerProps, topicKey1, rowCount)

      val queue   = dataGroups.createIfAbsent("group0").queue
      val queue1  = dataGroups.createIfAbsent("group1").queue
      val queue1a = dataGroups.createIfAbsent("group1").queue
      val queue2  = dataGroups.createIfAbsent("group2").queue
      queue should !==(queue1)
      queue1 should !==(queue2)
      queue1 should ===(queue1a)
      dataGroups.size should ===(3)

      val rows  = countRows(queue, 10 * 1000, ec)
      val rows1 = countRows(queue1, 10 * 1000, ec)
      val rows2 = countRows(queue2, 10 * 1000, ec)

      Await.result(rows, Duration(30, TimeUnit.SECONDS)) should ===(rowCount)
      Await.result(rows1, Duration(30, TimeUnit.SECONDS)) should ===(rowCount)
      Await.result(rows2, Duration(30, TimeUnit.SECONDS)) should ===(rowCount)
    } finally {
      Releasable.close(dataGroups)
      threadPool.shutdown()
    }
  }

  @Test
  def testRowQueueIsIdle(): Unit = {
    val idleTime = JDuration.ofSeconds(2)
    val rowQueue = new RowQueue()
    multipleRows(100).foreach(rowQueue.add)
    rowQueue.poll() should !==(null)
    rowQueue.isIdle(idleTime) should ===(false)
    TimeUnit.SECONDS.sleep(1)

    rowQueue.poll() should !==(null)
    rowQueue.isIdle(idleTime) should ===(false)
    TimeUnit.SECONDS.sleep(3)

    rowQueue.isIdle(idleTime) should ===(true)
  }

  @Test
  def testFreeIdleGroup(): Unit = {
    val threadPool: ExecutorService                  = newThreadPool()
    implicit val ec: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(threadPool)
    val objectKey                                    = ObjectKey.of("g", "n")
    val topicKey1                                    = createTopicKey
    val idleTime                                     = JDuration.ofSeconds(3)
    val dataGroups =
      new SinkDataGroups(
        objectKey,
        brokerProps,
        Set(topicKey1),
        DurationConverters.toJava(Duration(10, TimeUnit.SECONDS))
      )
    try {
      val group0Name = "group0"
      val group1Name = "group1"
      val group0     = dataGroups.createIfAbsent(group0Name)
      val group1     = dataGroups.createIfAbsent(group1Name)
      dataGroups.size should ===(2)

      // test for not over idle time
      countRows(group0.queue, 1 * 1000, ec)
      countRows(group1.queue, 1 * 1000, ec)
      TimeUnit.SECONDS.sleep(idleTime.getSeconds - 1)
      dataGroups.freeIdleGroup(idleTime)
      dataGroups.groupExist(group0Name) should ===(true)
      dataGroups.groupExist(group1Name) should ===(true)

      // test for idle time has passed
      countRows(group0.queue, 2 * 1000, ec)
      TimeUnit.SECONDS.sleep(idleTime.getSeconds)
      dataGroups.freeIdleGroup(idleTime)
      dataGroups.size should ===(1)
      dataGroups.groupExist(group0Name) should ===(true)
      dataGroups.groupExist(group1Name) should ===(false)
    } finally {
      Releasable.close(dataGroups)
      threadPool.shutdown()
    }
  }
}
