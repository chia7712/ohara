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
import java.util.concurrent.TimeUnit

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.{ContentTypes, StatusCodes}
import akka.http.scaladsl.testkit.RouteTestTimeout
import oharastream.ohara.common.data.Row
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.metrics.BeanChannel
import oharastream.ohara.metrics.basic.CounterMBean
import oharastream.ohara.shabondi._
import oharastream.ohara.shabondi.common.JsonSupport
import oharastream.ohara.shabondi.common.JsonSupport.RowData
import org.junit.jupiter.api.Test
import org.scalatest.matchers.should.Matchers._
import spray.json.DefaultJsonProtocol._

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters._
final class TestSinkRoute extends BasicShabondiTest {
  import oharastream.ohara.shabondi.ShabondiRouteTestSupport._

  // Extend the timeout to avoid the exception:
  // org.scalatest.exceptions.TestFailedException: Request was neither completed nor rejected within 1 second
  implicit val routeTestTimeout = RouteTestTimeout(Duration(5, TimeUnit.SECONDS))

  private def pollRowsRequest(
    webServer: WebServer,
    dataGroup: String,
    timeout: Long,
    ec: ExecutionContext
  ): Future[Seq[Row]] =
    Future {
      log.debug("pollRowsRequest[{}] begin...", dataGroup)
      val resultRows = ArrayBuffer.empty[Row]
      val request    = Get(uri = s"/groups/$dataGroup")
      var idx: Int   = 0
      val baseTime   = System.currentTimeMillis()
      var running    = true
      while (running) {
        idx += 1
        request ~> webServer.routes ~> check {
          val result = entityAs[Seq[RowData]].map(JsonSupport.toRow)
          resultRows ++= result
        }
        running = (System.currentTimeMillis() - baseTime) < timeout
        Thread.sleep(10)
      }
      log.debug("pollRowsRequest[{}] done.", dataGroup)
      resultRows.toSeq
    }(ec)

  @Test
  def testInvalidRequest(): Unit = {
    val topicKey1 = createTopicKey
    val config    = defaultSinkConfig(Seq(topicKey1))
    val webServer = new WebServer(config)
    webServer.routes

    try {
      val rowCount1 = 50
      KafkaSupport.prepareBulkOfRow(brokerProps, topicKey1, rowCount1, Duration(10, TimeUnit.SECONDS))
      log.info("produce {} rows", rowCount1)

      // test invalid endpoint
      val request0 = Get(uri = s"/xxx")
      request0 ~> webServer.routes ~> check {
        status should ===(StatusCodes.NotFound)
      }

      // test not allowed method
      val request1 = Post(uri = s"/groups/group0")
      request1 ~> webServer.routes ~> check {
        status should ===(StatusCodes.MethodNotAllowed)
      }

      // test invalid group name
      val request2 = Get(uri = s"/groups/group-0")
      request2 ~> webServer.routes ~> check {
        status should ===(StatusCodes.NotAcceptable)
        entityAs[String] should ===("Illegal group name, only accept alpha and numeric.")
      }

      val request3 = Get(uri = s"/groups/group0")
      request3 ~> webServer.routes ~> check {
        status should ===(StatusCodes.OK)
        contentType should ===(ContentTypes.`application/json`)
      }
    } finally {
      Releasable.close(webServer)
    }
  }

  @Test
  def testSingleGroup(): Unit = {
    implicit val ec = ExecutionContext.fromExecutorService(newThreadPool())

    val groupName = "g0"
    val topicKey1 = createTopicKey
    val config    = defaultSinkConfig(Seq(topicKey1))
    val webServer = new WebServer(config)
    webServer.routes // create route handle first.

    try {
      val clientFetch: Future[Seq[Row]]  = pollRowsRequest(webServer, groupName, 10 * 1000, ec)
      val clientFetch1: Future[Seq[Row]] = pollRowsRequest(webServer, groupName, 10 * 1000, ec)

      val rowCount1 = 50
      KafkaSupport.prepareBulkOfRow(brokerProps, topicKey1, rowCount1, Duration(10, TimeUnit.SECONDS))
      log.info("produce {} rows", rowCount1); Thread.sleep(1000)

      val rowCount2 = 80
      KafkaSupport.prepareBulkOfRow(brokerProps, topicKey1, rowCount2, Duration(10, TimeUnit.SECONDS))
      log.info("produce {} rows", rowCount2); Thread.sleep(1000)

      val rowCount3 = 40
      KafkaSupport.prepareBulkOfRow(brokerProps, topicKey1, rowCount3, Duration(10, TimeUnit.SECONDS))
      log.info("produce {} rows", rowCount3); Thread.sleep(1000)

      val totalCount = rowCount1 + rowCount2 + rowCount3
      val rows       = Await.result(clientFetch, Duration.Inf)
      val rows1      = Await.result(clientFetch1, Duration.Inf)
      (rows.size + rows1.size) should ===(totalCount)

      // assert metrics
      val beans = counterMBeans()
      beans.size should ===(1)
      beans(0).key should ===(config.objectKey)
      beans(0).item should ===(s"rows-$groupName")
      beans(0).getValue should ===(totalCount)
    } finally {
      Releasable.close(webServer)
      ec.shutdown()
    }
  }

  @Test
  def testMultipleGroup(): Unit = {
    implicit val ec = ExecutionContext.fromExecutorService(newThreadPool())

    val topicKey1 = createTopicKey
    val config    = defaultSinkConfig(Seq(topicKey1))
    val webServer = new WebServer(config)
    webServer.routes // create route handle first.

    try {
      val group0Name = "g0"
      val group1Name = "g1"
      // two data group concurrent request
      val clientFetch: Future[Seq[Row]]  = pollRowsRequest(webServer, group0Name, 10 * 1000, ec)
      val clientFetch1: Future[Seq[Row]] = pollRowsRequest(webServer, group1Name, 10 * 1000, ec)
      val clientFetch2: Future[Seq[Row]] = pollRowsRequest(webServer, group0Name, 10 * 1000, ec)
      val clientFetch3: Future[Seq[Row]] = pollRowsRequest(webServer, group1Name, 10 * 1000, ec)

      val rowCount1 = 150
      KafkaSupport.prepareBulkOfRow(brokerProps, topicKey1, rowCount1, Duration(10, TimeUnit.SECONDS))
      log.info("produce {} rows", rowCount1); Thread.sleep(1000)

      val rowCount2 = 180
      KafkaSupport.prepareBulkOfRow(brokerProps, topicKey1, rowCount2, Duration(10, TimeUnit.SECONDS))
      log.info("produce {} rows", rowCount2); Thread.sleep(1000)

      val rowCount3 = 140
      KafkaSupport.prepareBulkOfRow(brokerProps, topicKey1, rowCount3, Duration(10, TimeUnit.SECONDS))
      log.info("produce {} rows", rowCount3); Thread.sleep(1000)

      val totalCount = rowCount1 + rowCount2 + rowCount3

      val rows  = Await.result(clientFetch, Duration.Inf)
      val rows1 = Await.result(clientFetch1, Duration.Inf)
      val rows2 = Await.result(clientFetch2, Duration.Inf)
      val rows3 = Await.result(clientFetch3, Duration.Inf)

      (rows.size + rows2.size) should ===(totalCount)
      (rows1.size + rows3.size) should ===(totalCount)

      // assert metrics
      val beans = counterMBeans()
      beans.size should ===(2)
      beans.foreach { bean =>
        bean.item match {
          case "rows-g0" | "rows-g1" =>
            bean.key should ===(config.objectKey)
            bean.getValue should ===(totalCount)
          case n =>
            fail(s"Unknown counter: $n")
        }
      }
    } finally {
      Releasable.close(webServer)
      ec.shutdown()
    }
  }

  @Test
  def testSinkFreeIdleGroup(): Unit = {
    implicit val ec = ExecutionContext.fromExecutorService(newThreadPool())

    val topicKey1    = createTopicKey
    val config       = defaultSinkConfig(Seq(topicKey1))
    val intervalTime = JDuration.ofSeconds(2)
    val idleTime     = JDuration.ofSeconds(3)

    val webServer = new WebServer(config)
    webServer.routeHandler.scheduleFreeIdleGroups(intervalTime, idleTime)
    val dataGroups = webServer.routeHandler.dataGroups

    try {
      log.debug("Start poll request...")
      val group0Name = "group0"
      val group1Name = "group1"
      pollRowsRequest(webServer, group0Name, 1 * 1000, ec)
      pollRowsRequest(webServer, group1Name, 1 * 1000, ec)
      TimeUnit.SECONDS.sleep(2)

      log.info("There should have two data groups.")
      dataGroups.groupExist(group0Name) should ===(true)
      dataGroups.groupExist(group1Name) should ===(true)

      pollRowsRequest(webServer, group1Name, 2 * 1000, ec)
      CommonUtils.await(() => !dataGroups.groupExist(group1Name), java.time.Duration.ofSeconds(10))
      CommonUtils.await(() => !dataGroups.groupExist(group0Name), java.time.Duration.ofSeconds(10))
      dataGroups.size shouldBe 0
    } finally {
      Releasable.close(webServer)
      ec.shutdown()
    }
  }

  private def counterMBeans(): Seq[CounterMBean] = BeanChannel.local().counterMBeans().asScala.toSeq
}
