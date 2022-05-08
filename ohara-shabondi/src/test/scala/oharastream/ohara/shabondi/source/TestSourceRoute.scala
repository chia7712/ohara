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

package oharastream.ohara.shabondi.source

import java.util.concurrent.TimeUnit

import akka.http.scaladsl.model._
import akka.http.scaladsl.testkit.RouteTestTimeout
import oharastream.ohara.common.data.Row
import oharastream.ohara.kafka.Consumer
import oharastream.ohara.metrics.BeanChannel
import oharastream.ohara.metrics.basic.CounterMBean
import oharastream.ohara.shabondi.{BasicShabondiTest, KafkaSupport}
import org.junit.jupiter.api.Test
import org.scalatest.matchers.should.Matchers._
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._
final class TestSourceRoute extends BasicShabondiTest {
  import oharastream.ohara.shabondi.ShabondiRouteTestSupport._

  // Extend the timeout to avoid the exception:
  // org.scalatest.exceptions.TestFailedException: Request was neither completed nor rejected within 1 second
  implicit val routeTestTimeout = RouteTestTimeout(Duration(5, TimeUnit.SECONDS))

  private val columnCount  = 6
  private val requestCount = 200

  private def sourceData: Map[String, Int] =
    (1 to columnCount).foldLeft(Map.empty[String, Int]) { (m, v) =>
      m + ("col-" + v -> v)
    }

  @Test
  def testInvalidRequest(): Unit = {
    val topicKey1 = createTopicKey
    val config    = defaultSourceConfig(Seq(topicKey1))
    val webServer = new WebServer(config)

    val request = Get("/")
    request ~> webServer.routes ~> check {
      response.status should ===(StatusCodes.MethodNotAllowed)
      contentType should ===(ContentTypes.`text/plain(UTF-8)`)
    }

    val request2 = Post("/")
    request2 ~> webServer.routes ~> check {
      response.status should ===(StatusCodes.BadRequest)
      contentType should ===(ContentTypes.`text/plain(UTF-8)`)
    }

    val jsonRow  = sourceData.toJson.compactPrint
    val entity   = HttpEntity(ContentTypes.`application/json`, jsonRow)
    val request3 = Post("/", entity)
    request3 ~> webServer.routes ~> check {
      response.status should ===(StatusCodes.OK)
      contentType should ===(ContentTypes.`text/plain(UTF-8)`)
    }
  }

  @Test
  def testSourceRoute(): Unit = {
    val topicKey1 = createTopicKey
    val config    = defaultSourceConfig(Seq(topicKey1))
    val webServer = new WebServer(config)
    try {
      (1 to requestCount).foreach { _ =>
        val jsonRow = sourceData.toJson.compactPrint
        val entity  = HttpEntity(ContentTypes.`application/json`, jsonRow)
        val request = Post(uri = "/", entity)

        request ~> webServer.routes ~> check {
          entityAs[String] should ===("OK")
        }
      }

      // assertion
      val rowsTopic1: Seq[Consumer.Record[Row, Array[Byte]]] =
        KafkaSupport.pollTopicOnce(brokerProps, topicKey1, 60, requestCount)
      rowsTopic1.size should ===(requestCount)
      rowsTopic1(0).key.get.cells.size should ===(columnCount)

      // assert metrics
      val beans = counterMBeans()
      beans.size should ===(1)
      beans(0).getValue should ===(requestCount)
    } finally {
      webServer.close()
      topicAdmin.deleteTopic(topicKey1)
    }
  }

  private def counterMBeans(): Seq[CounterMBean] = BeanChannel.local().counterMBeans().asScala.toSeq
}
