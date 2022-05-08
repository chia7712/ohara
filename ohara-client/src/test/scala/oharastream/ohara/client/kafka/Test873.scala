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

package oharastream.ohara.client.kafka

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.{Http, server}
import oharastream.ohara.client.kafka.WorkerJson.{ConnectorCreationResponse, KafkaConnectorTaskId, _}
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.setting.ConnectorKey
import oharastream.ohara.common.util.CommonUtils
import oharastream.ohara.kafka.connector.json.Creation
import org.junit.jupiter.api.Test
import org.scalatest.matchers.should.Matchers._

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._

/**
  * https://github.com/oharastream/ohara/issues/873.
  */
class Test873 extends OharaTest {
  private[this] def result[T](f: Future[T]): T = Await.result(f, Duration(60, TimeUnit.SECONDS))

  @Test
  def testCreateConnector(): Unit = {
    val className = CommonUtils.randomString()
    val settings = Map(
      CommonUtils.randomString() -> CommonUtils.randomString()
    )
    val tasks = Seq(
      KafkaConnectorTaskId(
        connector = CommonUtils.randomString(),
        task = 10
      )
    )
    val server = toServer {
      path("connectors") {
        post {
          entity(as[Creation]) { req =>
            complete(
              ConnectorCreationResponse(
                name = req.name(),
                config = req.configs().asScala.toMap,
                tasks = tasks
              )
            )
          }
        }
      }
    }

    try {
      val connectorKey = ConnectorKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))
      val client       = ConnectorAdmin(s"${server.hostname}:${server.port}")
      val response = result(
        client.connectorCreator().connectorKey(connectorKey).settings(settings).className(className).create()
      )
      response.name shouldBe connectorKey.connectorNameOnKafka()
      response.tasks shouldBe tasks
      settings.foreach {
        case (k, v) =>
          response.config(k) shouldBe v
      }
    } finally server.close()
  }

  private[this] def toServer(route: server.Route): SimpleServer = {
    implicit val system: ActorSystem = ActorSystem("my-system")
    val server                       = Await.result(Http().bindAndHandle(route, "localhost", 0), Duration(30, TimeUnit.SECONDS))
    new SimpleServer {
      override def hostname: String = server.localAddress.getHostString
      override def port: Int        = server.localAddress.getPort
      override def close(): Unit = {
        Await.result(server.unbind(), Duration(30, TimeUnit.SECONDS))
        Await.result(system.terminate(), Duration(30, TimeUnit.SECONDS))
      }
    }
  }
}
