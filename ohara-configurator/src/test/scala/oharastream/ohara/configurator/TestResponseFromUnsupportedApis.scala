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

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.{HttpMethod, HttpMethods, HttpRequest}
import akka.http.scaladsl.unmarshalling.Unmarshal
import oharastream.ohara.client.configurator.ErrorApi
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import org.junit.jupiter.api.{AfterEach, Test}
import org.scalatest.matchers.should.Matchers._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class TestResponseFromUnsupportedApis extends OharaTest {
  private[this] val configurator = Configurator.builder.fake().build()

  private[this] implicit val actorSystem: ActorSystem = ActorSystem("Executor-TestResponseFromUnsupportedApis")

  private[this] val expectedMessage = oharastream.ohara.configurator.route.apiUrl

  private[this] def result[T](f: Future[T]): T = Await.result(f, Duration(20, TimeUnit.SECONDS))

  @Test
  def testGet(): Unit = sendRequest(HttpMethods.GET, CommonUtils.randomString()).apiUrl.get shouldBe expectedMessage

  @Test
  def testPut(): Unit = sendRequest(HttpMethods.PUT, CommonUtils.randomString()).apiUrl.get shouldBe expectedMessage

  @Test
  def testDelete(): Unit =
    sendRequest(HttpMethods.DELETE, CommonUtils.randomString()).apiUrl.get shouldBe expectedMessage

  @Test
  def testPost(): Unit = sendRequest(HttpMethods.POST, CommonUtils.randomString()).apiUrl.get shouldBe expectedMessage

  private[this] def sendRequest(method: HttpMethod, postfix: String): ErrorApi.Error =
    result(
      Http()
        .singleRequest(HttpRequest(method, s"http://${configurator.hostname}:${configurator.port}/$postfix"))
        .flatMap { response =>
          if (response.status.isSuccess()) Future.failed(new AssertionError())
          else Unmarshal(response.entity).to[ErrorApi.Error]
        }
    )

  @AfterEach
  def tearDown(): Unit = {
    Releasable.close(configurator)
    result(actorSystem.terminate())
  }
}
