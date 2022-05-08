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

import oharastream.ohara.client.configurator.ObjectApi
import oharastream.ohara.client.configurator.ObjectApi.ObjectInfo
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.setting.ObjectKey
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.configurator.Configurator
import org.junit.jupiter.api.{AfterEach, Test}
import org.scalatest.matchers.should.Matchers._
import spray.json.JsString

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
class TestObjectRoute extends OharaTest {
  private[this] val configurator = Configurator.builder.fake(1, 1).build()

  private[this] val objectApi = ObjectApi.access.hostname(configurator.hostname).port(configurator.port)

  private[this] def result[T](f: Future[T]): T = Await.result(f, Duration(20, TimeUnit.SECONDS))

  private[this] def create(): ObjectInfo = {
    val key = ObjectKey.of("g", "n")
    val settings = Map(
      CommonUtils.randomString() -> JsString(CommonUtils.randomString()),
      CommonUtils.randomString() -> JsString(CommonUtils.randomString())
    )
    val objectInfo = result(objectApi.request.key(key).settings(settings).create())
    objectInfo.key shouldBe key
    settings.foreach {
      case (k, v) => objectInfo.settings(k) shouldBe v
    }
    objectInfo
  }

  @Test
  def testCreate(): Unit = create()

  @Test
  def testGet(): Unit = {
    val objectInfo = create()
    objectInfo shouldBe result(objectApi.get(objectInfo.key))
  }

  @Test
  def testGetNothing(): Unit =
    an[IllegalArgumentException] should be thrownBy result(objectApi.get(ObjectKey.of(CommonUtils.randomString(), "n")))

  @Test
  def testList(): Unit = {
    val objectInfo = create()
    objectInfo shouldBe result(objectApi.list()).head
  }

  @Test
  def testDelete(): Unit = {
    val objectInfo = create()
    result(objectApi.delete(objectInfo.key))
    result(objectApi.list()) shouldBe Seq.empty
  }

  @Test
  def testUpdate(): Unit = {
    val objectInfo = create()
    val settings = Map(
      CommonUtils.randomString() -> JsString(CommonUtils.randomString()),
      CommonUtils.randomString() -> JsString(CommonUtils.randomString())
    )
    val updated = result(objectApi.request.key(objectInfo.key).settings(settings).update())
    settings.foreach {
      case (k, v) => updated.settings(k) shouldBe v
    }
    objectInfo.settings.foreach {
      case (k, v) =>
        if (k == "lastModified") updated.settings(k) should not be v
        else updated.settings(k) shouldBe v
    }
  }

  @AfterEach
  def tearDown(): Unit = Releasable.close(configurator)
}
