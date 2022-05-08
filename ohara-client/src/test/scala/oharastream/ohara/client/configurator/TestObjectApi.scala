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

package oharastream.ohara.client.configurator

import oharastream.ohara.client.configurator.ObjectApi._
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.setting.ObjectKey
import oharastream.ohara.common.util.CommonUtils
import org.junit.jupiter.api.Test
import org.scalatest.matchers.should.Matchers._
import spray.json.JsString
class TestObjectApi extends OharaTest {
  @Test
  def testSetLastModified(): Unit = ObjectInfo(Map.empty, 123).lastModified shouldBe 123

  @Test
  def testEquals(): Unit =
    ObjectInfo(Map("a" -> JsString("b")), 123) shouldBe ObjectInfo(Map("a" -> JsString("b")), 123)

  @Test
  def testNameInCreation(): Unit = ObjectApi.access.request.name("ab").creation.name shouldBe "ab"

  @Test
  def testGroupInCreation(): Unit = ObjectApi.access.request.name("ab").group("ab").creation.name shouldBe "ab"

  @Test
  def testKeyInCreation(): Unit = {
    val creation = ObjectApi.access.request.key(ObjectKey.of("g", "n")).creation
    creation.group shouldBe "g"
    creation.name shouldBe "n"
  }

  @Test
  def testTagsInCreation(): Unit =
    ObjectApi.access.request.key(ObjectKey.of("g", "n")).creation.tags shouldBe Map.empty

  @Test
  def testDefaultGroup(): Unit =
    ObjectApi.access.request.name(CommonUtils.randomString(10)).creation.group shouldBe GROUP_DEFAULT
}
