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

import oharastream.ohara.client.configurator.PipelineApi._
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.setting.SettingDef
import oharastream.ohara.common.util.CommonUtils
import org.junit.jupiter.api.Test
import org.scalatest.matchers.should.Matchers._
import spray.json.{DeserializationException, _}

import scala.concurrent.ExecutionContext.Implicits.global
class TestPipelineApi extends OharaTest {
  @Test
  def nullKeyInGet(): Unit =
    an[NullPointerException] should be thrownBy PipelineApi.access.get(null)

  @Test
  def nullKeyInDelete(): Unit =
    an[NullPointerException] should be thrownBy PipelineApi.access.delete(null)

  @Test
  def ignoreNameOnCreation(): Unit =
    PipelineApi.access
      .hostname(CommonUtils.randomString(10))
      .port(CommonUtils.availablePort())
      .request
      .creation
      .name
      .length should not be 0

  @Test
  def ignoreNameOnUpdate(): Unit =
    an[NullPointerException] should be thrownBy PipelineApi.access
      .hostname(CommonUtils.randomString(10))
      .port(CommonUtils.availablePort())
      .request
      .update()

  @Test
  def emptyGroup(): Unit = an[IllegalArgumentException] should be thrownBy PipelineApi.access.request.group("")

  @Test
  def nullGroup(): Unit = an[NullPointerException] should be thrownBy PipelineApi.access.request.group(null)

  @Test
  def emptyName(): Unit = an[IllegalArgumentException] should be thrownBy PipelineApi.access.request.name("")

  @Test
  def nullName(): Unit = an[NullPointerException] should be thrownBy PipelineApi.access.request.name(null)

  @Test
  def parseCreation(): Unit = {
    val creation = CREATION_FORMAT.read(s"""
                                                          |  {
                                                          |  }
                                                          |
    """.stripMargin.parseJson)
    creation.group shouldBe GROUP_DEFAULT
    creation.name.length shouldBe SettingDef.STRING_LENGTH_LIMIT
    creation.endpoints shouldBe Set.empty

    val group     = CommonUtils.randomString(10)
    val name      = CommonUtils.randomString(10)
    val creation2 = CREATION_FORMAT.read(s"""
        |  {
        |    "group": "$group",
        |    "name": "$name"
        |  }
        |
    """.stripMargin.parseJson)
    creation2.group shouldBe group
    creation2.name shouldBe name
    creation2.endpoints shouldBe Set.empty
    creation2.tags shouldBe Map.empty
  }

  @Test
  def emptyNameInCreation(): Unit =
    an[DeserializationException] should be thrownBy CREATION_FORMAT.read("""
      |  {
      |    "name": ""
      |  }
      |
    """.stripMargin.parseJson)

  @Test
  def nullTags(): Unit = an[NullPointerException] should be thrownBy PipelineApi.access.request.tags(null)

  @Test
  def emptyTags(): Unit = PipelineApi.access.request.tags(Map.empty)

  @Test
  def testNameLimit(): Unit =
    an[DeserializationException] should be thrownBy
      PipelineApi.access
        .hostname(CommonUtils.randomString(10))
        .port(CommonUtils.availablePort())
        .request
        .name(CommonUtils.randomString(SettingDef.STRING_LENGTH_LIMIT + 1))
        .group(CommonUtils.randomString(SettingDef.STRING_LENGTH_LIMIT))
        .creation
  @Test
  def testGroupLimit(): Unit =
    an[DeserializationException] should be thrownBy
      PipelineApi.access
        .hostname(CommonUtils.randomString(10))
        .port(CommonUtils.availablePort())
        .request
        .name(CommonUtils.randomString(SettingDef.STRING_LENGTH_LIMIT))
        .group(CommonUtils.randomString(SettingDef.STRING_LENGTH_LIMIT + 1))
        .creation
}
