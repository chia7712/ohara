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

import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.util.CommonUtils
import org.junit.jupiter.api.Test
import org.scalatest.matchers.should.Matchers._
import spray.json._

class TestKeyApi extends OharaTest {
  @Test
  def testObjectKey(): Unit = {
    val name      = CommonUtils.randomString()
    val objectKey = OBJECT_KEY_FORMAT.read(s"""
                                              |
                                              | {
                                              |   "name": "$name"
                                              | }
                                              |""".stripMargin.parseJson)
    objectKey.group() shouldBe GROUP_DEFAULT
    objectKey.name() shouldBe name
  }

  @Test
  def testEmptyNameInObjectKey(): Unit =
    an[DeserializationException] should be thrownBy
      OBJECT_KEY_FORMAT.read(s"""
                              |
                              | {
                              |   "name": ""
                              | }
                              |""".stripMargin.parseJson)

  @Test
  def testEmptyGroupInObjectKey(): Unit =
    an[DeserializationException] should be thrownBy
      OBJECT_KEY_FORMAT.read(s"""
                              |
                              | {
                              |   "group": "g0",
                              |   "name": ""
                              | }
                              |""".stripMargin.parseJson)

  @Test
  def testTopicKey(): Unit = {
    val name     = CommonUtils.randomString()
    val topicKey = TOPIC_KEY_FORMAT.read(s"""
                                              |
                                              | {
                                              |   "name": "$name"
                                              | }
                                              |""".stripMargin.parseJson)
    topicKey.group() shouldBe GROUP_DEFAULT
    topicKey.name() shouldBe name
  }

  @Test
  def testEmptyNameInTopicKey(): Unit =
    an[DeserializationException] should be thrownBy
      TOPIC_KEY_FORMAT.read(s"""
                              |
                              | {
                              |   "name": ""
                              | }
                              |""".stripMargin.parseJson)

  @Test
  def testEmptyGroupInTopicKey(): Unit =
    an[DeserializationException] should be thrownBy
      TOPIC_KEY_FORMAT.read(s"""
                              |
                              | {
                              |   "group": "g0",
                              |   "name": ""
                              | }
                              |""".stripMargin.parseJson)

  @Test
  def testPureStringInObjectKey(): Unit = {
    val name = CommonUtils.randomString()
    val key  = OBJECT_KEY_FORMAT.read(JsString(name))
    key.group() shouldBe GROUP_DEFAULT
    key.name() shouldBe name
  }

  @Test
  def testPureStringInTopicKey(): Unit = {
    val name = CommonUtils.randomString()
    val key  = TOPIC_KEY_FORMAT.read(JsString(name))
    key.group() shouldBe GROUP_DEFAULT
    key.name() shouldBe name
  }

  @Test
  def testPureStringInConnectorKey(): Unit = {
    val name = CommonUtils.randomString()
    val key  = CONNECTOR_KEY_FORMAT.read(JsString(name))
    key.group() shouldBe GROUP_DEFAULT
    key.name() shouldBe name
  }

  @Test
  def testGroupAndNameInObjectKey(): Unit = {
    val group = CommonUtils.randomString()
    val name  = CommonUtils.randomString()
    val key   = OBJECT_KEY_FORMAT.read(s"""
                                        |
                                        | {
                                        |   "group": "$group",
                                        |   "name": "$name"
                                        | }
                                        |""".stripMargin.parseJson)
    key.group() shouldBe group
    key.name() shouldBe name
  }

  @Test
  def testGroupAndNameInTopicKey(): Unit = {
    val group = CommonUtils.randomString()
    val name  = CommonUtils.randomString()
    val key   = TOPIC_KEY_FORMAT.read(s"""
                                        |
                                        | {
                                        |   "group": "$group",
                                        |   "name": "$name"
                                        | }
                                        |""".stripMargin.parseJson)
    key.group() shouldBe group
    key.name() shouldBe name
  }

  @Test
  def testGroupAndNameInConnectorKey(): Unit = {
    val group = CommonUtils.randomString()
    val name  = CommonUtils.randomString()
    val key   = CONNECTOR_KEY_FORMAT.read(s"""
                                       |
                                       | {
                                       |   "group": "$group",
                                       |   "name": "$name"
                                       | }
                                       |""".stripMargin.parseJson)
    key.group() shouldBe group
    key.name() shouldBe name
  }

  @Test
  def incorrectRequestShouldGetCorrectException(): Unit = {
    an[DeserializationException] should be thrownBy CONNECTOR_KEY_FORMAT.read(s"""
                                                                                 |
                                                                                 | {
                                                                                 |   "group": "g"
                                                                                 | }
                                                                                 |""".stripMargin.parseJson)

    an[DeserializationException] should be thrownBy TOPIC_KEY_FORMAT.read(s"""
                                                                                 |
                                                                                 | {
                                                                                 |   "group": ["g"]
                                                                                 | }
                                                                                 |""".stripMargin.parseJson)

    an[DeserializationException] should be thrownBy OBJECT_KEY_FORMAT.read(s"""
                                                                                 |
                                                                                 | {
                                                                                 |   "group": {
                                                                                 |     "A": "A"
                                                                                 |   }
                                                                                 | }
                                                                                 |""".stripMargin.parseJson)

    an[DeserializationException] should be thrownBy CONNECTOR_KEY_FORMAT.read(s"""
                                                                                 |
                                                                                 | {
                                                                                 |   "group": "",
                                                                                 |   "name": "n"
                                                                                 | }
                                                                                 |""".stripMargin.parseJson)

    an[DeserializationException] should be thrownBy CONNECTOR_KEY_FORMAT.read(s"""
                                                                                 |
                                                                                 | {
                                                                                 |   "group": "g",
                                                                                 |   "name": ""
                                                                                 | }
                                                                                 |""".stripMargin.parseJson)
  }
}
