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

import oharastream.ohara.client.configurator.NodeApi._
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.util.CommonUtils
import org.junit.jupiter.api.Test
import org.scalatest.matchers.should.Matchers._
import spray.json.{DeserializationException, _}

import scala.concurrent.ExecutionContext.Implicits.global

class TestNodeApi extends OharaTest {
  @Test
  def ignorePortOnCreation(): Unit =
    NodeApi.access
      .hostname(CommonUtils.randomString(10))
      .port(CommonUtils.availablePort())
      .request
      .nodeName(CommonUtils.randomString(10))
      .user(CommonUtils.randomString(10))
      .password(CommonUtils.randomString(10))
      .create()

  @Test
  def negativePort(): Unit = an[IllegalArgumentException] should be thrownBy NodeApi.access.request.port(-1)

  @Test
  def ignoreUserOnCreation(): Unit =
    an[NullPointerException] should be thrownBy NodeApi.access
      .hostname(CommonUtils.randomString(10))
      .port(CommonUtils.availablePort())
      .request
      .nodeName(CommonUtils.randomString(10))
      .port(CommonUtils.availablePort())
      .password(CommonUtils.randomString(10))
      .create()

  @Test
  def emptyUser(): Unit = an[IllegalArgumentException] should be thrownBy NodeApi.access.request.user("")

  @Test
  def nullUser(): Unit = an[NullPointerException] should be thrownBy NodeApi.access.request.user(null)

  @Test
  def ignoreHostnameOnCreation(): Unit =
    an[NullPointerException] should be thrownBy NodeApi.access
      .hostname(CommonUtils.randomString(10))
      .port(CommonUtils.availablePort())
      .request
      .password(CommonUtils.randomString(10))
      .port(CommonUtils.availablePort())
      .user(CommonUtils.randomString(10))
      .create()

  @Test
  def ignoreHostnameOnUpdate(): Unit =
    an[NullPointerException] should be thrownBy NodeApi.access
      .hostname(CommonUtils.randomString(10))
      .port(CommonUtils.availablePort())
      .request
      .password(CommonUtils.randomString(10))
      .port(CommonUtils.availablePort())
      .user(CommonUtils.randomString(10))
      .update()

  @Test
  def emptyHostname(): Unit = an[IllegalArgumentException] should be thrownBy NodeApi.access.request.nodeName("")

  @Test
  def nullHostname(): Unit = an[NullPointerException] should be thrownBy NodeApi.access.request.nodeName(null)

  @Test
  def ignorePasswordOnCreation(): Unit =
    an[NullPointerException] should be thrownBy NodeApi.access
      .hostname(CommonUtils.randomString(10))
      .port(CommonUtils.availablePort())
      .request
      .nodeName(CommonUtils.randomString(10))
      .port(CommonUtils.availablePort())
      .user(CommonUtils.randomString(10))
      .create()

  @Test
  def emptyPassword(): Unit = an[IllegalArgumentException] should be thrownBy NodeApi.access.request.password("")

  @Test
  def nullPassword(): Unit = an[NullPointerException] should be thrownBy NodeApi.access.request.password(null)

  @Test
  def testCreation(): Unit = {
    val hostname = CommonUtils.randomString(10)
    val user     = CommonUtils.randomString(10)
    val password = CommonUtils.randomString(10)
    val port     = CommonUtils.availablePort()
    val creation =
      NodeApi.access.request.nodeName(hostname).user(user).password(password).port(port).creation
    creation.name shouldBe hostname
    creation.hostname shouldBe hostname
    creation.user shouldBe user
    creation.password shouldBe password
    creation.port shouldBe port
  }

  @Test
  def testUpdate(): Unit = {
    val user     = CommonUtils.randomString(10)
    val password = CommonUtils.randomString(10)
    val port     = CommonUtils.availablePort()
    val update   = NodeApi.access.request.user(user).password(password).port(port).updating
    update.user.get shouldBe user
    update.password.get shouldBe password
    update.port.get shouldBe port

    NodeApi.access.request.updating.port shouldBe None
    NodeApi.access.request.updating.user shouldBe None
    NodeApi.access.request.updating.password shouldBe None
  }

  @Test
  def testNegativePortInUpdate(): Unit =
    an[DeserializationException] should be thrownBy NodeApi.UPDATING_FORMAT.read("""
                                                                                           |{
                                                                                           | "port": -1
                                                                                           |}
                                                                                         """.stripMargin.parseJson)

  @Test
  def testZeroPortInUpdate(): Unit =
    an[DeserializationException] should be thrownBy NodeApi.UPDATING_FORMAT.read("""
                                                                                           |{
                                                                                           | "port": 0
                                                                                           |}
                                                                                         """.stripMargin.parseJson)

  @Test
  def testEmptyUserInUpdate(): Unit =
    an[DeserializationException] should be thrownBy NodeApi.UPDATING_FORMAT.read("""
                                                                                         |{
                                                                                         | "user": ""
                                                                                         |}
                                                                                       """.stripMargin.parseJson)

  @Test
  def testEmptyPasswordInUpdate(): Unit =
    an[DeserializationException] should be thrownBy NodeApi.UPDATING_FORMAT.read("""
                                                                                           |{
                                                                                           | "password": ""
                                                                                           |}
                                                                                         """.stripMargin.parseJson)

  @Test
  def testParseUpdate(): Unit = {
    val port     = CommonUtils.availablePort()
    val user     = CommonUtils.randomString(10)
    val password = CommonUtils.randomString(10)
    val update   = NodeApi.UPDATING_FORMAT.read(s"""
                                                       |{
                                                       | "port": $port,
                                                       | "user": "$user",
                                                       | "password": "$password"
                                                       |}
                                       """.stripMargin.parseJson)
    update.port.get shouldBe port
    update.user.get shouldBe user
    update.password.get shouldBe password
  }

  @Test
  def testParseDefaultPortInCreation(): Unit = {
    val hostname = CommonUtils.randomString(10)
    val creation = NodeApi.CREATION_FORMAT.read(s"""
                                                            |{
                                                            | "hostname": "$hostname",
                                                            | "user": "user",
                                                            | "password": "password"
                                                            |}
                                                          """.stripMargin.parseJson)

    creation.hostname shouldBe hostname
    creation.user shouldBe "user"
    creation.password shouldBe "password"
    // default is ssh port: 22
    creation.port shouldBe 22
  }

  @Test
  def testEmptyNameInCreation(): Unit =
    an[DeserializationException] should be thrownBy NodeApi.CREATION_FORMAT.read("""
                                                                                           |{
                                                                                           | "hostname": "",
                                                                                           | "port": 123,
                                                                                           | "user": "user",
                                                                                           | "password": "password"
                                                                                           |}
                                                                                         """.stripMargin.parseJson)

  @Test
  def testNegativePortInCreation(): Unit =
    an[DeserializationException] should be thrownBy NodeApi.CREATION_FORMAT.read("""
                                                                                             |{
                                                                                             | "hostname": "hostname",
                                                                                             | "port": -1,
                                                                                             | "user": "user",
                                                                                             | "password": "password"
                                                                                             |}
                                                                                           """.stripMargin.parseJson)

  @Test
  def testZeroPortInCreation(): Unit =
    an[DeserializationException] should be thrownBy NodeApi.CREATION_FORMAT.read("""
                                                                                             |{
                                                                                             | "hostname": "hostname",
                                                                                             | "port": 0,
                                                                                             | "user": "user",
                                                                                             | "password": "password"
                                                                                             |}
                                                                                           """.stripMargin.parseJson)

  @Test
  def testEmptyUserInCreation(): Unit =
    an[DeserializationException] should be thrownBy NodeApi.CREATION_FORMAT.read("""
                                                                                             |{
                                                                                             | "hostname": "hostname",
                                                                                             | "port": 123,
                                                                                             | "user": "",
                                                                                             | "password": "password"
                                                                                             |}
                                                                                           """.stripMargin.parseJson)

  @Test
  def testEmptyPasswordInCreation(): Unit =
    an[DeserializationException] should be thrownBy NodeApi.CREATION_FORMAT.read("""
                                                                                             |{
                                                                                             | "hostname": "hostname",
                                                                                             | "port": 123,
                                                                                             | "user": "user",
                                                                                             | "password": ""
                                                                                             |}
                                                                                           """.stripMargin.parseJson)

  @Test
  def testParseCreation(): Unit = {
    val name     = CommonUtils.randomString(10)
    val port     = CommonUtils.availablePort()
    val user     = CommonUtils.randomString(10)
    val password = CommonUtils.randomString(10)
    val creation = NodeApi.CREATION_FORMAT.read(s"""
                                                         |{
                                                         | "hostname": "$name",
                                                         | "port": $port,
                                                         | "user": "$user",
                                                         | "password": "$password"
                                                         |}
                                       """.stripMargin.parseJson)
    creation.group shouldBe GROUP_DEFAULT
    creation.name shouldBe name
    creation.hostname shouldBe name
    creation.port shouldBe port
    creation.user shouldBe user
    creation.password shouldBe password

    val hostname  = CommonUtils.randomString(10)
    val creation2 = NodeApi.CREATION_FORMAT.read(s"""
                                                             |{
                                                             | "group": "${CommonUtils.randomString(10)}",
                                                             | "hostname": "$name",
                                                             | "hostname": "$hostname",
                                                             | "port": $port,
                                                             | "user": "$user",
                                                             | "password": "$password"
                                                             |}
                                       """.stripMargin.parseJson)
    // node does support custom group
    creation2.group shouldBe GROUP_DEFAULT
    // the name is alias to hostname
    creation2.name shouldBe hostname
    creation2.hostname shouldBe hostname
    creation2.port shouldBe port
    creation2.user shouldBe user
    creation2.password shouldBe password
  }

  @Test
  def nullTags(): Unit = an[NullPointerException] should be thrownBy NodeApi.access.request.tags(null)

  @Test
  def emptyTags(): Unit = NodeApi.access.request.tags(Map.empty)

  @Test
  def testHostnameLimit(): Unit =
    an[DeserializationException] should be thrownBy
      NodeApi.access
        .hostname(CommonUtils.randomString(10))
        .port(CommonUtils.availablePort())
        .request
        .user("user")
        .password("password")
        .nodeName(CommonUtils.randomString(LIMIT_OF_HOSTNAME_LENGTH + 1))
        .creation

  @Test
  def testCoresResource(): Unit = Resource.cpu(2, None).unit shouldBe "cores"

  @Test
  def testCoreResource(): Unit = Resource.cpu(1, None).unit shouldBe "core"

  @Test
  def testMemoryResourceInBytes(): Unit = Resource.memory(1024 - 1, None).unit shouldBe "bytes"

  @Test
  def testMemoryResourceInKB(): Unit = Resource.memory(1024 * 1024 - 1, None).unit shouldBe "KB"

  @Test
  def testMemoryResourceInMB(): Unit = Resource.memory(1024 * 1024 * 1024 - 1, None).unit shouldBe "MB"

  @Test
  def testMemoryResourceInGB(): Unit = Resource.memory(1024 * 1024 * 1024, None).unit shouldBe "GB"

  @Test
  def testNodeSetter(): Unit = NodeApi.access.request.node(Node(CommonUtils.randomString(10), "user", "password"))
}
