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
import oharastream.ohara.common.setting.ObjectKey
import oharastream.ohara.common.util.{CommonUtils, VersionUtils}
import org.junit.jupiter.api.Test
import spray.json.DefaultJsonProtocol._
import org.scalatest.matchers.should.Matchers._
import spray.json._

class TestInspectApi extends OharaTest {
  @Test
  def testJsonStringFromVersionUtils(): Unit = {
    val fields = VersionUtils.jsonString().parseJson.asJsObject.fields
    fields("version").convertTo[String] shouldBe VersionUtils.VERSION
    fields("branch").convertTo[String] shouldBe VersionUtils.BRANCH
    fields("revision").convertTo[String] shouldBe VersionUtils.REVISION
    fields("user").convertTo[String] shouldBe VersionUtils.USER
    fields("date").convertTo[String] shouldBe VersionUtils.DATE
  }

  @Test
  def testBasicQueryObject(): Unit = {
    val url              = CommonUtils.randomString(10)
    val user             = CommonUtils.randomString(10)
    val password         = CommonUtils.randomString(10)
    val workerClusterKey = ObjectKey.of("default", "wk")
    val query = InspectApi.access
      .hostname(CommonUtils.randomString(10))
      .port(CommonUtils.availablePort())
      .rdbRequest
      .jdbcUrl(url)
      .user(user)
      .password(password)
      .workerClusterKey(workerClusterKey)
      .query

    query.url shouldBe url
    query.user shouldBe user
    query.password shouldBe password
    query.workerClusterKey shouldBe workerClusterKey
    query.catalogPattern shouldBe None
    query.schemaPattern shouldBe None
    query.tableName shouldBe None
  }

  @Test
  def testQueryObjectWithAllFields(): Unit = {
    val url              = CommonUtils.randomString(10)
    val user             = CommonUtils.randomString(10)
    val password         = CommonUtils.randomString(10)
    val workerClusterKey = ObjectKey.of(CommonUtils.randomString(10), CommonUtils.randomString(10))
    val catalogPattern   = CommonUtils.randomString(10)
    val schemaPattern    = CommonUtils.randomString(10)
    val tableName        = CommonUtils.randomString(10)
    val query = InspectApi.access
      .hostname(CommonUtils.randomString(10))
      .port(CommonUtils.availablePort())
      .rdbRequest
      .jdbcUrl(url)
      .user(user)
      .password(password)
      .workerClusterKey(workerClusterKey)
      .catalogPattern(catalogPattern)
      .schemaPattern(schemaPattern)
      .tableName(tableName)
      .query

    query.url shouldBe url
    query.user shouldBe user
    query.password shouldBe password
    query.workerClusterKey shouldBe workerClusterKey
    query.catalogPattern.get shouldBe catalogPattern
    query.schemaPattern.get shouldBe schemaPattern
    query.tableName.get shouldBe tableName
  }

  @Test
  def ignoreUrlOnCreation(): Unit =
    an[NullPointerException] should be thrownBy InspectApi.access
      .hostname(CommonUtils.randomString(10))
      .port(CommonUtils.availablePort())
      .rdbRequest
      .user(CommonUtils.randomString())
      .password(CommonUtils.randomString())
      .query

  @Test
  def ignoreUserOnCreation(): Unit =
    an[NullPointerException] should be thrownBy InspectApi.access
      .hostname(CommonUtils.randomString(10))
      .port(CommonUtils.availablePort())
      .rdbRequest
      .jdbcUrl(CommonUtils.randomString())
      .password(CommonUtils.randomString())
      .query

  @Test
  def ignorePasswordOnCreation(): Unit =
    an[NullPointerException] should be thrownBy InspectApi.access
      .hostname(CommonUtils.randomString(10))
      .port(CommonUtils.availablePort())
      .rdbRequest
      .jdbcUrl(CommonUtils.randomString())
      .user(CommonUtils.randomString())
      .query

  @Test
  def nullUrl(): Unit = an[NullPointerException] should be thrownBy InspectApi.access.rdbRequest.jdbcUrl(null)

  @Test
  def emptyUrl(): Unit = an[IllegalArgumentException] should be thrownBy InspectApi.access.rdbRequest.jdbcUrl("")

  @Test
  def nullUser(): Unit = an[NullPointerException] should be thrownBy InspectApi.access.rdbRequest.user(null)

  @Test
  def emptyUser(): Unit = an[IllegalArgumentException] should be thrownBy InspectApi.access.rdbRequest.user("")

  @Test
  def nullPassword(): Unit = an[NullPointerException] should be thrownBy InspectApi.access.rdbRequest.password(null)

  @Test
  def emptyPassword(): Unit = an[IllegalArgumentException] should be thrownBy InspectApi.access.rdbRequest.password("")

  @Test
  def nullWorkerClusterKey(): Unit =
    an[NullPointerException] should be thrownBy InspectApi.access.rdbRequest.workerClusterKey(null)

  @Test
  def nullSchemaPattern(): Unit =
    an[NullPointerException] should be thrownBy InspectApi.access.rdbRequest.schemaPattern(null)

  @Test
  def emptySchemaPattern(): Unit =
    an[IllegalArgumentException] should be thrownBy InspectApi.access.rdbRequest.schemaPattern("")

  @Test
  def nullCatalogPattern(): Unit =
    an[NullPointerException] should be thrownBy InspectApi.access.rdbRequest.catalogPattern(null)

  @Test
  def emptyCatalogPattern(): Unit =
    an[IllegalArgumentException] should be thrownBy InspectApi.access.rdbRequest.catalogPattern("")

  @Test
  def nullTableName(): Unit = an[NullPointerException] should be thrownBy InspectApi.access.rdbRequest.tableName(null)

  @Test
  def emptyTableName(): Unit =
    an[IllegalArgumentException] should be thrownBy InspectApi.access.rdbRequest.tableName("")

  @Test
  def testParseJson(): Unit = {
    val url               = CommonUtils.randomString()
    val user              = CommonUtils.randomString()
    val password          = CommonUtils.randomString()
    val workerClusterName = CommonUtils.randomString()
    val catalogPattern    = CommonUtils.randomString()
    val schemaPattern     = CommonUtils.randomString()
    val tableName         = CommonUtils.randomString()

    val query = InspectApi.RDB_QUERY_FORMAT.read(s"""
         |{
         |  "url": "$url",
         |  "user": "$user",
         |  "password": "$password",
         |  "workerClusterKey": "$workerClusterName",
         |  "catalogPattern": "$catalogPattern",
         |  "schemaPattern": "$schemaPattern",
         |  "tableName": "$tableName"
         |}
     """.stripMargin.parseJson)

    query.url shouldBe url
    query.user shouldBe user
    query.password shouldBe password
    query.workerClusterKey.name() shouldBe workerClusterName
    query.catalogPattern.get shouldBe catalogPattern
    query.schemaPattern.get shouldBe schemaPattern
    query.tableName.get shouldBe tableName
  }

  @Test
  def testParseEmptyUrl(): Unit =
    parseInvalidJson(s"""
       |{
       |  "url": "",
       |  "user": "user",
       |  "password": "password"
       |}
     """.stripMargin.parseJson)

  @Test
  def testParseEmptyUser(): Unit = parseInvalidJson(s"""
         |{
         |  "url": "url",
         |  "user": "",
         |  "password": "password"
         |}
     """.stripMargin.parseJson)

  @Test
  def testParseEmptyPassword(): Unit = parseInvalidJson(s"""
     |{
     |  "url": "url",
     |  "user": "user",
     |  "password": ""
     |}
     """.stripMargin.parseJson)

  @Test
  def testParseEmptyWorkerClusterName(): Unit = parseInvalidJson(s"""
     |{
     |  "url": "url",
     |  "user": "user",
     |  "password": "password",
     |  "workerClusterKey": ""
     |}
     """.stripMargin.parseJson)

  @Test
  def testParseEmptyCatalogPattern(): Unit = parseInvalidJson(s"""
                                                                    |{
                                                                    |  "url": "url",
                                                                    |  "user": "user",
                                                                    |  "password": "password",
                                                                    |  "catalogPattern": ""
                                                                    |}
     """.stripMargin.parseJson)

  @Test
  def testParseEmptySchemaPattern(): Unit = parseInvalidJson(s"""
                                                                 |{
                                                                 |  "url": "url",
                                                                 |  "user": "user",
                                                                 |  "password": "password",
                                                                 |  "schemaPattern": ""
                                                                 |}
     """.stripMargin.parseJson)

  @Test
  def testParseEmptyTableName(): Unit = parseInvalidJson(s"""
                                                                |{
                                                                |  "url": "url",
                                                                |  "user": "user",
                                                                |  "password": "password",
                                                                |  "tableName": ""
                                                                |}
     """.stripMargin.parseJson)

  private[this] def parseInvalidJson(json: JsValue): Unit =
    an[DeserializationException] should be thrownBy InspectApi.RDB_QUERY_FORMAT.read(json)
}
